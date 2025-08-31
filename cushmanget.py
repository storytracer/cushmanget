#!/usr/bin/env python3
"""
cushmanget - Charles W. Cushman Kodachrome Slides Collection Scraper
Parallel, resumable scraper with caching for metadata and image downloading
"""

import os
import re
import json
import csv
import asyncio
from pathlib import Path
from urllib.parse import urljoin, urlparse
import aiohttp
from aiohttp_client_cache import CachedSession, FileBackend
import aiofiles
from bs4 import BeautifulSoup
from tqdm import tqdm
import click
from fake_useragent import UserAgent
from typing import List, Dict, Set, Optional


class CushmanScraper:
    def __init__(self, base_url="https://digitalcollections.iu.edu", max_concurrent=10, delay=0.1, cache_dir=".cache"):
        self.base_url = base_url
        self.collection_url = f"{base_url}/collections/2801pg36g"
        self.max_concurrent = max_concurrent
        self.delay = delay
        self.cache_dir = Path(cache_dir)
        self.session = None
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.ua = UserAgent()
        
    async def __aenter__(self):
        # Setup filesystem-based HTTP cache
        self.cache_dir.mkdir(exist_ok=True)
        cache = FileBackend(
            cache_name=str(self.cache_dir / 'http_cache'),
            expire_after=3600  # Cache for 1 hour
        )
        
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=self.max_concurrent)
        timeout = aiohttp.ClientTimeout(total=60)
        
        self.session = CachedSession(
            cache=cache,
            connector=connector,
            timeout=timeout,
            trust_env=True,  # Respect proxy environment variables
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def load_progress(self, progress_file: Path) -> Dict:
        """Load progress from file"""
        if progress_file.exists():
            async with aiofiles.open(progress_file, 'r') as f:
                content = await f.read()
                return json.loads(content)
        return {
            'discovered_items': [],
            'processed_metadata': set(),
            'downloaded_images': set(),
            'failed_items': set(),
            'last_page': 0
        }
    
    async def save_progress(self, progress_file: Path, progress: Dict):
        """Save progress to file"""
        progress_copy = progress.copy()
        for key in ['processed_metadata', 'downloaded_images', 'failed_items']:
            if isinstance(progress_copy[key], set):
                progress_copy[key] = list(progress_copy[key])
        
        async with aiofiles.open(progress_file, 'w') as f:
            await f.write(json.dumps(progress_copy, indent=2))
    
    async def get_page_items(self, page: int, pages_dir: Path) -> List[Dict]:
        """Get items from a specific page with caching and save page metadata"""
        async with self.semaphore:
            page_url = f"{self.collection_url}?page={page}"
            
            try:
                headers = {'User-Agent': self.ua.random}
                async with self.session.get(page_url, headers=headers) as response:
                    response.raise_for_status()
                    content = await response.text()
                    
                    # Check if response was cached (silent for cleaner progress bars)
                    
                soup = BeautifulSoup(content, 'html.parser')
                item_links = soup.find_all('a', href=re.compile(r'/concern/images/[a-z0-9]+'))
                
                items = []
                for link in item_links:
                    # Parse URL properly to extract clean ID
                    parsed_url = urlparse(link['href'])
                    item_id = parsed_url.path.split('/')[-1]
                    items.append({
                        'url': urljoin(self.base_url, link['href']),
                        'id': item_id
                    })
                
                # Save page metadata
                page_metadata = {
                    'page': page,
                    'url': page_url,
                    'item_count': len(items),
                    'items': items,
                    'scraped_at': asyncio.get_event_loop().time()
                }
                
                page_file = pages_dir / f"{page:04d}.json"
                async with aiofiles.open(page_file, 'w') as f:
                    await f.write(json.dumps(page_metadata, indent=2))
                
                await asyncio.sleep(self.delay)
                return items
                
            except Exception as e:
                print(f"Error fetching page {page}: {e}")
                return []
    
    async def discover_all_items(self, progress: Dict, pages_dir: Path, max_pages: Optional[int] = None) -> List[Dict]:
        """Discover all items in the collection with resume support"""
        if progress['discovered_items']:
            print(f"Resuming from {len(progress['discovered_items'])} discovered items")
            return progress['discovered_items']
        
        all_items = []
        page = max(1, progress['last_page'])
        
        # Sample first page to check for items
        sample_items = await self.get_page_items(1, pages_dir)
        if not sample_items:
            print("Could not fetch any items from page 1")
            return []
        
        # Create page discovery queue
        page_queue = asyncio.Queue()
        start_page = page
        end_page = max_pages if max_pages else 1500  # Estimate based on collection size
        
        for p in range(start_page, end_page + 1):
            await page_queue.put(p)
        
        # Create progress bar for page discovery
        discovery_pbar = tqdm(total=end_page - start_page + 1, desc="Discovering pages", unit="pages")
        
        async def page_worker():
            items = []
            consecutive_empty = 0
            
            while consecutive_empty < 3:  # Stop after 3 consecutive empty pages
                try:
                    page_num = await asyncio.wait_for(page_queue.get(), timeout=1.0)
                    page_items = await self.get_page_items(page_num, pages_dir)
                    
                    if not page_items:
                        consecutive_empty += 1
                        discovery_pbar.set_description(f"Discovering pages (empty: {consecutive_empty})")
                    else:
                        consecutive_empty = 0
                        items.extend(page_items)
                        progress['last_page'] = page_num
                        discovery_pbar.set_description(f"Discovering pages (found: {len(page_items)} items)")
                    
                    discovery_pbar.update(1)
                    page_queue.task_done()
                    
                except asyncio.TimeoutError:
                    break
                except Exception as e:
                    discovery_pbar.set_description(f"Discovering pages (error: {str(e)[:20]})")
                    consecutive_empty += 1
            
            return items
        
        # Start page discovery workers
        workers = [asyncio.create_task(page_worker()) for _ in range(min(3, self.max_concurrent))]
        worker_results = await asyncio.gather(*workers, return_exceptions=True)
        
        discovery_pbar.close()
        
        # Collect and deduplicate results
        seen = set()
        for result in worker_results:
            if isinstance(result, list):
                for item in result:
                    if item['id'] not in seen:
                        seen.add(item['id'])
                        all_items.append(item)
        
        progress['discovered_items'] = all_items
        print(f"Discovered {len(all_items)} unique items")
        return all_items
    
    async def extract_metadata(self, item: Dict, items_dir: Path) -> Optional[Dict]:
        """Extract metadata from an individual item page with caching and save individual item metadata"""
        async with self.semaphore:
            try:
                headers = {'User-Agent': self.ua.random}
                async with self.session.get(item['url'], headers=headers) as response:
                    response.raise_for_status()
                    content = await response.text()
                
                soup = BeautifulSoup(content, 'html.parser')
                
                metadata = {
                    'url': item['url'],
                    'id': item['id'],
                    'scraped_at': asyncio.get_event_loop().time()
                }
                
                # Extract title
                title_elem = soup.find('h1') or soup.find('title')
                if title_elem:
                    metadata['title'] = title_elem.get_text().strip()
                
                # Extract metadata from definition lists
                dl_elements = soup.find_all('dl')
                for dl in dl_elements:
                    dt_elements = dl.find_all('dt')
                    dd_elements = dl.find_all('dd')
                    
                    for dt, dd in zip(dt_elements, dd_elements):
                        key = dt.get_text().strip().lower().replace(':', '')
                        value = dd.get_text().strip()
                        
                        field_mappings = {
                            'creator': 'creator',
                            'date': 'date',
                            'subject': 'subjects',
                            'description': 'description',
                            'collection': 'collection',
                            'holding location': 'holding_location',
                            'persistent url': 'persistent_url'
                        }
                        
                        mapped_key = field_mappings.get(key, key.replace(' ', '_'))
                        metadata[mapped_key] = value
                
                # Extract JP2 download link
                jp2_link = soup.find('a', href=re.compile(r'.*\.jp2$'))
                if jp2_link:
                    metadata['jp2_url'] = urljoin(self.base_url, jp2_link['href'])
                    metadata['jp2_filename'] = jp2_link['href'].split('/')[-1]
                
                # Extract persistent URL if not found
                if 'persistent_url' not in metadata:
                    purl_link = soup.find('a', href=re.compile(r'http://purl\.dlib\.indiana\.edu/'))
                    if purl_link:
                        metadata['persistent_url'] = purl_link['href']
                
                # Save individual item metadata
                item_file = items_dir / f"{item['id']}.json"
                async with aiofiles.open(item_file, 'w') as f:
                    await f.write(json.dumps(metadata, indent=2))
                
                await asyncio.sleep(self.delay)
                return metadata
                
            except Exception as e:
                # Silent error handling for cleaner progress bars
                return None
    
    async def download_image(self, jp2_url: str, output_path: Path) -> bool:
        """Download JP2 image file"""
        async with self.semaphore:
            try:
                if output_path.exists():
                    return True
                
                # Create a new session for image downloads (no caching for large files)
                connector = aiohttp.TCPConnector(limit=100)
                timeout = aiohttp.ClientTimeout(total=300)  # 5 minute timeout for large images
                
                # Use regular ClientSession (not CachedSession) to avoid caching large image files
                async with aiohttp.ClientSession(connector=connector, timeout=timeout, trust_env=True) as download_session:
                    headers = {'User-Agent': self.ua.random}
                    async with download_session.get(jp2_url, headers=headers) as response:
                        response.raise_for_status()
                        
                        # Create temporary file first
                        temp_path = output_path.with_suffix('.tmp')
                        
                        async with aiofiles.open(temp_path, 'wb') as f:
                            async for chunk in response.content.iter_chunked(8192):
                                await f.write(chunk)
                        
                        # Atomically move to final location
                        temp_path.rename(output_path)
                
                await asyncio.sleep(self.delay)
                return True
                
            except Exception as e:
                # Silent error handling for cleaner progress bars
                # Clean up partial download
                if output_path.exists():
                    output_path.unlink()
                temp_path = output_path.with_suffix('.tmp')
                if temp_path.exists():
                    temp_path.unlink()
                return False
    
    async def metadata_worker(self, metadata_queue: asyncio.Queue, progress: Dict, items_dir: Path, pbar: tqdm):
        """Worker for processing metadata extraction"""
        while True:
            try:
                item = await asyncio.wait_for(metadata_queue.get(), timeout=1.0)
                
                if item['id'] in progress['processed_metadata']:
                    metadata_queue.task_done()
                    pbar.update(1)
                    continue
                
                metadata = await self.extract_metadata(item, items_dir)
                if metadata:
                    progress['metadata_results'].append(metadata)
                    progress['processed_metadata'].add(item['id'])
                else:
                    progress['failed_items'].add(item['id'])
                
                metadata_queue.task_done()
                pbar.update(1)
                
            except asyncio.TimeoutError:
                break
            except Exception as e:
                # Silent error handling for cleaner progress bars
                break
    
    async def download_worker(self, download_queue: asyncio.Queue, progress: Dict, images_path: Path, pbar: tqdm):
        """Worker for downloading images"""
        while True:
            try:
                metadata = await asyncio.wait_for(download_queue.get(), timeout=1.0)
                
                if 'jp2_filename' not in metadata or metadata['id'] in progress['downloaded_images']:
                    download_queue.task_done()
                    pbar.update(1)
                    continue
                
                output_path = images_path / metadata['jp2_filename']
                success = await self.download_image(metadata['jp2_url'], output_path)
                
                if success:
                    progress['downloaded_images'].add(metadata['id'])
                else:
                    progress['failed_items'].add(metadata['id'])
                
                download_queue.task_done()
                pbar.update(1)
                
            except asyncio.TimeoutError:
                break
            except Exception as e:
                # Silent error handling for cleaner progress bars
                break
    
    async def scrape_collection(self, output_dir="./output", download_images=True, 
                              max_pages=None, max_items=None):
        """Main async scraping function"""
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        # Create metadata directories
        metadata_dir = output_path / "metadata"
        metadata_dir.mkdir(exist_ok=True)
        
        pages_dir = metadata_dir / "pages"
        pages_dir.mkdir(exist_ok=True)
        
        items_dir = metadata_dir / "items"
        items_dir.mkdir(exist_ok=True)
        
        progress_file = output_path / "progress.json"
        progress = await self.load_progress(progress_file)
        
        # Convert lists back to sets for processing
        for key in ['processed_metadata', 'downloaded_images', 'failed_items']:
            if isinstance(progress[key], list):
                progress[key] = set(progress[key])
        
        if 'metadata_results' not in progress:
            progress['metadata_results'] = []
        
        if download_images:
            images_path = output_path / "images"
            images_path.mkdir(exist_ok=True)
        
        # Discover all items
        items = await self.discover_all_items(progress, pages_dir, max_pages)
        await self.save_progress(progress_file, progress)
        
        if max_items:
            items = items[:max_items]
        
        print(f"Processing {len(items)} items")
        
        # Create queues
        metadata_queue = asyncio.Queue()
        download_queue = asyncio.Queue()
        
        # Add items to metadata queue (excluding already processed)
        for item in items:
            if item['id'] not in progress['processed_metadata']:
                await metadata_queue.put(item)
        
        if metadata_queue.qsize() > 0:
            # Start metadata workers
            metadata_pbar = tqdm(total=metadata_queue.qsize(), desc="Extracting metadata")
            metadata_workers = [
                asyncio.create_task(self.metadata_worker(metadata_queue, progress, items_dir, metadata_pbar))
                for _ in range(self.max_concurrent)
            ]
            
            # Wait for metadata extraction
            await metadata_queue.join()
            
            # Cancel metadata workers
            for worker in metadata_workers:
                worker.cancel()
            
            metadata_pbar.close()
            
            # Save progress after metadata extraction
            await self.save_progress(progress_file, progress)
        else:
            print("All metadata already extracted")
        
        # Start image downloads if requested
        if download_images:
            # Add metadata with JP2 URLs to download queue (excluding already downloaded)
            download_count = 0
            for metadata in progress['metadata_results']:
                if 'jp2_url' in metadata and metadata['id'] not in progress['downloaded_images']:
                    await download_queue.put(metadata)
                    download_count += 1
            
            if download_count > 0:
                download_pbar = tqdm(total=download_count, desc="Downloading images")
                download_workers = [
                    asyncio.create_task(self.download_worker(download_queue, progress, images_path, download_pbar))
                    for _ in range(self.max_concurrent)
                ]
                
                # Wait for downloads
                await download_queue.join()
                
                # Cancel download workers
                for worker in download_workers:
                    worker.cancel()
                
                download_pbar.close()
            else:
                print("All images already downloaded")
        
        # Save final metadata files
        await self.save_final_metadata(output_path, progress['metadata_results'])
        
        # Final progress save
        await self.save_progress(progress_file, progress)
        
        print(f"\nScraping complete!")
        print(f"Processed {len(progress['processed_metadata'])} items")
        if download_images:
            print(f"Downloaded {len(progress['downloaded_images'])} images")
        print(f"Failed items: {len(progress['failed_items'])}")
        print(f"Cache directory: {self.cache_dir}")
    
    async def save_final_metadata(self, output_path: Path, metadata_results: List[Dict]):
        """Save metadata to JSON and CSV files"""
        if not metadata_results:
            return
        
        # Save JSON
        json_path = output_path / "metadata.json"
        async with aiofiles.open(json_path, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(metadata_results, indent=2, ensure_ascii=False))
        
        # Save CSV
        csv_path = output_path / "metadata.csv"
        fieldnames = set()
        for item in metadata_results:
            fieldnames.update(item.keys())
        fieldnames = sorted(list(fieldnames))
        
        # Write CSV synchronously (aiofiles doesn't support csv.writer)
        import csv as sync_csv
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = sync_csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(metadata_results)


@click.command()
@click.option('--output-dir', '-o', default='./output', help='Output directory for files')
@click.option('--cache-dir', default='.cache', help='Cache directory for HTTP responses')
@click.option('--no-images', is_flag=True, help='Skip image downloads, metadata only')
@click.option('--max-pages', type=int, help='Maximum number of pages to scrape')
@click.option('--max-items', type=int, help='Maximum number of items to process')
@click.option('--transfers', type=int, default=10, help='Number of concurrent transfers')
@click.option('--delay', type=float, default=0.1, help='Delay between requests in seconds')
def main(output_dir, cache_dir, no_images, max_pages, max_items, transfers, delay):
    """Charles W. Cushman Kodachrome Slides Collection Scraper
    
    Scrapes metadata and images from Indiana University's digital collection
    with parallel processing, caching, and resume capability.
    
    Examples:
      cushmanget                              # Scrape everything
      cushmanget --no-images                  # Metadata only
      cushmanget --max-items 100              # Limit to 100 items
      cushmanget --transfers 20               # Increase parallelism
      cushmanget --cache-dir ./custom_cache   # Custom cache location
    """
    
    async def run_scraper():
        async with CushmanScraper(
            max_concurrent=transfers, 
            delay=delay, 
            cache_dir=cache_dir
        ) as scraper:
            await scraper.scrape_collection(
                output_dir=output_dir,
                download_images=not no_images,
                max_pages=max_pages,
                max_items=max_items
            )
    
    asyncio.run(run_scraper())


if __name__ == '__main__':
    main()