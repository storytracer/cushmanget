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
import time
from datetime import datetime, timezone
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
        self.cache = None
        self.cached_session = None
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.ua = UserAgent()
        
    async def __aenter__(self):
        # Setup filesystem-based HTTP cache
        self.cache_dir.mkdir(exist_ok=True)
        self.cache = FileBackend(
            cache_name=str(self.cache_dir / 'http_cache'),
            expire_after=3600  # Cache for 1 hour
        )
        
        # Create a persistent session for cached requests
        timeout = aiohttp.ClientTimeout(total=60)
        self.cached_session = CachedSession(
            cache=self.cache,
            timeout=timeout,
            trust_env=True
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.cached_session:
            await self.cached_session.close()
        if self.cache:
            await self.cache.close()
    
    async def scan_existing_progress(self, pages_dir: Path, items_dir: Path, images_dir: Path = None) -> Dict:
        """Scan existing metadata files to determine progress"""
        progress = {
            'discovered_items': [],
            'processed_metadata': set(),
            'downloaded_images': set(),
            'failed_items': set(),
            'metadata_results': [],
            'last_page': 0
        }
        
        # Use a set to track unique item IDs and avoid duplicates
        unique_items = {}
        
        # Scan existing page files
        if pages_dir.exists():
            page_files = list(pages_dir.glob("*.json"))
            for page_file in page_files:
                try:
                    # Extract page number from filename (e.g., "0001.json" -> 1)
                    page_num = int(page_file.stem)
                    progress['last_page'] = max(progress['last_page'], page_num)
                    
                    # Load page metadata to get discovered items
                    async with aiofiles.open(page_file, 'r') as f:
                        page_data = json.loads(await f.read())
                        if 'items' in page_data:
                            for item in page_data['items']:
                                # Only add if we haven't seen this item ID before
                                if item['id'] not in unique_items:
                                    unique_items[item['id']] = item
                except (ValueError, json.JSONDecodeError):
                    continue
        
        # Convert unique items dict back to list
        progress['discovered_items'] = list(unique_items.values())
        
        # Scan existing item metadata files
        if items_dir.exists():
            item_files = list(items_dir.glob("*.json"))
            for item_file in item_files:
                try:
                    item_id = item_file.stem
                    progress['processed_metadata'].add(item_id)
                    
                    # Load item metadata for final results
                    async with aiofiles.open(item_file, 'r') as f:
                        item_data = json.loads(await f.read())
                        progress['metadata_results'].append(item_data)
                except json.JSONDecodeError:
                    continue
        
        # Scan existing image files
        if images_dir and images_dir.exists():
            image_files = list(images_dir.glob("*.jp2"))
            for image_file in image_files:
                # Extract item ID from JP2 filename (e.g., "P15818.jp2" -> find matching item)
                filename = image_file.name
                # Find corresponding item by checking metadata for this filename
                for item_data in progress['metadata_results']:
                    if item_data.get('jp2_filename') == filename:
                        progress['downloaded_images'].add(item_data['id'])
                        break
        
        return progress
    
    async def get_max_page_number(self) -> int:
        """Get the maximum page number from the last li element in pagination"""
        try:
            headers = {'User-Agent': self.ua.random}
            async with self.cached_session.get(f"{self.collection_url}?page=1", headers=headers) as response:
                response.raise_for_status()
                content = await response.text()
            
            soup = BeautifulSoup(content, 'html.parser')
            
            # Find pagination ul element
            pagination_ul = soup.find('ul', class_='pagination')
            if pagination_ul:
                # Get all li elements in the pagination
                li_elements = pagination_ul.find_all('li')
                
                # Start from the last li element and work backwards to find the highest page number
                for li in reversed(li_elements):
                    # Look for an 'a' tag with href containing page= parameter
                    link = li.find('a', href=re.compile(r'page=\d+'))
                    if link:
                        href = link.get('href', '')
                        match = re.search(r'page=(\d+)', href)
                        if match:
                            return int(match.group(1))
            
            return None
            
        except Exception as e:
            print(f"Could not determine max page number: {e}")
            return None
    
    async def get_page_items(self, page: int, pages_dir: Path) -> List[Dict]:
        """Get items from a specific page with caching and save page metadata"""
        async with self.semaphore:
            page_url = f"{self.collection_url}?page={page}"
            
            try:
                headers = {'User-Agent': self.ua.random}
                
                # Use the persistent cached session
                async with self.cached_session.get(page_url, headers=headers) as response:
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
                    'scraped_at': datetime.now(timezone.utc).isoformat()
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
        
        # Get actual max page number from HTML
        if not max_pages:
            max_pages = await self.get_max_page_number()
            if max_pages:
                print(f"Detected {max_pages} pages in collection")
            else:
                print("Could not determine total pages, will discover until empty pages found")
        
        # Create page discovery queue
        page_queue = asyncio.Queue()
        start_page = page
        
        if max_pages:
            end_page = max_pages
            for p in range(start_page, end_page + 1):
                await page_queue.put(p)
            
            # Create progress bar with known total
            discovery_pbar = tqdm(total=end_page - start_page + 1, desc="Discovering pages", unit="pages")
        else:
            # Unknown total pages - add a reasonable batch to start
            end_page = start_page + 100  # Start with 100 pages, will expand if needed
            for p in range(start_page, end_page + 1):
                await page_queue.put(p)
            
            # Create progress bar without known total
            discovery_pbar = tqdm(desc="Discovering pages", unit="pages")
        
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
                        discovery_pbar.set_description("Discovering pages")
                    
                    discovery_pbar.update(1)
                    page_queue.task_done()
                    
                except asyncio.TimeoutError:
                    break
                except Exception as e:
                    discovery_pbar.set_description(f"Discovering pages (error: {str(e)[:20]})")
                    consecutive_empty += 1
            
            return items
        
        # Start page discovery workers
        workers = [asyncio.create_task(page_worker()) for _ in range(min(self.max_concurrent, 20))]
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
        
        # Remove duplicates from discovered items
        seen = set()
        unique_items = []
        for item in all_items:
            if item['id'] not in seen:
                seen.add(item['id'])
                unique_items.append(item)
        
        progress['discovered_items'] = unique_items
        print(f"Discovered {len(unique_items)} unique items")
        return unique_items
    
    async def extract_metadata(self, item: Dict, items_dir: Path) -> Optional[Dict]:
        """Extract metadata from an individual item page with caching and save individual item metadata"""
        async with self.semaphore:
            # Check if JSON file already exists
            item_file = items_dir / f"{item['id']}.json"
            if item_file.exists():
                # Load existing metadata and return it
                try:
                    async with aiofiles.open(item_file, 'r') as f:
                        existing_metadata = json.loads(await f.read())
                        return existing_metadata
                except (json.JSONDecodeError, OSError):
                    # If file is corrupted, continue with re-scraping
                    pass
            
            try:
                headers = {'User-Agent': self.ua.random}
                
                # Use the persistent cached session
                async with self.cached_session.get(item['url'], headers=headers) as response:
                    response.raise_for_status()
                    content = await response.text()
                
                soup = BeautifulSoup(content, 'html.parser')
                
                metadata = {
                    'url': item['url'],
                    'id': item['id'],
                    'scraped_at': datetime.now(timezone.utc).isoformat()
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
                            'persistent url': 'persistent_url',
                            'state/province': 'state',
                            'cushman identifier': 'cushman_identifier',
                        }
                        
                        mapped_key = field_mappings.get(key, key.replace(' ', '_'))
                        
                        # Special handling for subjects - parse as list from ul/li elements
                        if mapped_key == 'subjects':
                            # Look for ul element within the dd element
                            ul_element = dd.find('ul')
                            if ul_element:
                                # Extract text from each li element
                                subjects = [li.get_text().strip() for li in ul_element.find_all('li') if li.get_text().strip()]
                            else:
                                # Fallback to plain text if no ul found
                                subjects = [value.strip()] if value.strip() else []
                            
                            metadata[mapped_key] = subjects
                        else:
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
                
                # Use cushman_identifier for filename if available, otherwise fall back to item ID
                filename = metadata.get('cushman_identifier')
                item_file = items_dir / f"{filename}.json"
                
                # Save individual item metadata
                if metadata.get('cushman_identifier'):
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
                timeout = aiohttp.ClientTimeout(total=300)  # 5 minute timeout for large images
                headers = {'User-Agent': self.ua.random}
                
                # Use regular ClientSession (not CachedSession) to avoid caching large image files
                async with aiohttp.ClientSession(timeout=timeout, trust_env=True) as download_session:
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
        
        if download_images:
            images_path = output_path / "images"
            images_path.mkdir(exist_ok=True)
        
        # Scan existing files to determine progress instead of using progress.json
        progress = await self.scan_existing_progress(pages_dir, items_dir, images_path if download_images else None)
        
        # Discover all items
        items = await self.discover_all_items(progress, pages_dir, max_pages)
        
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