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
                    # Use item ID from filename as the primary key
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
            image_files = list(images_dir.glob("*.*"))  # Accept any image file extension
            
            # Extract item IDs from image filenames (remove extension)
            for image_file in image_files:
                # Get the stem (filename without extension) which should be the item_id
                item_id = image_file.stem
                progress['downloaded_images'].add(item_id)
        
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
            
            raise ValueError("Could not find pagination with valid page numbers")
            
        except Exception as e:
            raise ValueError(f"Could not determine max page number: {e}")
    
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
                item_links = soup.select('main > .hyc-container > .hyc-bl-results p.media-heading > strong > a')
                
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
        
        # Get actual max page number from HTML (mandatory)
        if not max_pages:
            max_pages = await self.get_max_page_number()
            print(f"Detected {max_pages} pages in collection")
        
        # Determine which pages already exist by scanning the pages folder
        existing_pages = set()
        if pages_dir.exists():
            for page_file in pages_dir.glob("*.json"):
                try:
                    page_num = int(page_file.stem)
                    existing_pages.add(page_num)
                except ValueError:
                    continue
        
        # Check if we have complete page discovery
        if len(existing_pages) == max_pages and all(p in existing_pages for p in range(1, max_pages + 1)):
            if progress['discovered_items']:
                print(f"Page discovery complete: found {len(progress['discovered_items'])} items from {max_pages} pages")
                return progress['discovered_items']
            else:
                print(f"All {max_pages} page files exist, loading items...")
                # Load items from existing page files
                unique_items = {}
                page_files = list(pages_dir.glob("*.json"))
                for page_file in page_files:
                    try:
                        async with aiofiles.open(page_file, 'r') as f:
                            page_data = json.loads(await f.read())
                            if 'items' in page_data:
                                for item in page_data['items']:
                                    unique_items[item['id']] = item
                    except (ValueError, json.JSONDecodeError):
                        continue
                
                items_list = list(unique_items.values())
                progress['discovered_items'] = items_list
                print(f"Loaded {len(items_list)} items from existing page files")
                return items_list
        
        all_items = []
        
        # Sample first page to check for items if we don't have it
        if 1 not in existing_pages:
            sample_items = await self.get_page_items(1, pages_dir)
            if not sample_items:
                print("Could not fetch any items from page 1")
                return []
        
        # Create page discovery queue for missing pages
        page_queue = asyncio.Queue()
        
        # Add missing pages to queue
        missing_pages = []
        for p in range(1, max_pages + 1):
            if p not in existing_pages:
                missing_pages.append(p)
                await page_queue.put(p)
        
        if missing_pages:
            print(f"Downloading {len(missing_pages)} missing pages (have {len(existing_pages)} pages)")
            discovery_pbar = tqdm(total=len(missing_pages), desc="Downloading missing pages", unit="pages")
        else:
            print(f"All {max_pages} pages already downloaded")
            discovery_pbar = None
        
        # If no pages need downloading, just reload existing items and return
        if page_queue.empty():
            if discovery_pbar:
                discovery_pbar.close()
            
            # Reload all items from existing pages
            if progress['discovered_items']:
                return progress['discovered_items']
            
            # Load items from all page files
            unique_items = {}
            if pages_dir.exists():
                page_files = list(pages_dir.glob("*.json"))
                for page_file in page_files:
                    try:
                        async with aiofiles.open(page_file, 'r') as f:
                            page_data = json.loads(await f.read())
                            if 'items' in page_data:
                                for item in page_data['items']:
                                    unique_items[item['id']] = item
                    except (ValueError, json.JSONDecodeError):
                        continue
            
            items_list = list(unique_items.values())
            progress['discovered_items'] = items_list
            return items_list
        
        async def page_worker():
            items = []
            consecutive_empty = 0
            
            while consecutive_empty < 3:  # Stop after 3 consecutive empty pages
                try:
                    page_num = await asyncio.wait_for(page_queue.get(), timeout=1.0)
                    page_items = await self.get_page_items(page_num, pages_dir)
                    
                    if not page_items:
                        consecutive_empty += 1
                        if discovery_pbar:
                            discovery_pbar.set_description(f"Downloading missing pages (empty: {consecutive_empty})")
                    else:
                        consecutive_empty = 0
                        items.extend(page_items)
                        progress['last_page'] = page_num
                        if discovery_pbar:
                            discovery_pbar.set_description("Downloading missing pages")
                    
                    if discovery_pbar:
                        discovery_pbar.update(1)
                    page_queue.task_done()
                    
                except asyncio.TimeoutError:
                    break
                except Exception as e:
                    if discovery_pbar:
                        discovery_pbar.set_description(f"Downloading pages (error: {str(e)[:20]})")
                    consecutive_empty += 1
            
            return items
        
        # Start page discovery workers
        workers = [asyncio.create_task(page_worker()) for _ in range(min(self.max_concurrent, 20))]
        worker_results = await asyncio.gather(*workers, return_exceptions=True)
        
        if discovery_pbar:
            discovery_pbar.close()
        
        # Collect results from workers
        new_items = []
        for result in worker_results:
            if isinstance(result, list):
                new_items.extend(result)
        
        # Now reload all items from all page files to get complete list
        unique_items = {}
        if pages_dir.exists():
            page_files = list(pages_dir.glob("*.json"))
            for page_file in page_files:
                try:
                    async with aiofiles.open(page_file, 'r') as f:
                        page_data = json.loads(await f.read())
                        if 'items' in page_data:
                            for item in page_data['items']:
                                unique_items[item['id']] = item
                except (ValueError, json.JSONDecodeError):
                    continue
        
        items_list = list(unique_items.values())
        progress['discovered_items'] = items_list
        print(f"Page discovery complete: found {len(items_list)} total items")
        return items_list
    
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
                
                # Extract download links from work-items table
                work_items_div = soup.find('div', id='work-items')
                download_links = {}
                if work_items_div:
                    # Find the table with related files
                    table = work_items_div.find('table', class_='table')
                    if table:
                        # Process each row in the table body
                        rows = table.find('tbody').find_all('tr') if table.find('tbody') else []
                        for row in rows:
                            # Extract filename from the title field (second column)
                            title_cell = row.find('td', class_='attribute-filename')
                            filename = None
                            if title_cell:
                                title_link = title_cell.find('a')
                                if title_link:
                                    filename = title_link.get_text().strip()
                            
                            # Extract download link from the actions column
                            download_link = row.find('a', class_='file_download')
                            if filename and download_link and download_link.get('href'):
                                download_url = urljoin(self.base_url, download_link['href'])
                                download_links[filename] = download_url
                
                if download_links:
                    metadata['download_links'] = download_links
                
                
                # Extract persistent URL if not found
                if 'persistent_url' not in metadata:
                    purl_link = soup.find('a', href=re.compile(r'http://purl\.dlib\.indiana\.edu/'))
                    if purl_link:
                        metadata['persistent_url'] = purl_link['href']
                
                # Always use item ID for filename to ensure consistent tracking
                item_file = items_dir / f"{item['id']}.json"
                
                # Save individual item metadata
                async with aiofiles.open(item_file, 'w') as f:
                    await f.write(json.dumps(metadata, indent=2))

                await asyncio.sleep(self.delay)
                return metadata
                
            except Exception as e:
                # Silent error handling for cleaner progress bars
                return None
    
    async def download_image(self, download_url: str, images_dir: Path, item_id: str, desired_format: str = '') -> tuple[bool, Optional[str]]:
        """Download image file, returning success status and filename from headers"""
        async with self.semaphore:
            try:
                # Create a new session for image downloads (no caching for large files)
                timeout = aiohttp.ClientTimeout(total=300)  # 5 minute timeout for large images
                headers = {'User-Agent': self.ua.random}
                
                # Use regular ClientSession (not CachedSession) to avoid caching large image files
                async with aiohttp.ClientSession(timeout=timeout, trust_env=True) as download_session:
                    async with download_session.get(download_url, headers=headers) as response:
                        response.raise_for_status()
                        
                        # Extract file extension from Content-Disposition header
                        file_extension = None
                        content_disposition = response.headers.get('Content-Disposition', '')
                        if content_disposition:
                            import re
                            filename_match = re.search(r'filename[*]?=([^;]+)', content_disposition)
                            if filename_match:
                                original_filename = filename_match.group(1).strip('"\'')
                                if '.' in original_filename:
                                    file_extension = '.' + original_filename.split('.')[-1]
                        
                        # Raise exception if no file extension found
                        if not file_extension:
                            raise ValueError(f"No file extension found in Content-Disposition header for {download_url}")
                        
                        # Check if the file extension matches the desired format
                        file_format = file_extension.lstrip('.').lower()
                        if desired_format and file_format != desired_format.lower():
                            # Skip this file as it doesn't match the desired format
                            return False, None
                        
                        # Use item_id as filename with original extension
                        filename = f"{item_id}{file_extension}"
                        output_path = images_dir / filename
                        
                        # Check if file already exists
                        if output_path.exists():
                            return True, filename
                        
                        # Write file directly
                        async with aiofiles.open(output_path, 'wb') as f:
                            async for chunk in response.content.iter_chunked(8192):
                                await f.write(chunk)
                
                await asyncio.sleep(self.delay)
                return True, filename
                
            except Exception as e:
                # Silent error handling for cleaner progress bars
                return False, None
    
    async def metadata_worker(self, metadata_queue: asyncio.Queue, progress: Dict, items_dir: Path, pbar: tqdm):
        """Worker for processing metadata extraction"""
        while True:
            try:
                item = await asyncio.wait_for(metadata_queue.get(), timeout=1.0)
                
                metadata = await self.extract_metadata(item, items_dir)
                if metadata:
                    progress['metadata_results'].append(metadata)
                    # Use item ID as primary key for tracking
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
    
    async def download_worker(self, download_queue: asyncio.Queue, progress: Dict, images_path: Path, pbar: tqdm, image_format: str = ''):
        """Worker for downloading images"""
        while True:
            try:
                metadata = await asyncio.wait_for(download_queue.get(), timeout=1.0)
                
                if metadata['id'] in progress['downloaded_images']:
                    download_queue.task_done()
                    pbar.update(1)
                    continue
                
                # Use download_links field, filter by desired format
                download_links = metadata.get('download_links', {})
                
                # Pre-filter download links by file format
                matching_urls = []
                for filename, url in download_links.items():
                    if filename.lower().endswith(f'.{image_format.lower()}'):
                        matching_urls.append(url)
                
                download_url = matching_urls[0] if matching_urls else None
                if not download_url:
                    download_queue.task_done()
                    pbar.update(1)
                    continue
                
                success, filename = await self.download_image(download_url, images_path, metadata['id'], image_format)
                
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
    
    async def download_images_only(self, output_dir="./output", image_format="jp2"):
        """Download images for existing metadata files only"""
        output_path = Path(output_dir)
        
        # Check if items directory exists
        items_dir = output_path / "items"
        
        if not items_dir.exists():
            print(f"No items directory found at {items_dir}")
            print("Run 'cushmanget metadata' first to scrape metadata")
            return
        
        # Create images directory
        images_path = output_path / "images"
        images_path.mkdir(exist_ok=True)
        
        # Load existing metadata files
        metadata_results = []
        item_files = list(items_dir.glob("*.json"))
        
        if not item_files:
            print("No metadata files found")
            print("Run 'cushmanget metadata' first to scrape metadata")
            return
        
        print(f"Loading metadata from {len(item_files)} files")
        for item_file in item_files:
            try:
                async with aiofiles.open(item_file, 'r') as f:
                    item_data = json.loads(await f.read())
                    metadata_results.append(item_data)
            except (json.JSONDecodeError, OSError):
                continue
        
        if not metadata_results:
            print("No valid metadata files found")
            return
        
        # Filter for items with JP2 URLs that don't have downloaded images yet
        download_queue = asyncio.Queue()
        download_count = 0
        downloaded_images = set()
        
        # Check which images are already downloaded
        if images_path.exists():
            image_files = list(images_path.glob("*.*"))  # Accept any image file extension
            
            # Extract item IDs from image filenames (remove extension)
            for image_file in image_files:
                # Get the stem (filename without extension) which should be the item_id
                item_id = image_file.stem
                downloaded_images.add(item_id)
        
        # Add items with download URLs to download queue (excluding already downloaded)
        for metadata in metadata_results:
            download_links = metadata.get('download_links', {})
            
            # Pre-filter download links by file format
            has_matching_format = any(
                filename.lower().endswith(f'.{image_format.lower()}') 
                for filename in download_links.keys()
            )
            
            if has_matching_format and metadata['id'] not in downloaded_images:
                await download_queue.put(metadata)
                download_count += 1
        
        if download_count == 0:
            print("All available images already downloaded")
            return
        
        print(f"Downloading {download_count} images")
        
        # Progress tracking
        progress = {'downloaded_images': downloaded_images, 'failed_items': set()}
        
        # Start download workers
        download_pbar = tqdm(total=download_count, desc="Downloading images")
        download_workers = [
            asyncio.create_task(self.download_worker(download_queue, progress, images_path, download_pbar, image_format))
            for _ in range(self.max_concurrent)
        ]
        
        # Wait for downloads
        await download_queue.join()
        
        # Cancel download workers
        for worker in download_workers:
            worker.cancel()
        
        download_pbar.close()
        
        print(f"\nImage download complete!")
        print(f"Downloaded {len(progress['downloaded_images']) - len(downloaded_images)} new images")
        print(f"Failed downloads: {len(progress['failed_items'])}")
        print(f"Cache directory: {self.cache_dir}")

    async def scrape_collection(self, output_dir="./output", download_images=True, 
                              max_pages=None, max_items=None, image_format="jp2"):
        """Main async scraping function"""
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        # Create directories directly in output
        pages_dir = output_path / "pages"
        pages_dir.mkdir(exist_ok=True)
        
        items_dir = output_path / "items"
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
        
        # Show resume message only if there are processed metadata files
        if progress['processed_metadata']:
            print(f"Resuming: {len(progress['processed_metadata'])} items already processed")
        
        print(f"Processing {len(items)} items")
        
        # Create queues
        metadata_queue = asyncio.Queue()
        download_queue = asyncio.Queue()
        
        # Add items to metadata queue (excluding already processed)
        for item in items:
            # Check if this item has already been processed using item ID
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
            # Add metadata with download URLs to download queue (excluding already downloaded)
            download_count = 0
            for metadata in progress['metadata_results']:
                download_links = metadata.get('download_links', {})
                
                # Pre-filter download links by file format
                has_matching_format = any(
                    filename.lower().endswith(f'.{image_format.lower()}') 
                    for filename in download_links.keys()
                )
                
                if has_matching_format and metadata['id'] not in progress['downloaded_images']:
                    await download_queue.put(metadata)
                    download_count += 1
            
            if download_count > 0:
                download_pbar = tqdm(total=download_count, desc="Downloading images")
                download_workers = [
                    asyncio.create_task(self.download_worker(download_queue, progress, images_path, download_pbar, image_format))
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
        
        print(f"\nScraping complete!")
        print(f"Processed {len(progress['processed_metadata'])} items")
        if download_images:
            print(f"Downloaded {len(progress['downloaded_images'])} images")
        print(f"Failed items: {len(progress['failed_items'])}")
        print(f"Cache directory: {self.cache_dir}")


# Global options for all commands
@click.group()
@click.option('--output-dir', '-o', default='./output', help='Output directory for files')
@click.option('--cache-dir', default='.cache', help='Cache directory for HTTP responses')
@click.option('--max-pages', type=int, help='Maximum number of pages to scrape')
@click.option('--max-items', type=int, help='Maximum number of items to process')
@click.option('--transfers', type=int, default=10, help='Number of concurrent transfers')
@click.option('--delay', type=float, default=0.1, help='Delay between requests in seconds')
@click.pass_context
def main(ctx, output_dir, cache_dir, max_pages, max_items, transfers, delay):
    """Charles W. Cushman Kodachrome Slides Collection Scraper
    
    Scrapes metadata and images from Indiana University's digital collection
    with parallel processing, caching, and resume capability.
    """
    # Store global options in context for subcommands
    ctx.ensure_object(dict)
    ctx.obj['output_dir'] = output_dir
    ctx.obj['cache_dir'] = cache_dir
    ctx.obj['max_pages'] = max_pages
    ctx.obj['max_items'] = max_items
    ctx.obj['transfers'] = transfers
    ctx.obj['delay'] = delay


@main.command()
@click.pass_context
def metadata(ctx):
    """Scrape metadata only (no image downloads)
    
    Examples:
      cushmanget metadata                     # Scrape all metadata
      cushmanget --max-items 100 metadata    # Limit to 100 items
      cushmanget --transfers 20 metadata     # Increase parallelism
    """
    
    async def run_scraper():
        async with CushmanScraper(
            max_concurrent=ctx.obj['transfers'], 
            delay=ctx.obj['delay'], 
            cache_dir=ctx.obj['cache_dir']
        ) as scraper:
            await scraper.scrape_collection(
                output_dir=ctx.obj['output_dir'],
                download_images=False,
                max_pages=ctx.obj['max_pages'],
                max_items=ctx.obj['max_items']
            )
    
    asyncio.run(run_scraper())


@main.command()
@click.option('--format', default='', help='Image format to download (jp2, jpg, png, etc.)', show_default=True)
@click.pass_context
def images(ctx, format):
    """Download images for existing metadata files
    
    This command only downloads images for items that already have 
    metadata JSON files. Run 'metadata' command first.
    
    Examples:
      cushmanget images                       # Download all jp2 images (default)
      cushmanget images --format jpg         # Download jpg images only
      cushmanget --cache-dir ./cache images  # Custom cache location
      cushmanget --transfers 20 images       # Increase parallelism
    """
    
    async def run_downloader():
        async with CushmanScraper(
            max_concurrent=ctx.obj['transfers'], 
            delay=ctx.obj['delay'], 
            cache_dir=ctx.obj['cache_dir']
        ) as scraper:
            await scraper.download_images_only(
                output_dir=ctx.obj['output_dir'],
                image_format=format
            )
    
    asyncio.run(run_downloader())


if __name__ == '__main__':
    main()