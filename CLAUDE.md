# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python web scraper for the Charles W. Cushman Kodachrome Slides collection from Indiana University Digital Collections. The scraper uses asyncio for parallel processing, includes HTTP caching, and supports resumable operations.

## Development Commands

### Setup and Dependencies
```bash
pip install -r requirements.txt
```

### Running the Scraper
```bash
# Basic usage - scrape everything
python cushmanget.py

# Metadata only (no image downloads)
python cushmanget.py --no-images

# Testing with limited items
python cushmanget.py --max-items 100

# Performance tuning
python cushmanget.py --transfers 20 --delay 0.05

# Conservative settings
python cushmanget.py --transfers 5 --delay 0.5
```

## Architecture

### Core Components

**CushmanScraper Class** (`cushmanget.py:26`): Main scraper class that handles:
- Async context management with HTTP session caching
- Semaphore-based concurrency control (default 10 concurrent requests)
- Queue-based processing architecture for metadata extraction and image downloads

### Key Methods

**Discovery Phase** (`cushmanget.py:207`):
- `discover_all_items()`: Discovers all items across collection pages
- `get_page_items()`: Extracts item links from individual collection pages
- `get_max_page_number()`: Determines total pages from pagination HTML

**Processing Phase** (`cushmanget.py:306`):
- `extract_metadata()`: Scrapes individual item pages for metadata using BeautifulSoup
- `download_image()`: Downloads JP2 image files with atomic operations
- Worker functions use separate queues for metadata vs image processing

**Resume Capability** (`cushmanget.py:61`):
- `scan_existing_progress()`: Scans output directories to determine what's already been processed
- Uses filesystem state rather than progress files for resume logic

### Data Flow

1. **Page Discovery**: Scrapes collection pages to discover all item URLs
2. **Metadata Extraction**: Parallel workers extract metadata from item pages
3. **Image Downloads**: Separate workers download JP2 images (if enabled)
4. **Output Generation**: Saves metadata as both JSON and CSV formats

### Caching Strategy

- HTTP responses cached using `aiohttp-client-cache` with filesystem backend (1 hour TTL)
- Large image files use uncached sessions to avoid filling cache
- Resume functionality based on existing output files rather than progress state

### Output Structure

```
output/
├── metadata.json          # Complete metadata
├── metadata.csv           # CSV format for analysis
├── metadata/
│   ├── pages/            # Individual page metadata
│   └── items/            # Individual item metadata
└── images/               # JP2 image files (if downloaded)

.cache/
└── http_cache/           # HTTP response cache
```

### Error Handling

The scraper uses graceful error handling:
- Failed items tracked in `progress['failed_items']` set
- Silent error handling during processing to maintain clean progress bars
- Atomic file operations (temp files renamed to final location)
- Resume capability handles partial downloads and corrupted files