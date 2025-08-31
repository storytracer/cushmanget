# Charles W. Cushman Kodachrome Slides Scraper

An integrated metadata scraper and image downloader for the [Charles W. Cushman Kodachrome Slides collection](https://digitalcollections.iu.edu/collections/2801pg36g) from Indiana University Digital Collections.

## Features

- **Parallel Processing**: Uses asyncio with configurable concurrency (default 10 concurrent requests)
- **Fully Resumable**: Tracks progress and can resume from interruptions
- **Queue-based Architecture**: Separate queues for metadata extraction and image downloads
- **Incremental Metadata Saving**: Page and item metadata saved as JSON files as they're scraped
- **HTTP Caching**: Filesystem-based response caching for faster resumed runs
- Scrapes all 14,425+ items from the collection
- Extracts metadata from HTML elements including title, creator, date, subjects, etc.
- Downloads original JP2 image files
- Rate limiting to be respectful to the server
- Saves metadata in both JSON and CSV formats
- Command-line interface with various options

## Installation

```bash
pip install -r requirements.txt
```

## Usage

### Basic Usage

```bash
# Scrape everything (metadata + images)
python cushmanget.py

# Metadata only (no image downloads)
python cushmanget.py --no-images

# Limit to 100 items for testing
python cushmanget.py --max-items 100
```

### Advanced Options

```bash
# High performance (20 concurrent transfers)
python cushmanget.py --transfers 20 --delay 0.05

# Conservative settings for slower connections
python cushmanget.py --transfers 5 --delay 0.5

# Custom directories
python cushmanget.py -o /path/to/output --cache-dir ./custom_cache

# Limit scraping scope
python cushmanget.py --max-pages 10      # First 10 pages only
python cushmanget.py --max-items 100     # First 100 items only
```

### Resume Capability

The scraper automatically saves progress to `progress.json`. If interrupted, simply run the same command again to resume:

```bash
# This will resume from where it left off
python cushmanget.py
```

### HTTP Caching

The scraper uses `aiohttp-client-cache` with filesystem backend to cache HTTP responses, significantly speeding up resumed runs and reducing server load.

## Output Structure

```
output/
├── progress.json          # Resume state (can be deleted after completion)
├── metadata.json          # Complete metadata in JSON format
├── metadata.csv           # Metadata in CSV format for Excel/analysis
├── metadata/              # Individual JSON files saved as scraped
│   ├── pages/             # Page metadata with item listings
│   │   ├── page_0001.json
│   │   ├── page_0002.json
│   │   └── ...
│   └── items/             # Individual item metadata
│       ├── item_1z40kt94j.json
│       ├── item_2n49t287m.json
│       └── ...
└── images/                # Downloaded JP2 image files
    ├── P15818.jp2
    ├── P15819.jp2
    └── ...

.cache/
└── http_cache/            # HTTP response cache (speeds up resumed runs)
```

## Metadata Fields

The scraper extracts the following metadata fields when available:
- `title` - Image title/description
- `creator` - Charles W. Cushman (1896-1972)
- `date` - Date of photograph
- `subjects` - Subject tags
- `description` - Detailed description
- `collection` - Collection name
- `holding_location` - Physical location of original
- `persistent_url` - Permanent URL for the item
- `jp2_url` - Direct download link for JP2 image
- `jp2_filename` - Filename of the JP2 image

## Performance & Rate Limiting

- Default: 10 concurrent transfers with 0.1s delay
- Recommended for metadata-only: `--transfers 20 --delay 0.05`
- Conservative for slower connections: `--transfers 5 --delay 0.5`
- HTTP responses are cached for 1 hour, dramatically speeding up resumed runs

## Error Handling & Resume Features

- **Full Resume Support**: Automatically saves progress and resumes from interruptions
- **Failed Item Tracking**: Keeps track of failed items and reports them
- **Duplicate Handling**: Won't re-download existing files or re-process completed items
- **Progress Monitoring**: Real-time progress bars for both metadata extraction and downloads
- **Graceful Error Handling**: Continues processing even when individual items fail

## Legal Note

This scraper is designed for research and educational purposes. The Charles W. Cushman collection is part of Indiana University's digital collections. Please respect their terms of use and copyright policies.