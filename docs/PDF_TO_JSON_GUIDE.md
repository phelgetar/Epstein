# PDF to JSON Conversion and Search Guide

## Overview
Convert all downloaded Epstein DOJ PDFs into searchable JSON files, then search through them efficiently.

## Step 1: Install Dependencies

```bash
# Install poppler (includes pdftotext and pdfinfo)
brew install poppler

# Verify installation
pdftotext -v
pdfinfo -v
```

## Step 2: Convert PDFs to JSON

```bash
python3 pdf_to_json.py
```

This will:
1. Process all PDFs in `epstein_doj_files/data-set-*/`
2. Extract full text and metadata from each PDF
3. Create 4 JSON files:

### Output Files:

**1. `epstein_pdfs_full.json`** (LARGEST - complete archive)
- Contains all text from every PDF
- Full metadata for each file
- Use this for comprehensive analysis

**2. `epstein_pdfs_search_index.json`** (RECOMMENDED for searching)
- Flat structure optimized for searching
- Full text included
- Faster to load and search

**3. `epstein_pdfs_summary.json`** (SMALL - overview only)
- Metadata only (no full text)
- Text previews (first 500 characters)
- Good for browsing file list

**4. `epstein_pdfs_file_list.json`** (SMALLEST - inventory)
- Just filenames, page counts, sizes
- Quick reference

## Step 3: Search the PDFs

### Interactive Search Mode:
```bash
python3 search_pdfs.py
```

Then enter search terms:
```
Search> Epstein
Search> Maxwell AND island
Search> flight OR plane
```

### Command Line Search:
```bash
# Single term
python3 search_pdfs.py "Epstein"

# Multiple terms (AND)
python3 search_pdfs.py "Epstein AND Maxwell"

# Multiple terms (OR)
python3 search_pdfs.py "flight OR plane"
```

## Search Features

- **Case-insensitive** by default
- **Context snippets** showing text around matches
- **Match counts** per document
- **Dataset tracking** (which data-set each file came from)
- **AND/OR logic** for complex queries

## Example Usage

### Find all documents mentioning a person:
```bash
python3 search_pdfs.py "Prince Andrew"
```

### Find documents with multiple terms:
```bash
python3 search_pdfs.py "flight AND island"
```

### Find documents with any of several terms:
```bash
python3 search_pdfs.py "Maxwell OR Brunel OR Richardson"
```

## JSON Structure

### Full JSON Format:
```json
{
  "metadata": {
    "extraction_date": "2026-02-08T12:00:00Z",
    "total_files": 150,
    "total_pages": 5000,
    "total_size_mb": 500.5
  },
  "datasets": [
    {
      "dataset_number": 1,
      "dataset_name": "data-set-1",
      "files": [
        {
          "filename": "document.pdf",
          "pages": 10,
          "size_mb": 1.5,
          "full_text": "...",
          "word_count": 2500,
          "title": "...",
          "author": "...",
          "creation_date": "..."
        }
      ]
    }
  ]
}
```

### Search Index Format:
```json
[
  {
    "dataset": 1,
    "filename": "document.pdf",
    "filepath": "/path/to/document.pdf",
    "pages": 10,
    "text": "full document text..."
  }
]
```

## Advanced: Using JSON Programmatically

### Python:
```python
import json

# Load the search index
with open('epstein_pdfs_search_index.json', 'r') as f:
    data = json.load(f)

# Search for a term
for doc in data:
    if 'epstein' in doc['text'].lower():
        print(f"Found in: {doc['filename']}")
        print(f"  Dataset: {doc['dataset']}")
        print(f"  Pages: {doc['pages']}")
```

### JavaScript (Node.js):
```javascript
const fs = require('fs');

// Load the search index
const data = JSON.parse(
  fs.readFileSync('epstein_pdfs_search_index.json', 'utf8')
);

// Search
data.forEach(doc => {
  if (doc.text.toLowerCase().includes('epstein')) {
    console.log(`Found in: ${doc.filename}`);
  }
});
```

### jq (command line):
```bash
# Count total files
jq 'length' epstein_pdfs_search_index.json

# Find files in dataset 1
jq '.[] | select(.dataset == 1) | .filename' epstein_pdfs_search_index.json

# Get all filenames
jq '.[].filename' epstein_pdfs_search_index.json

# Search for term (basic)
jq '.[] | select(.text | contains("Epstein")) | .filename' epstein_pdfs_search_index.json
```

## Performance Tips

1. **Use search_index.json for searching** - it's optimized for this
2. **Use summary.json for browsing** - lighter weight
3. **Use full.json for complete analysis** - has all data
4. **Cache results** if doing repeated searches

## Troubleshooting

### "pdftotext not found"
```bash
brew install poppler
```

### "No PDF files found"
Make sure PDFs are in: `epstein_doj_files/data-set-*/`

### Empty or garbled text extraction
Some PDFs might be:
- Scanned images (need OCR)
- Password protected
- Corrupted

To check individual files:
```bash
pdftotext epstein_doj_files/data-set-1/file.pdf -
```

### Large JSON files
The full JSON might be very large (100+ MB). Use:
- `epstein_pdfs_search_index.json` for searching (smaller)
- `epstein_pdfs_summary.json` for metadata only
- Stream processing for very large datasets

## Processing Time

Typical processing times:
- ~100 PDFs: 2-5 minutes
- ~500 PDFs: 10-15 minutes  
- ~1000 PDFs: 20-30 minutes

Progress is shown in real-time.

## Next Steps

After creating the JSON:

1. **Search interactively**:
   ```bash
   python3 search_pdfs.py
   ```

2. **Export search results**:
   Modify `search_pdfs.py` to save results to CSV/JSON

3. **Analyze with pandas**:
   ```python
   import pandas as pd
   import json
   
   with open('epstein_pdfs_search_index.json') as f:
       data = json.load(f)
   
   df = pd.DataFrame(data)
   print(df.describe())
   ```

4. **Build a web interface**:
   Use the JSON as a backend for a search UI

## File Size Estimates

For ~500 PDFs totaling ~500MB:
- `epstein_pdfs_full.json`: ~200-500 MB
- `epstein_pdfs_search_index.json`: ~150-400 MB
- `epstein_pdfs_summary.json`: ~5-20 MB
- `epstein_pdfs_file_list.json`: ~100 KB

Actual sizes depend on how much text is in the PDFs.
