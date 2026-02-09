# Epstein DOJ Files - Web Search Interface

A beautiful, locally-hosted web application for searching through the Epstein DOJ disclosure documents.

## Features

✨ **Powerful Search**
- Full-text search across all documents
- Boolean operators (AND/OR)
- Case-sensitive searching
- Whole word matching
- Regular expression support

📊 **Rich Results**
- Context snippets around matches
- Match highlighting
- Document metadata (pages, dataset)
- Multiple matches per document
- **View PDF button** - Click to open and read the full document

🎨 **Beautiful Interface**
- Editorial newspaper-inspired design
- Responsive layout
- Smooth animations
- Professional typography
- **Built-in PDF viewer** - Read documents without leaving the page

## Quick Start

### Step 1: Create the Search Index (One-time setup)

If you haven't already, process the PDFs into a searchable JSON file:

```bash
python3 pdf_to_json.py
```

This will create `epstein_pdfs_search_index.json` which the web interface needs.

### Step 2: Start the Server

**On macOS/Linux:**
```bash
./start.sh
```

**On Windows:**
```
start.bat
```

**Or manually:**
```bash
python3 start_server.py
```

The script will:
1. Start a local web server on port 8000
2. Automatically open your browser to http://localhost:8000/search.html

### Step 3: Search!

Enter search terms and press Search or hit Enter.

### Step 4: View PDFs

Click the **"View PDF"** button on any search result to:
- Open the full document in a modal viewer
- Read the complete PDF without leaving the page
- Close with the X button or press ESC key

If the PDF doesn't load in the viewer, click "Open in New Tab" to view it in your browser's native PDF viewer.

## Search Examples

### Simple Search
```
Epstein
```
Finds all documents containing "Epstein"

### AND Search (all terms must match)
```
Epstein AND Maxwell
```
Finds documents containing both "Epstein" AND "Maxwell"

### OR Search (any term can match)
```
flight OR plane OR helicopter
```
Finds documents with any of these terms

### Complex Searches
```
Maxwell AND (island OR estate)
```
Note: Parentheses are not yet supported, but you can do multiple searches

### Case Sensitive
Check the "Case Sensitive" box to match exact capitalization:
```
FBI
```
Will only match "FBI", not "fbi"

### Whole Words Only
Check "Whole Words Only" to avoid partial matches:
```
land
```
With whole words: matches "land" but not "island"
Without: matches both

### Regular Expression
Check "Regular Expression" for advanced pattern matching:
```
\b\d{3}-\d{3}-\d{4}\b
```
Matches phone numbers like 555-123-4567

## File Structure

```
your-project-folder/
├── search.html                          # Main web interface
├── start_server.py                      # Python web server
├── start.sh                             # Startup script (macOS/Linux)
├── start.bat                            # Startup script (Windows)
├── epstein_pdfs_search_index.json       # Search database (created by pdf_to_json.py)
└── epstein_doj_files/                   # Your PDF files (MUST be here for viewing)
    ├── data-set-1/
    │   ├── document1.pdf                # ← Clickable from search results
    │   ├── document2.pdf
    │   └── ...
    ├── data-set-2/
    │   └── ...
    └── ...
```

**IMPORTANT:** The PDF files must remain in `epstein_doj_files/` directory (or wherever they were when you ran `pdf_to_json.py`) for the "View PDF" buttons to work. The web server serves files relative to where it's running.

## Requirements

- Python 3.6 or higher (for the web server)
- Modern web browser (Chrome, Firefox, Safari, Edge)
- The search index JSON file (created by `pdf_to_json.py`)

## Customization

### Change the Port

Edit `start_server.py` and change:
```python
PORT = 8000  # Change to any available port
```

### Modify Search Results

Edit `search.html` to change:
- Number of context snippets per result (line ~543: `slice(0, 5)`)
- Context size (line ~451: `contextSize = 200`)
- Design and styling (CSS section)

### Styling

The interface uses a newspaper/editorial aesthetic with:
- Crimson Pro (serif) for body text
- JetBrains Mono (monospace) for metadata
- Paper texture background
- Bold ink-and-paper color scheme

Edit the CSS `:root` variables to customize colors:
```css
:root {
    --paper: #FAF8F3;
    --ink-dark: #1A1512;
    --accent: #C1440E;
    /* ... more variables ... */
}
```

## Troubleshooting

### "Error loading documents"

**Problem:** The search index JSON file isn't found

**Solution:**
1. Make sure you've run `python3 pdf_to_json.py` first
2. Check that `epstein_pdfs_search_index.json` is in the same directory as `search.html`
3. Try using the full path in the browser: `file:///full/path/to/search.html`

### "Port already in use"

**Problem:** Port 8000 is being used by another application

**Solution:**
1. Stop other web servers
2. Or change the port in `start_server.py`

### "No documents found"

**Problem:** Search terms don't match anything

**Solution:**
1. Try simpler, broader search terms
2. Disable "Case Sensitive" and "Whole Words Only"
3. Check the search index has data: `jq 'length' epstein_pdfs_search_index.json`

### Browser can't connect

**Problem:** Browser shows "Can't connect to localhost"

**Solution:**
1. Make sure the server is running (you should see "Starting server..." in terminal)
2. Check for firewall blocking localhost connections
3. Try http://127.0.0.1:8000/search.html instead

### PDF won't load in viewer

**Problem:** "Could not load PDF" error in the modal viewer

**Solution:**
1. Click "Open in New Tab" - this will open the PDF in your browser's native viewer
2. Make sure the PDF files are still in the `epstein_doj_files/` directory
3. Check that the file paths in the JSON are correct (absolute or relative to where the server is running)
4. Some browsers block iframe PDF viewing - try a different browser (Chrome works best)

### PDFs are in a different location

**Problem:** PDF files were moved or are in a custom directory

**Solution:**
The search index JSON stores file paths. If you move PDFs after creating the index:
1. Re-run `python3 pdf_to_json.py` to update the paths
2. Or edit the JSON file to update the `filepath` values
3. Or create symbolic links to keep original paths working

### Search is slow

**Problem:** Large dataset takes time to search

**Solution:**
- This is normal for client-side searching of large datasets
- The first search might be slower as browser loads the JSON
- Subsequent searches are faster (data is cached)
- For very large datasets (>1000 docs), consider using the Python search script instead

## Advanced Usage

### API Access

The search interface is purely client-side JavaScript. You can integrate it with other tools:

```javascript
// In browser console or your own script
fetch('epstein_pdfs_search_index.json')
  .then(r => r.json())
  .then(data => {
    console.log(`Loaded ${data.length} documents`);
    // Your custom search logic here
  });
```

### Batch Processing

For automated searches, use the Python search script:
```bash
python3 search_pdfs.py "your search terms" > results.txt
```

### Export Results

Modify `search.html` to add an export button:
```javascript
function exportResults(results) {
  const csv = results.map(r => 
    `"${r.filename}",${r.dataset},${r.matches.length}`
  ).join('\n');
  
  const blob = new Blob([csv], { type: 'text/csv' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = 'search-results.csv';
  a.click();
}
```

## Performance

Typical performance metrics:
- **Load time:** 1-3 seconds (depending on JSON file size)
- **Search time:** 10-500ms (depending on dataset size and query)
- **Memory usage:** ~100-500MB (browser holds full search index in RAM)

## Security Note

This is a **local-only** web application:
- Runs on your computer only (localhost)
- No data is sent to external servers
- Documents stay on your machine
- Safe to use with sensitive documents

## Browser Compatibility

Tested and working on:
- ✅ Chrome 90+
- ✅ Firefox 88+
- ✅ Safari 14+
- ✅ Edge 90+

Older browsers may not support all features.

## Credits

- Data source: https://www.justice.gov/epstein/doj-disclosures
- Interface design: Custom editorial/newspaper aesthetic
- Fonts: Crimson Pro (Google Fonts), JetBrains Mono

## License

The web interface code is provided as-is for searching the public DOJ documents.
The documents themselves are public government records.

## Support

For issues:
1. Check this README's troubleshooting section
2. Verify all files are in the correct location
3. Check browser console for errors (F12 → Console tab)
