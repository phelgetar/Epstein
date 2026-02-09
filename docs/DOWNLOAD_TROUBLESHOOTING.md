# Epstein DOJ Files Downloader - Troubleshooting Guide

## The Problem
The PDFs are appearing corrupted because wget/curl is likely downloading HTML error pages or redirects instead of the actual PDF files.

## Solution: Two Download Scripts

I've created two scripts - try both to see which works better:

### Option 1: Bash Script (download_epstein_pdfs.sh)
```bash
chmod +x download_epstein_pdfs.sh
./download_epstein_pdfs.sh
```

### Option 2: Python Script (download_epstein_pdfs.py) - RECOMMENDED
```bash
# Install dependencies first
pip3 install requests beautifulsoup4 --break-system-packages

# Run the script
python3 download_epstein_pdfs.py
```

## Why Files Might Be Corrupted

1. **HTML instead of PDF**: The server is returning error pages or redirects
2. **Missing User-Agent**: The server blocks requests without proper browser headers
3. **JavaScript Loading**: The page uses JavaScript to load PDF links dynamically
4. **Rate Limiting**: Too many requests too fast

## Diagnostic Steps

### Step 1: Check what's actually being downloaded
```bash
# Check the first few bytes of a "corrupted" file
head -c 100 epstein_doj_files/data-set-1/*.pdf | cat -v

# Or use file command
file epstein_doj_files/data-set-1/*.pdf
```

If you see `HTML` or `text`, that's the problem - you're downloading error pages.

### Step 2: Manually test one download
```bash
# Try downloading one file manually
curl -L -o test.pdf \
  -H "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36" \
  "https://www.justice.gov/epstein/doj-disclosures/data-set-1-files"

# Check what you got
file test.pdf
head -c 200 test.pdf
```

### Step 3: View the actual page in a browser
1. Open https://www.justice.gov/epstein/doj-disclosures/data-set-1-files in a browser
2. Right-click on a PDF link
3. Copy the actual link address
4. Test downloading that specific URL

### Step 4: Check if the page uses JavaScript
The Python script saves each page's HTML to `data-set-X/page_X.html`. Open these files to see:
- Are there actual PDF links in the HTML?
- Or does the page say "Loading..." or have JavaScript code?

## Alternative: Manual Browser Method

If the scripts don't work, the DOJ site might require:
1. Opening in a browser
2. Accepting cookies/terms
3. Then accessing the PDF files

You can use a browser extension like **DownThemAll** to batch download from the page.

## Wget Debugging Version

Try this verbose wget to see what's happening:

```bash
wget -d -v \
  --user-agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)" \
  --content-disposition \
  "https://www.justice.gov/epstein/doj-disclosures/data-set-1-files"
```

Look at the output for:
- Redirects (301, 302)
- Content-Type headers
- Final URL

## Check Server Response

```bash
# See what the server actually returns
curl -I -L \
  -H "User-Agent: Mozilla/5.0" \
  "https://www.justice.gov/epstein/doj-disclosures/data-set-1-files"
```

Look for:
- `Content-Type: application/pdf` (good)
- `Content-Type: text/html` (problem - not a PDF)

## Python Script Features

The Python script:
- ✓ Saves HTML of each page for inspection
- ✓ Verifies PDF signatures (%PDF- header)
- ✓ Uses proper browser headers
- ✓ Retries failed downloads
- ✓ Skips already-downloaded valid files
- ✓ Creates detailed logs

## If Both Scripts Fail

The website might:
1. **Require cookies/session**: Visit the main page first in a browser, then try downloading
2. **Use CAPTCHA**: You'll need to download manually
3. **Block automated access**: Use browser extensions instead
4. **Require authentication**: Check if you need to log in

## Browser Extension Method (Backup Plan)

1. Install **DownThemAll** or **Download All Files** extension
2. Visit each data-set page
3. Use the extension to download all PDFs
4. Manually organize into folders

## Verify Your Downloads

After downloading, run this to check validity:

```bash
#!/bin/bash
echo "Checking all PDFs..."
for pdf in epstein_doj_files/data-set-*/*.pdf; do
    if [ -f "$pdf" ]; then
        if file "$pdf" | grep -q "PDF"; then
            echo "✓ Valid: $(basename "$pdf")"
        else
            echo "✗ INVALID: $pdf"
            # Show first 200 bytes to see what it actually is
            echo "  Content preview:"
            head -c 200 "$pdf" | cat -v | head -5
        fi
    fi
done
```

## Next Steps After Successful Download

Once you have valid PDFs, run:

```bash
# Process them into JSON
python3 process_pdfs.py
```

This will create a JSON file with extracted text from all PDFs.

## Contact Information

If the downloads still fail:
1. Save one of the page HTMLs and inspect it
2. Check if PDFs are actually linked or if it's a different access method
3. Try accessing from a different network (some government sites block certain IPs)
