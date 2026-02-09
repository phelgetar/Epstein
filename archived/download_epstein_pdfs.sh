#!/bin/bash

################################################################################
# Epstein DOJ Files Downloader
# Downloads all PDF files from the DOJ Epstein disclosure datasets
################################################################################

set -e  # Exit on error

# Configuration
BASE_URL="https://www.justice.gov/epstein/doj-disclosures"
OUTPUT_DIR="epstein_doj_files"
NUM_DATASETS=12
LOG_FILE="download_log.txt"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Create output directory
mkdir -p "$OUTPUT_DIR"
cd "$OUTPUT_DIR"

# Start logging
echo "Download started: $(date)" | tee "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"
echo ""

# Function to download a single PDF
download_pdf() {
    local url="$1"
    local output_file="$2"
    local attempt=1
    local max_attempts=3
    
    while [ $attempt -le $max_attempts ]; do
        if [ $attempt -gt 1 ]; then
            echo "    Retry $attempt/$max_attempts..."
            sleep 2
        fi
        
        # Download with curl
        if curl -L -f -s -S \
            -H "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36" \
            -H "Accept: application/pdf,application/x-pdf,*/*" \
            -H "Accept-Language: en-US,en;q=0.9" \
            -H "Referer: $BASE_URL" \
            -o "$output_file" \
            "$url"; then
            
            # Verify it's actually a PDF
            if [ -f "$output_file" ]; then
                file_type=$(file -b "$output_file")
                if echo "$file_type" | grep -qi "PDF"; then
                    file_size=$(stat -f%z "$output_file" 2>/dev/null || stat -c%s "$output_file" 2>/dev/null)
                    echo -e "    ${GREEN}✓${NC} Downloaded: $output_file (${file_size} bytes)"
                    return 0
                else
                    echo -e "    ${YELLOW}⚠${NC} Not a PDF (got: $file_type), retrying..."
                    rm -f "$output_file"
                fi
            fi
        else
            echo -e "    ${YELLOW}⚠${NC} Download failed, retrying..."
        fi
        
        attempt=$((attempt + 1))
    done
    
    echo -e "    ${RED}✗${NC} Failed to download: $url" | tee -a "../$LOG_FILE"
    return 1
}

# Download datasets
total_downloaded=0
total_failed=0

for dataset_num in $(seq 1 $NUM_DATASETS); do
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}Processing Data Set $dataset_num of $NUM_DATASETS${NC}"
    echo -e "${BLUE}========================================${NC}"
    
    dataset_dir="data-set-$dataset_num"
    mkdir -p "$dataset_dir"
    
    page_url="$BASE_URL/data-set-$dataset_num-files"
    echo "URL: $page_url"
    
    # Download the page HTML
    echo "Fetching page content..."
    page_html=$(curl -s -L \
        -H "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36" \
        "$page_url")
    
    if [ -z "$page_html" ]; then
        echo -e "${RED}✗ Failed to fetch page${NC}" | tee -a "$LOG_FILE"
        continue
    fi
    
    # Try multiple patterns to extract PDF links
    echo "Extracting PDF links..."
    
    # Pattern 1: Standard href with .pdf
    pdf_links=$(echo "$page_html" | grep -oE 'href="[^"]*\.pdf[^"]*"' | sed 's/href="//;s/"//')
    
    # Pattern 2: Links in <a> tags
    if [ -z "$pdf_links" ]; then
        pdf_links=$(echo "$page_html" | grep -oE '<a[^>]+href="[^"]*\.pdf[^"]*"' | sed 's/.*href="//;s/".*//')
    fi
    
    # Pattern 3: Any URL ending in .pdf
    if [ -z "$pdf_links" ]; then
        pdf_links=$(echo "$page_html" | grep -oE 'https?://[^"[:space:]]+\.pdf')
    fi
    
    # Pattern 4: File path links
    if [ -z "$pdf_links" ]; then
        pdf_links=$(echo "$page_html" | grep -oE '/sites/[^"[:space:]]+\.pdf')
    fi
    
    link_count=$(echo "$pdf_links" | grep -c "pdf" || echo "0")
    
    if [ "$link_count" -eq 0 ]; then
        echo -e "${YELLOW}⚠ No PDF links found on this page${NC}" | tee -a "$LOG_FILE"
        echo "Saving page HTML for inspection..."
        echo "$page_html" > "data-set-$dataset_num-page.html"
        echo "Page saved to: data-set-$dataset_num-page.html"
        echo ""
        continue
    fi
    
    echo "Found $link_count PDF link(s)"
    echo ""
    
    dataset_downloaded=0
    dataset_failed=0
    
    # Download each PDF
    while IFS= read -r link; do
        [ -z "$link" ] && continue
        
        # Build full URL
        if [[ $link == http://* ]] || [[ $link == https://* ]]; then
            full_url="$link"
        elif [[ $link == //* ]]; then
            full_url="https:$link"
        elif [[ $link == /* ]]; then
            full_url="https://www.justice.gov$link"
        else
            full_url="$BASE_URL/$link"
        fi
        
        # Extract filename
        filename=$(basename "$full_url" | sed 's/%20/ /g; s/%28/(/g; s/%29/)/g')
        
        # Skip if already exists and is valid
        if [ -f "$dataset_dir/$filename" ]; then
            if file "$dataset_dir/$filename" | grep -q "PDF"; then
                echo "  ⊙ Skipping (already exists): $filename"
                dataset_downloaded=$((dataset_downloaded + 1))
                continue
            else
                echo "  Removing invalid existing file: $filename"
                rm "$dataset_dir/$filename"
            fi
        fi
        
        echo "  Downloading: $filename"
        
        if download_pdf "$full_url" "$dataset_dir/$filename"; then
            dataset_downloaded=$((dataset_downloaded + 1))
            total_downloaded=$((total_downloaded + 1))
        else
            dataset_failed=$((dataset_failed + 1))
            total_failed=$((total_failed + 1))
        fi
        
    done <<< "$pdf_links"
    
    echo ""
    echo -e "${GREEN}Data Set $dataset_num Summary:${NC}"
    echo "  Downloaded: $dataset_downloaded"
    echo "  Failed: $dataset_failed"
    echo "Data Set $dataset_num: Downloaded=$dataset_downloaded, Failed=$dataset_failed" >> "$LOG_FILE"
    echo ""
done

# Final summary
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Download Complete!${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo "Total PDFs downloaded: $total_downloaded"
echo "Total failures: $total_failed"
echo "Log file: $OUTPUT_DIR/$LOG_FILE"
echo ""

# Verify all downloaded files
echo "Verifying downloaded files..."
valid_count=0
invalid_count=0

for dataset_num in $(seq 1 $NUM_DATASETS); do
    dataset_dir="data-set-$dataset_num"
    if [ -d "$dataset_dir" ]; then
        for pdf in "$dataset_dir"/*.pdf 2>/dev/null; do
            if [ -f "$pdf" ]; then
                if file "$pdf" | grep -q "PDF"; then
                    valid_count=$((valid_count + 1))
                else
                    invalid_count=$((invalid_count + 1))
                    echo -e "${RED}Invalid PDF:${NC} $pdf"
                fi
            fi
        done
    fi
done

echo ""
echo "Valid PDFs: $valid_count"
echo "Invalid files: $invalid_count"

cd ..

echo ""
echo "Files are in: $(pwd)/$OUTPUT_DIR"
echo "Download completed: $(date)" | tee -a "$OUTPUT_DIR/$LOG_FILE"
