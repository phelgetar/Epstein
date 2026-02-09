#!/bin/bash

download_pdf() {
    local url="$1"
    local output="$2"
    local max_retries=3
    local retry=0

    while [ $retry -lt $max_retries ]; do
        echo "  Attempt $((retry + 1))/$max_retries: $output"

        curl -L -o "$output" \
          -H "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36" \
          -H "Accept: application/pdf,*/*" \
          -H "Accept-Encoding: identity" \
          --compressed \
          "$url"

        # Check if it's a valid PDF
        if [ -f "$output" ] && file "$output" | grep -q "PDF"; then
            echo "    ✓ Valid PDF downloaded"
            return 0
        else
            echo "    ✗ Invalid file, retrying..."
            rm -f "$output"
            retry=$((retry + 1))
            sleep 2
        fi
    done

    echo "    ✗ Failed after $max_retries attempts"
    return 1
}

mkdir -p epstein_doj_files
cd epstein_doj_files

for i in {1..12}; do
    echo "========================================="
    echo "Data Set $i"
    echo "========================================="

    mkdir -p "data-set-$i"
    cd "data-set-$i"

    page_url="https://www.justice.gov/epstein/doj-disclosures/data-set-$i-files"

    # Get all PDF links
    curl -s "$page_url" | \
      grep -oE 'href="[^"]*\.pdf"' | \
      sed 's/href="//;s/"$//' | \
      while read -r link; do
        # Build full URL
        if [[ $link == http* ]]; then
            url="$link"
        elif [[ $link == //* ]]; then
            url="https:$link"
        elif [[ $link == /* ]]; then
            url="https://www.justice.gov$link"
        else
            url="https://www.justice.gov/epstein/doj-disclosures/$link"
        fi

        filename=$(basename "$url" | sed 's/%20/ /g')

        # Skip if already downloaded
        if [ -f "$filename" ] && file "$filename" | grep -q "PDF"; then
            echo "  ⊙ Skipping (already exists): $filename"
            continue
        fi

        download_pdf "$url" "$filename"
    done

    # Report results
    pdf_count=$(ls -1 *.pdf 2>/dev/null | wc -l | xargs)
    echo "  Dataset $i: $pdf_count PDFs downloaded"

    cd ..
    echo ""
done

echo "========================================="
echo "All downloads complete!"
echo "========================================="