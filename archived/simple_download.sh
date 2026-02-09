#!/bin/bash

for i in {1..12}; do
    mkdir -p "epstein_doj_files/data-set-$i"

    curl -s "https://www.justice.gov/epstein/doj-disclosures/data-set-$i-files" | \
      grep -oE 'href="(/[^"]*\.pdf|[^"]*\.pdf)"' | \
      sed 's/href="//;s/"$//' | \
      while read link; do
        [[ $link == http* ]] && url="$link" || url="https://www.justice.gov$link"
        filename=$(basename "$url")

        echo "Downloading: $filename to data-set-$i"
        curl -L -o "epstein_doj_files/data-set-$i/$filename" \
          -H "User-Agent: Mozilla/5.0" \
          "$url"

        # Verify
        if ! file "epstein_doj_files/data-set-$i/$filename" | grep -q "PDF"; then
            echo "  Failed - removing"
            rm "epstein_doj_files/data-set-$i/$filename"
        fi
      done
done