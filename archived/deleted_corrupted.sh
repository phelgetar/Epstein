#!/bin/bash

# First, find and delete all corrupted PDFs
echo "Finding corrupted PDFs..."

for i in {1..12}; do
    dir="epstein_doj_files/data-set-$i"
    if [ -d "$dir" ]; then
        cd "$dir"

        # Check if there are any PDF files
        shopt -s nullglob
        pdf_files=(*.pdf)

        for pdf in "${pdf_files[@]}"; do
            if [ -f "$pdf" ]; then
                if ! file "$pdf" | grep -q "PDF"; then
                    echo "Removing corrupted: $pdf"
                    rm "$pdf"
                else
                    echo "Valid PDF: $pdf"
                fi
            fi
        done

        cd - > /dev/null
    fi
done

echo "Corrupted files removed."