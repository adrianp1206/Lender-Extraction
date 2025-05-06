
# Lender Extraction from SEC Filings

This project extracts lender names from SEC filings using NLP techniques and a named entity recognition (NER) pipeline. It is built to support private credit research by identifying financial institutions involved in corporate credit agreements.

---

## Files

- `edgar_web_extraction.py`: Main script to extract lender names from EDGAR filings in bulk.
- `dedupe_validated.py`: Post-processing utility to deduplicate lender names within each row of the output.

---

## How the Algorithm Works

1. **Loads Filings**  
   Downloads HTML content from EDGAR using URLs provided in an Excel file.

2. **Extracts Relevant Snippets**  
   Searches for legal phrases like _"Credit Agreement"_ or _"Term Loan"_ and extracts the surrounding text.

3. **Applies NER**  
   Uses the `Jean-Baptiste/roberta-large-ner-english` model to identify organization names (entities tagged as `ORG`).

4. **Validates Lenders**  
   Each identified entity is:
   - Normalized (e.g., lowercased, suffixes stripped).
   - Checked against a curated list of known financial institutions.
   - Matched using aliases and fuzzy logic when necessary.

5. **Flags for Manual Review**  
   If an entity cannot be matched to a known lender, it is flagged with a confidence score for human inspection.

6. **Runs in Batches**  
   The process runs in chunks (default: 100 rows at a time) to avoid:
   - Timeouts or failures from large-scale EDGAR downloads.
   - Excessive runtime per batch.

---

## How to Run

### 1. Install Dependencies

```bash
pip install pandas transformers beautifulsoup4 requests openpyxl torch
```

## Deduplication Script: `dedupe_validated.py`

This script is used to clean up the `lender_name_validated` column in the extracted batch Excel files by removing duplicate lender names **within each cell**.

---

### What It Does

For each extracted Excel file matching:

```plaintext
extracted_lenders/extracted_lenders_*_updated.xlsx
```

###  What the Script Does

The script will:

- Open the file using `pandas`.
- Split the `lender_name_validated` string in each row by `;`.
- Remove duplicate names while preserving the original order.
- Save the cleaned file back in-place.

---

###  Why It’s Needed

The main extraction process may assign the same lender multiple times to a single row (due to overlapping or redundant NER matches). This script ensures clean, unique entries like:


---

### ▶️ How to Run

Run this script after completing a round of lender extraction:

```bash
python dedupe_validated.py
```


