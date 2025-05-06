import pandas as pd
import re
import requests
from bs4 import BeautifulSoup
import difflib
from transformers import pipeline
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

logging.basicConfig(filename='lender_extraction_refactored.log', level=logging.INFO)

ner_pipeline = pipeline(
    "ner",
    model="Jean-Baptiste/roberta-large-ner-english",
    aggregation_strategy="simple"
)

# Constants
BASE_URL = "https://www.sec.gov/Archives/"
HEADERS = {'User-Agent': 'Mozilla/5.0 (compatible; MyScript/1.0)'}

KNOWN_LENDERS = [
    "JPMorgan Chase", "Bank of America", "Citibank", "Wells Fargo", "U.S. Bank",
    "Truist", "Capital One", "Fifth Third Bank", "Regions Bank", "PNC Financial",
    "KeyBank", "BB&T", "SunTrust", "First Republic Bank", "M&T Bank", "Huntington Bancshares",
    "BMO Harris Bank", "Citizens Bank", "Associated Bank", "Old National Bank", "Bank of the West",
    "HSBC", "Barclays Bank", "Deutsche Bank", "Credit Suisse", "BNP Paribas", "Societe Generale",
    "UniCredit", "Santander Bank", "Standard Chartered", "ING Bank", "NatWest", "Lloyds Banking Group",
    "UBS", "Comerica", "Flagstar Bank", "First Citizens Bank", "Signature Bank", "Zions Bancorporation",
    "Investors Bank", "UMB Financial", "Associated Banc-Corp", "Iberiabank",
    "Mitsubishi UFJ Financial Group", "Sumitomo Mitsui Banking Corporation", "Mizuho Bank",
    "Bank of China", "Industrial and Commercial Bank of China", "Agricultural Bank of China",
    "China Construction Bank", "Bank of Communications", "China Merchants Bank",
    "Bank of Montreal", "Royal Bank of Canada", "Toronto-Dominion Bank", "Scotiabank",
    "Banco do Brasil", "Itau Unibanco", "Banco Bradesco", "Banco Santander Brasil",
    "Navy Federal Credit Union", "Pentagon Federal Credit Union", "OneMain Financial",
    "CIT Group", "Ally Financial", "GE Capital", "Investec Bank", "Rabobank",
    "MetLife", "Prudential", "New York Life", "AIG Financial", "American Express", "Synchrony Financial",
    "Wachovia", "LaSalle Bank"
]

LENDER_ALIASES = {
    "wells fargo bank": "Wells Fargo",
    "wells fargo bank, n.a.": "Wells Fargo",
    "j.p. morgan securities": "JPMorgan Chase",
    "bank of america, n.a.": "Bank of America",
    "bb&t": "BB&T",
    "suntrust bank": "SunTrust",
    "mufg bank": "Mitsubishi UFJ Financial Group",
    "wachovia bank": "Wachovia",
    "la salle bank": "LaSalle Bank"
}

BLACKLIST = {"fasb", "eu", "u.s. financial accounting standards board", 
             "credit facility", "loan agreement", "administrative agent"}

KEY_PHRASES = [
    "Credit Agreement", "Revolving Credit", "Term Loan", "Note Purchase Agreement",
    "Credit Facility", "Financing Arrangements", "Loan Agreement", "Indenture",
    "Credit and Security", "Loan and Security", "Administrative Agent",
    "Syndication Agent", "Documentation Agent", "Arranger", "Co-Arranger",
    "Agent Bank", "between"
]

SEARCH_KEYWORDS = [
    r"\bbank\b", r"\bfinancial\b", r"\btrust\b", r"\bcapital\b", r"\bcredit\b",
    r"\binsurance\b", r"\bpartners\b", r"\bfund\b", r"\bsecurities\b", r"\blender\b",
    r"\bagent\b", r"\barranger\b", r"\bsyndicate\b", r"\bsyndication\b"
]

filing_cache = {}
unmatched_names = set()

def normalize_entity(name):
    name = name.lower()
    name = re.sub(r'[^\w\s]', '', name)
    name = re.sub(r'\b(inc|llc|ltd|plc|na|sa|corp|corporation|company|associates?)\b', '', name)
    return re.sub(r'\s+', ' ', name).strip()

def download_filing(filing_path):
    if filing_path in filing_cache:
        return filing_cache[filing_path]
    url = BASE_URL + filing_path
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    text = response.text
    filing_cache[filing_path] = text
    return text

def extract_snippets(html):
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator=" ")
    snippets = []
    for phrase in KEY_PHRASES:
        for match in re.finditer(re.escape(phrase), text, flags=re.IGNORECASE):
            start_idx = max(0, match.start() - 1000)
            end_idx = min(len(text), match.end() + 1000)
            snippet = text[start_idx:end_idx]
            snippets.append(snippet)
    return snippets if snippets else [text]

def extract_lenders(snippets):
    results = []
    for snippet in snippets:
        entities = ner_pipeline(snippet)
        for ent in entities:
            if ent["entity_group"] == "ORG":
                word = ent.get("word", "").strip()
                conf = round(ent.get("score", 0), 3)
                if (any(re.search(keyword, word.lower()) for keyword in SEARCH_KEYWORDS) and 
                    word.lower() not in BLACKLIST):
                    results.append((word, conf))
    return results

def validate_lender(name):
    normalized = normalize_entity(name)
    for alias, mapped in LENDER_ALIASES.items():
        if alias in normalized:
            return mapped
    for known in KNOWN_LENDERS:
        if normalize_entity(known) in normalized or normalized in normalize_entity(known):
            return known
    match = difflib.get_close_matches(normalized, [normalize_entity(k) for k in KNOWN_LENDERS], n=1, cutoff=0.9)
    if match:
        idx = [normalize_entity(k) for k in KNOWN_LENDERS].index(match[0])
        return KNOWN_LENDERS[idx]
    unmatched_names.add(name) 
    return None

def process_row(filename):
    try:
        html = download_filing(filename)
        snippets = extract_snippets(html)
        raw_lenders = extract_lenders(snippets)
        validated = []
        review_reasons = []
        for name, score in raw_lenders:
            validated_name = validate_lender(name)
            if validated_name:
                validated.append(validated_name)
            else:
                review_reasons.append(f"{name} (conf: {score})")
        return raw_lenders, validated, "; ".join(review_reasons)
    except Exception as e:
        logging.error(f"Failed to process {filename}: {e}")
        return [], [], str(e)

import math
import pandas as pd
import re
import requests
from bs4 import BeautifulSoup
import difflib
from transformers import pipeline
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import os

def batch_process(input_excel: str, chunk_size: int = 100):
    out_dir1 = "extracted_lenders"
    out_dir2 = "unmatched_lenders"
    df_full = pd.read_excel(input_excel)
    total = len(df_full)
    num_batches = math.ceil(total / chunk_size)

    for batch_idx in range(200, num_batches):
        start = batch_idx * chunk_size
        end   = min(start + chunk_size, total)
        df = df_full.iloc[start:end].copy()

        df["lender_name_raw"]       = None
        df["lender_name_validated"] = None
        df["manual_review"]         = False
        df["manual_review_reason"]  = None
        unmatched_names.clear()

        unique_files = df["filename"].dropna().unique()
        filing_results = {}

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = { executor.submit(process_row, fn): fn for fn in unique_files }
            for future in as_completed(futures):
                fn = futures[future]
                raw, validated, reason = future.result()
                filing_results[fn] = (raw, validated, reason)

        for idx, row in df.iterrows():
            fn = row["filename"]
            if pd.notna(fn) and fn in filing_results:
                raw, validated, reason = filing_results[fn]
                df.at[idx, "lender_name_raw"]       = "; ".join([r[0] for r in raw])
                df.at[idx, "lender_name_validated"] = "; ".join(validated)
                if not validated:
                    df.at[idx, "manual_review"]        = True
                    df.at[idx, "manual_review_reason"] = reason

        batch_num = batch_idx + 1
        out_excel = os.path.join(out_dir1, f"extracted_lenders_{batch_num}_updated.xlsx")
        out_csv   = os.path.join(out_dir2, f"unmatched_lender_names_{batch_num}.csv")

        df.to_excel(out_excel, index=False)
        pd.DataFrame(sorted(unmatched_names), columns=["unmatched_lender_name"])\
          .to_csv(out_csv, index=False)

        print(f"Batch {batch_num}/{num_batches} done: {out_excel}, {out_csv}")

if __name__ == '__main__':
    batch_process("CapitalIQ_final_sample_links.xlsx", chunk_size=100)