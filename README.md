# 📊 Budget Extraction Pipeline (UP Budget 2024)

## 🚀 Project Overview

This project aims to build a **robust data extraction pipeline** for analyzing **budget utilization in Uttar Pradesh (UP) for 2024**.

Government budget documents are highly inconsistent and come in multiple formats:

* Machine-readable PDFs
* Scanned PDFs
* Hindi documents (often non-Unicode encoded)
* Mixed-format documents

To handle this, we designed a **modular pipeline** that processes documents step by step.

---

# 🧱 Pipeline Architecture (Current)

```text
PDF Documents
    ↓
[1] Classification
    ↓
[2] Extraction
    ↓
[3] Parsing (Not Started)
    ↓
[4] Cleaning (Not Started)
    ↓
[5] Final Dataset
```

---

# ✅ Work Completed So Far

## 🔹 1. Document Classification (`classify.py`)

### Objective:

Automatically classify PDFs based on their structure.

### Categories:

* **Digital** → machine-readable text
* **Scanned** → image-based PDFs
* **Mixed** → combination of both

### Features Implemented:

* Text-based classification using pdfplumber
* Detection of:

  * CID encoding issues (`(cid:xx)`)
  * Image-heavy pages
  * Low-quality OCR-like text (whitespace ratio)
* Page sampling for faster classification
* Batch processing using multiprocessing

---

## 🔹 2. Table Extraction (`extract_pdf.py`)

### Objective:

Extract tabular data from machine-readable PDFs.

### Approach:

Dual extraction strategy:

* `pdfplumber` → handles messy/unstructured tables
* `camelot` → handles structured tables

### Key Features:

* Adaptive Camelot (stream → lattice fallback)
* Multi-line header reconstruction
* Table cleaning:

  * remove empty rows/columns
  * normalize structure
* Multi-page table merging
* Scoring system to choose best extraction method
* Windows-safe handling (fix for temp file locking issues)

---

## 🔹 3. Extraction Testing (`test_extract.py`)

### Objective:

Validate extraction results.

### Capabilities:

* Filters only digital/mixed PDFs
* Runs extraction pipeline
* Outputs:

  * number of tables
  * table shapes
* Saves extracted tables as CSV files
* Compares pdfplumber vs camelot outputs
* Measures execution time

---

## 📂 Sample Data

This repository includes a small sample to demonstrate the pipeline.

### 🔹 Sample Output

Location:

```text
output/debug_budget_ka_saar24_25.csv
```

This file shows extracted tabular data from a UP budget PDF.

### 📌 Notes

* Text may appear encoded due to Hindi font issues (Kruti Dev)
* Numeric values are correctly extracted
* Parsing and structuring will be handled in the next stage

---

# ⚠️ Problems Faced

## 🔴 1. Hindi Encoding Issue (Kruti Dev Fonts)

### Problem:

Extracted Hindi text appears like:

```text
okf"kZd
```

Instead of:

```text
वार्षिक
```

### Cause:

* PDFs use **legacy font encoding (Kruti Dev)**
* Text is not stored as Unicode

### Impact:

* Extracted text is unreadable
* Standard tools (pdfplumber, camelot) cannot decode it

---

## 🔴 2. Inconsistent PDF Formats

Different PDFs use different structures:

* Numbered rows (`1-`, `2-`)
* Numeric codes (`2202`, `3053`)
* Text-only rows
* Multi-line broken rows

### Impact:

* No single extraction or parsing method works for all files

---

## 🔴 3. Table Extraction Challenges

### Issues observed:

* Misaligned columns
* Broken headers
* Tables split across pages
* Some pages with no detectable tables

### Current Handling:

* Dual extraction strategy
* Scoring-based selection
* Table merging

---

## 🔴 4. Windows File Locking Issue

### Error:

```text
PermissionError: WinError 32
```

### Cause:

* Temporary files locked by pdfplumber/camelot

### Fix:

* Proper file handling (`with` statements)
* Garbage collection
* Small delays between operations

---

## 🔴 5. Mixed Document Types

Many PDFs contain:

* English (digital) sections
* Hindi (encoded or scanned) sections

### Impact:

* Partial extraction success
* Inconsistent outputs

---

# 🧠 Current Status

| Stage                           | Status            |
| ------------------------------- | ----------------- |
| Classification                  | ✅ Completed       |
| Extraction (Digital PDFs)       | ✅ Completed       |
| Extraction (Scanned PDFs / OCR) | ❌ Not implemented |
| Parsing                         | ❌ Not started     |
| Cleaning                        | ❌ Not started     |
| Final Dataset                   | ❌ Not created     |

---

# 🚀 Next Steps

## 🔹 1. Parsing (Next Step)

* Convert extracted tables into structured data
* Identify hierarchy (codes, categories)

---

## 🔹 2. Data Cleaning

* Normalize numeric values
* Fix column inconsistencies
* Handle missing values

---

## 🔹 3. OCR Pipeline (`extract_ocr.py`)

* Process scanned and encoded Hindi PDFs
* Use `pytesseract` for Hindi text extraction
* Reconstruct tables from OCR output

---

## 🔹 4. Dataset Integration

* Combine data from multiple PDFs
* Create a unified dataset for analysis

---

# 🧠 Key Learnings

* Government PDFs are highly inconsistent
* Extraction requires multiple tools and strategies
* Hindi encoding (Kruti Dev) is a major challenge
* Testing and validation are critical
* Building a modular pipeline is essential

---

# 📌 Summary

So far, we have built a **robust classification and extraction system** that can:

* Process multiple PDF formats
* Extract tabular data reliably
* Prepare data for further processing

The next phase focuses on:
👉 **structuring and cleaning the extracted data for analysis**

---

## 👨‍💻 Project Status

🟢 Extraction Pipeline: Completed
🟡 Data Structuring: Pending
🔴 OCR + Hindi Handling: Pending

---
