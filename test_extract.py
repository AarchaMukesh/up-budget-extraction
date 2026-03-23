import os
import time
import pandas as pd

from pipeline.classify import classify_pdf
from pipeline.extract_pdf import (
    extract_tables,
    extract_with_pdfplumber,
    extract_with_camelot,
    score_tables,
)


# -------------------------------
# Load only digital/mixed PDFs
# -------------------------------
def load_files(data_dir: str = os.path.join("data", "raw")) -> list[str]:
    pdf_files: list[str] = []

    if not os.path.isdir(data_dir):
        print(f"[load_files] Missing directory: {data_dir}")
        return pdf_files

    for root, _dirs, files in os.walk(data_dir):
        for file_name in files:
            if not file_name.lower().endswith(".pdf"):
                continue

            file_path = os.path.join(root, file_name)

            try:
                classification = classify_pdf(file_path)
            except Exception as exc:
                print(f"[load_files] classify_pdf failed for '{file_name}': {exc}")
                continue

            if classification in ("digital", "mixed"):
                pdf_files.append(file_path)

    return sorted(pdf_files)


# -------------------------------
# Compare extraction methods
# -------------------------------
def compare_methods(file_path: str) -> tuple[int, int]:
    try:
        pdf_tables = extract_with_pdfplumber(file_path)
        camelot_tables = extract_with_camelot(file_path)

        score_pdf = score_tables(pdf_tables)
        score_camelot = score_tables(camelot_tables)

        print(f"  [compare] pdfplumber={score_pdf} | camelot={score_camelot}")

        return score_pdf, score_camelot

    except Exception as exc:
        print(f"  [compare] ERROR: {exc}")
        return 0, 0


# -------------------------------
# Main extraction test
# -------------------------------
def run_extraction_test(pdf_files: list[str], output_dir: str = "output") -> None:
    os.makedirs(output_dir, exist_ok=True)

    total_start = time.perf_counter()

    for idx, file_path in enumerate(pdf_files, start=1):
        file_name = os.path.basename(file_path)
        base_name = os.path.splitext(file_name)[0]

        file_start = time.perf_counter()

        try:
            result = extract_tables(file_path, return_metadata=True)

            tables = result["tables"]
            method_used = result["method_used"]
            num_tables = result["num_tables"]
            total_cells = result["total_cells"]

            print(f"[{idx}] {file_name}")
            print(f"  → method={method_used} | tables={num_tables} | cells={total_cells}")

            if not tables:
                print("  ⚠️ No tables extracted")

            # Inspect tables
            for t_idx, df in enumerate(tables):
                if df is None or not isinstance(df, pd.DataFrame) or df.empty:
                    continue

                rows, cols = df.shape
                print(f"  table[{t_idx}] shape=({rows}, {cols})")
                print(df.head(3).to_string(index=False))

            # Save first table for manual inspection
            if tables:
                first_df = tables[0]
                if first_df is not None and not first_df.empty:
                    out_path = os.path.join(output_dir, f"debug_{base_name}.csv")
                    first_df.to_csv(out_path, index=False)

            # Prevent Windows file locking issues
            time.sleep(0.3)

            # Compare extraction methods
            compare_methods(file_path)

        except Exception as exc:
            print(f"[{idx}] {file_name} ERROR: {exc}")

        finally:
            elapsed = time.perf_counter() - file_start
            print(f"  time: {elapsed:.2f}s\n")

    total_elapsed = time.perf_counter() - total_start
    print(f"Total execution time: {total_elapsed:.2f}s")


# -------------------------------
# Entry point
# -------------------------------
def main() -> None:
    pdf_files = load_files()

    if not pdf_files:
        print("[main] No PDFs found (digital/mixed) in data/raw.")
        return

    print(f"[main] Found {len(pdf_files)} valid PDFs\n")

    run_extraction_test(pdf_files)


if __name__ == "__main__":
    main()