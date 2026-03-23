"""PDF table extraction utilities.

This module extracts tables from machine-readable government PDF documents
using both `pdfplumber` and (optionally) `camelot`, then selects the best
result using a simple scoring heuristic.

Only extraction is handled here; downstream steps (cleaning, validation,
structure mapping) belong to other pipeline modules.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple, Union

import gc
import time
import pandas as pd
import pdfplumber

try:
    import camelot  # type: ignore
except ImportError:  # pragma: no cover - depends on environment
    camelot = None


STRUCTURE_YEAR_RE = re.compile(r"^\d{4}$")
GENERIC_HEADER_KEYWORDS = ("budget", "estimate", "actual", "revised")
SPECIAL_COLS = {"page", "is_structure"}


def _is_blank_cell(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    return False


def _clean_header_cell(value: Any) -> str:
    """Convert a cell to a header-safe string."""
    if _is_blank_cell(value):
        return ""
    return str(value).replace("\n", " ").replace("\r", " ").strip()


def _normalize_column_name(name: Any) -> str:
    """Normalize a column name for comparison and readability."""
    if name is None:
        return ""
    return str(name).replace("\n", " ").replace("\r", " ").strip()


def _row_has_generic_headers(row: List[str]) -> bool:
    """Heuristic: detect "generic" first header row (Budget/Estimate/etc)."""
    non_empty = [cell for cell in row if cell and cell.strip()]
    if not non_empty:
        return False

    lower_cells = [cell.lower() for cell in non_empty]
    matches = sum(
        1 for cell in lower_cells if any(keyword in cell for keyword in GENERIC_HEADER_KEYWORDS)
    )

    # Require more than a single incidental match.
    threshold = max(1, len(non_empty) // 4)
    return matches >= threshold


def _looks_like_year_row(row: List[str]) -> bool:
    """Heuristic: detect rows dominated by 4-digit year-like values."""
    cells = [c.strip() for c in row if c and c.strip()]
    if not cells:
        return False
    year_like = sum(1 for c in cells if STRUCTURE_YEAR_RE.match(c))
    # If at least ~25% of non-empty cells look like years.
    return year_like >= max(1, len(cells) // 4)


def _construct_header(rows: List[List[str]]) -> Tuple[List[str], List[List[str]]]:
    """Construct a (possibly multi-line) header and return (header, data_rows).

    Rules:
    - Inspect the first 2–3 rows.
    - If the first row contains generic headers (Budget/Estimate/etc),
      combine up to 2 rows, or up to 3 if a third line looks year-like.
    - Join header rows column-wise: "Budget" + "2024" -> "Budget 2024".
    """
    if not rows:
        return [], []

    num_cols = len(rows[0])
    max_header_rows = min(3, len(rows))

    header_row_count = 1
    if max_header_rows >= 2 and _row_has_generic_headers(rows[0]):
        header_row_count = 2
        if max_header_rows >= 3 and (_looks_like_year_row(rows[1]) or _looks_like_year_row(rows[2])):
            header_row_count = 3

    header_rows = rows[:header_row_count]
    data_rows = rows[header_row_count:]

    header: List[str] = []
    for col_idx in range(num_cols):
        parts: List[str] = []
        for r in header_rows:
            cell = r[col_idx] if col_idx < len(r) else ""
            cell_clean = _normalize_column_name(cell)
            if cell_clean:
                parts.append(cell_clean)

        combined = " ".join(parts).strip()
        header.append(combined if combined else f"column_{col_idx + 1}")

    return header, data_rows


def _rows_to_dataframe(
    table_rows: Any, *, page_num: Optional[int] = None
) -> Optional[pd.DataFrame]:
    """Convert list-of-rows table data into a DataFrame.

    The first row is treated as the header; remaining rows are data.
    """
    if not table_rows or not isinstance(table_rows, list):
        return None
    if len(table_rows) < 2:
        return None

    # pdfplumber sometimes produces uneven rows.
    row_lengths = [len(r) for r in table_rows if isinstance(r, list)]
    if not row_lengths:
        return None
    num_cols = max(row_lengths)
    if num_cols < 2:
        return None

    def _pad_row(r: Any) -> List[Any]:
        if not isinstance(r, list):
            r = []
        padded = list(r[:num_cols])
        if len(padded) < num_cols:
            padded.extend([None] * (num_cols - len(padded)))
        return padded

    def _to_str_cell(v: Any) -> str:
        if v is None:
            return ""
        if pd.isna(v):
            return ""
        return str(v).replace("\n", " ").replace("\r", " ").strip()

    padded_rows: List[List[Any]] = [_pad_row(r) for r in table_rows]
    rows_str: List[List[str]] = [[_to_str_cell(v) for v in row] for row in padded_rows]

    header, data_rows_str = _construct_header(rows_str)
    if not header or not data_rows_str:
        return None

    df = pd.DataFrame(data_rows_str, columns=header)

    # Convert blank strings to NA for "empty table" detection.
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].map(lambda v: pd.NA if _is_blank_cell(v) else v)

    df.dropna(axis=0, how="all", inplace=True)
    df.dropna(axis=1, how="all", inplace=True)
    if df.empty:
        return None

    # Minor improvements: skip tiny/junk tables.
    if df.shape[1] <= 1 or df.shape[0] < 2:
        return None

    df.columns = [_normalize_column_name(c) for c in df.columns]

    # Head-of-account detection: if first column value is exactly 4 digits, tag the row.
    first_col = df.columns[0]
    is_structure: List[bool] = []
    for v in df[first_col].tolist():
        s = "" if pd.isna(v) else str(v).strip()
        is_structure.append(bool(STRUCTURE_YEAR_RE.match(s)))
    df["is_structure"] = is_structure

    if page_num is not None:
        df["page"] = int(page_num)

    return df


def extract_with_pdfplumber(file_path: str) -> List[pd.DataFrame]:
    """Extract tables from each page using `pdfplumber`.

    For each page, `page.extract_table()` is attempted. The first row becomes
    the header and the remaining rows are treated as data.

    Returns:
        List of extracted tables as pandas DataFrames. Each DataFrame includes
        a `page` column (1-based page number).
    """
    tables: List[pd.DataFrame] = []

    try:
        print("Running pdfplumber...")
        with pdfplumber.open(file_path) as pdf:
            for page_index, page in enumerate(pdf.pages, start=1):
                try:
                    raw_table = page.extract_table()
                    df = _rows_to_dataframe(raw_table, page_num=page_index)
                    if df is not None:
                        tables.append(df)
                except Exception as exc:
                    # Continue extracting other pages even if one fails.
                    print(f"[extract_with_pdfplumber] page {page_index} failed: {exc}")
    except PermissionError:
        # Let callers retry (WinError 32 can happen when temp artifacts
        # are still locked by previous extraction processes).
        raise
    except Exception as exc:
        print(f"[extract_with_pdfplumber] Failed to read '{file_path}': {exc}")

    return tables


def safe_extract_with_camelot(file_path: str) -> List[pd.DataFrame]:
    """Extract tables with `camelot` and ensure temp-file cleanup on Windows.

    Camelot/ghostscript may create temporary files that can remain locked on
    Windows (PermissionError: WinError 32). This wrapper aggressively releases
    camelot objects and triggers GC to reduce lingering locks.

    NOTE: Extraction should be run sequentially (do not call this concurrently
    from multiple threads/processes).
    """
    if camelot is None:
        print("[extract_with_camelot] camelot not installed; skipping.")
        return []

    def _has_enough_columns(
        tables: List[pd.DataFrame], *, min_effective_cols: int = 3
    ) -> bool:
        """Whether at least one table has enough non-special columns."""
        for df in tables:
            if df is None or df.empty:
                continue
            effective_cols = [c for c in df.columns if c not in SPECIAL_COLS]
            if len(effective_cols) >= min_effective_cols:
                return True
        return False

    def _extract_camelot_flavor(flavor: str) -> List[pd.DataFrame]:
        """Extract camelot tables for one flavor with cleanup.

        Cleanup is performed in `finally` to ensure temporary artifacts are
        released even if extraction fails.
        """
        tables: Any = None
        try:
            tables = camelot.read_pdf(file_path, pages="all", flavor=flavor)

            extracted: List[pd.DataFrame] = []
            for table in tables:
                # camelot table.df includes both header and data rows.
                raw_df = getattr(table, "df", None)
                if (
                    raw_df is None
                    or not isinstance(raw_df, pd.DataFrame)
                    or raw_df.empty
                ):
                    continue

                if raw_df.shape[0] < 2 or raw_df.shape[1] < 2:
                    continue

                raw_rows: List[List[Any]] = raw_df.values.tolist()

                page_num: Optional[int] = None
                table_page = getattr(table, "page", None)
                try:
                    # camelot often exposes page as an int-like string.
                    if table_page is not None:
                        page_num = int(table_page)
                except (TypeError, ValueError):
                    page_num = None

                df = _rows_to_dataframe(raw_rows, page_num=page_num)
                if df is not None:
                    extracted.append(df)

                # Drop references as we go to reduce object lifetime.
                try:
                    del raw_df
                except Exception:
                    pass

            return extracted
        except PermissionError:
            # Preserve PermissionError for callers to retry.
            raise
        except Exception as exc:
            print(f"[extract_with_camelot] {flavor} mode failed: {exc}")
            return []
        finally:
            # Required cleanup to reduce Windows temp-file locking issues.
            print("Cleaning up resources...")
            try:
                if tables is not None:
                    del tables
            except Exception:
                pass
            import gc
            import time

            gc.collect()
            time.sleep(0.2)

    try:
        print("Running camelot...")

        stream_tables = _extract_camelot_flavor("stream")
        stream_score = score_tables(stream_tables)
        stream_ok = (
            bool(stream_tables)
            and stream_score >= 50
            and _has_enough_columns(stream_tables)
        )

        if stream_ok:
            return stream_tables

        # Retry with lattice if stream output is missing/low quality.
        lattice_tables = _extract_camelot_flavor("lattice")
        lattice_score = score_tables(lattice_tables)

        if not lattice_tables:
            return stream_tables

        return lattice_tables if lattice_score >= stream_score else stream_tables
    except PermissionError:
        # Ensure caller can retry, while cleanup above still runs.
        raise
    except Exception as exc:
        print(f"[extract_with_camelot] camelot extraction failed: {exc}")
        return []
    finally:
        # Outer cleanup as a backstop.
        print("Cleaning up resources...")
        import gc
        import time

        gc.collect()
        time.sleep(0.2)


def extract_with_camelot(file_path: str) -> List[pd.DataFrame]:
    """Extract tables from a PDF using `camelot`.

    Thin wrapper around :func:`safe_extract_with_camelot` to keep the original
    API stable.
    """
    return safe_extract_with_camelot(file_path)


def score_tables(tables: List[pd.DataFrame]) -> int:
    """Score extracted tables by (rows × effective_columns) with heuristics.

    Empty tables are ignored.
    """
    total_score = 0
    for df in tables:
        if df is None or df.empty:
            continue

        df_nonempty = df.dropna(how="all")
        if df_nonempty.empty:
            continue

        rows = int(df_nonempty.shape[0])
        effective_cols = [c for c in df_nonempty.columns if c not in SPECIAL_COLS]
        cols = len(effective_cols)

        if rows < 2 or cols <= 1:
            continue

        if cols < 3:
            penalty = 0.5
        elif 5 <= cols <= 8:
            penalty = 1.5
        else:
            penalty = 1.0

        total_score += int(rows * cols * penalty)
    return total_score


def choose_best_tables(
    pdfplumber_tables: List[pd.DataFrame], camelot_tables: List[pd.DataFrame]
) -> List[pd.DataFrame]:
    """Choose whichever extraction method produced the better-scoring tables."""
    if not pdfplumber_tables:
        return camelot_tables
    if not camelot_tables:
        return pdfplumber_tables

    pdf_score = score_tables(pdfplumber_tables)
    camelot_score = score_tables(camelot_tables)

    return pdfplumber_tables if pdf_score >= camelot_score else camelot_tables


def merge_consecutive_tables(tables: List[pd.DataFrame]) -> List[pd.DataFrame]:
    """Merge consecutive tables when their column names are highly similar.

    Intended for multi-page tables where extraction yields one DataFrame per
    page but columns are consistent.
    """
    if not tables:
        return []

    def _normalized_signature(df: pd.DataFrame) -> List[str]:
        cols = [c for c in df.columns if c != "page"]
        return [_normalize_column_name(c).lower() for c in cols]

    def _similar(sig_a: List[str], sig_b: List[str]) -> bool:
        if not sig_a or not sig_b:
            return False
        set_a, set_b = set(sig_a), set(sig_b)
        if not set_a or not set_b:
            return False
        jaccard = len(set_a & set_b) / max(len(set_a | set_b), 1)
        # Require some structural consistency and a high overlap.
        return jaccard >= 0.85 and len(set_a) >= 2 and len(set_b) >= 2

    merged: List[pd.DataFrame] = []
    current = tables[0]
    current_sig = _normalized_signature(current)

    for nxt in tables[1:]:
        nxt_sig = _normalized_signature(nxt)
        if _similar(current_sig, nxt_sig):
            # Align columns to avoid concat errors when `page` is missing/present.
            if list(nxt.columns) != list(current.columns):
                nxt_aligned = nxt.reindex(columns=current.columns)
            else:
                nxt_aligned = nxt

            current = pd.concat([current, nxt_aligned], ignore_index=True)
        else:
            merged.append(current)
            current = nxt
            current_sig = nxt_sig

    merged.append(current)
    return merged


def extract_tables(
    file_path: str, return_metadata: bool = False
) -> Union[List[pd.DataFrame], Dict[str, Any]]:
    """Extract tables from `file_path` using pdfplumber and camelot.

    Extraction is performed with both methods (camelot may be skipped if it is
    not installed). The output set with the higher `score_tables(...)` score
    is returned.

    Args:
        file_path: Path to the PDF document.
        return_metadata: If True, return a dictionary with additional metadata.

    Returns:
        - If `return_metadata=False`: List of extracted tables (DataFrames).
        - If `return_metadata=True`: Dictionary with keys:
          `tables`, `method_used`, `num_tables`, `total_cells`.
    """
    # Extraction should be run sequentially to avoid Windows temp-file
    # locking conflicts (do not call this function concurrently).
    pdfplumber_tables: List[pd.DataFrame]
    camelot_tables: List[pd.DataFrame]

    for attempt in range(2):
        try:
            pdfplumber_tables = extract_with_pdfplumber(file_path)
            camelot_tables = extract_with_camelot(file_path)
            break
        except PermissionError as exc:
            print(
                f"[extract_tables] PermissionError during extraction attempt {attempt + 1}/2: {exc}"
            )
            time.sleep(0.5)
            if attempt == 1:
                raise

    pdf_score = score_tables(pdfplumber_tables)
    camelot_score = score_tables(camelot_tables)
    best_tables: List[pd.DataFrame]
    method_used: str
    if not pdfplumber_tables:
        method_used = "camelot"
        best_tables = camelot_tables
    elif not camelot_tables:
        method_used = "pdfplumber"
        best_tables = pdfplumber_tables
    else:
        method_used = "pdfplumber" if pdf_score >= camelot_score else "camelot"
        best_tables = (
            pdfplumber_tables if method_used == "pdfplumber" else camelot_tables
        )

    best_tables = merge_consecutive_tables(best_tables)

    print(
        f"[extract_tables] pdfplumber tables={len(pdfplumber_tables)} "
        f"camelot tables={len(camelot_tables)} chosen={method_used}"
    )

    if not return_metadata:
        return best_tables

    total_cells = 0
    for df in best_tables:
        if df is None or df.empty:
            continue
        total_cells += int(df.shape[0]) * int(df.shape[1])

    return {
        "tables": best_tables,
        "method_used": method_used,
        "num_tables": len(best_tables),
        "total_cells": total_cells,
    }


def extract_pdf_tables(file_path: str, return_metadata: bool = False):
    """Compatibility wrapper for older pipeline code."""
    return extract_tables(file_path, return_metadata=return_metadata)
