import glob
import os
import sys
import time
from typing import Dict, List, Tuple

# Ensure project root is importable so that multiprocessing can re-import `pipeline.classify`.
PROJECT_ROOT = os.path.dirname(__file__)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from pipeline.classify import classify_batch, classify_pdf, classify_pdf_debug


DEBUG = False
USE_METADATA = True

# Optional ground truth validation (fill in as needed).
ground_truth: Dict[str, str] = {
    # "example.pdf": "digital",
}


def _collect_pdf_files(data_dir: str) -> List[str]:
    pattern = os.path.join(data_dir, "**", "*.pdf")
    return sorted(glob.glob(pattern, recursive=True))


def _print_file_result(file_path: str, classification: str, prefix: str = "") -> None:
    rel = os.path.relpath(file_path, PROJECT_ROOT)
    if prefix:
        print(f"{prefix} {rel}: {classification}")
    else:
        print(f"{rel}: {classification}")


def run_basic_test(file_paths: List[str]) -> Tuple[Dict[str, str], Dict[str, int], float]:
    """Run classify_pdf on each file and print results."""
    results: Dict[str, str] = {}
    counts = {"digital": 0, "scanned": 0, "mixed": 0, "error": 0}

    start = time.perf_counter()
    for i, file_path in enumerate(file_paths, start=1):
        try:
            classification = classify_pdf(file_path)
        except Exception as exc:
            classification = "error"
            print(f"[basic_test] Exception for {file_path}: {exc}")

        results[file_path] = classification
        if classification not in counts:
            counts["error"] += 1
        else:
            counts[classification] += 1

        rel = os.path.relpath(file_path, PROJECT_ROOT)
        print(f"[{i:03d}] {rel} -> {classification}")

        if DEBUG:
            page_info = classify_pdf_debug(file_path)
            print(f"    [debug] pages={len(page_info)}")
            for page_number, has_text in page_info:
                page_class = "digital" if has_text else "scanned"
                print(f"        page {page_number}: {page_class}")

        if USE_METADATA:
            metadata = classify_pdf(file_path, return_metadata=True)
            classification_from_meta = metadata.get("classification")
            total_pages = metadata.get("total_pages")
            avg_cid_ratio = metadata.get("confidence_metrics", {}).get("avg_cid_ratio")
            avg_whitespace_ratio = metadata.get("confidence_metrics", {}).get("avg_whitespace_ratio")
            image_dominant_pages = metadata.get("confidence_metrics", {}).get("image_dominant_pages")

            print(
                "    [metadata] "
                f"classification={classification_from_meta} total_pages={total_pages} "
                f"avg_cid_ratio={avg_cid_ratio} avg_whitespace_ratio={avg_whitespace_ratio} "
                f"image_dominant_pages={image_dominant_pages}"
            )

    elapsed = time.perf_counter() - start
    return results, counts, elapsed


def run_ground_truth_validation(results: Dict[str, str]) -> None:
    """Compare predicted vs ground truth and print mismatches."""
    if not ground_truth:
        return

    mismatches = 0
    for file_path, predicted in results.items():
        rel = os.path.relpath(file_path, PROJECT_ROOT)
        # ground_truth keys are expected to be file names (not full paths) as in the prompt.
        key = os.path.basename(file_path)
        if key in ground_truth:
            expected = ground_truth[key]
            if predicted != expected:
                mismatches += 1
                print(
                    f"[ground_truth] mismatch: {rel} predicted={predicted} expected={expected}"
                )

    if mismatches == 0:
        print("[ground_truth] All labeled files matched.")
    else:
        print(f"[ground_truth] Total mismatches: {mismatches}")


def run_batch_test(file_paths: List[str], single_results: Dict[str, str]) -> None:
    """Run classify_batch and compare results with single-run classification."""
    print("[batch_test] Running classify_batch...")
    start = time.perf_counter()

    try:
        batch_results = classify_batch(file_paths)
    except Exception as exc:
        print(f"[batch_test] Exception during batch classification: {exc}")
        return

    elapsed = time.perf_counter() - start

    inconsistencies = 0
    for file_path in file_paths:
        predicted_single = single_results.get(file_path)
        predicted_batch = batch_results.get(file_path)
        if predicted_single != predicted_batch:
            inconsistencies += 1
            rel = os.path.relpath(file_path, PROJECT_ROOT)
            print(
                f"[batch_test] inconsistent: {rel} single={predicted_single} batch={predicted_batch}"
            )

    print(f"[batch_test] done in {elapsed:.2f}s inconsistencies={inconsistencies}")


def main() -> None:
    data_dir = os.path.join(PROJECT_ROOT, "data", "raw")
    file_paths = _collect_pdf_files(data_dir)
    if not file_paths:
        print(f"No PDF files found under: {os.path.relpath(data_dir, PROJECT_ROOT)}")
        return

    print(f"[test] Found {len(file_paths)} PDF files.")
    print(f"[test] DEBUG={DEBUG} USE_METADATA={USE_METADATA}")

    single_results, counts, elapsed = run_basic_test(file_paths)
    print(f"[test] single-run classification time: {elapsed:.2f}s")

    print("\nSummary:")
    for category in ["digital", "scanned", "mixed", "error"]:
        print(f"  {category}: {counts.get(category, 0)}")

    run_ground_truth_validation(single_results)

    run_batch_test(file_paths, single_results)


if __name__ == "__main__":
    main()

