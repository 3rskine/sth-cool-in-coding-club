# extract_quotes_diagnostic.py
# 診斷版本：加入詳細輸出來確認解析是否正確

import re
from pathlib import Path
import csv
import sys

# ----------------- CONFIG -----------------
BASE_FOLDER = "C:/Users/a7385/OneDrive/Desktop/程式接案/StockInfo"
OUT_CSV = "final_stock_prices_2020_20.csv"
OUT_DEBUG = "final_stock_prices_2020_20_debug.csv"
ENCODING = "cp950"
MAX_FILES_PER_YEAR = 20
YEAR_ONLY = "2020"
MIN_PRICE = 0.01
MAX_PRICE = 1_000_000.0
# ------------------------------------------

# S38 slice positions (0-index, end exclusive)
POS = {
    "stock_id": (0, 6),
    "stock_name": (6, 22),
    "open_price_raw": (22, 31),
    "high_price_raw": (31, 40),
    "low_price_raw": (40, 49),
    "close_price_raw": (49, 58),
    "change_flag": (58, 59)
}

date_header_re = re.compile(r'(\d{8})')

def parse_9digit_price(raw_str):
    """Parse S38 9-digit price (9(5)V9(4))."""
    if raw_str is None:
        return None
    s = str(raw_str)
    digits = re.sub(r'[^0-9]', '', s)
    if digits == "":
        return None
    
    if len(digits) < 9:
        digits = digits.zfill(9)
    else:
        digits = digits[-9:]
    
    try:
        whole = int(digits[:-4])
        frac = int(digits[-4:])
        value = whole + frac / 10000.0
        return round(value, 4)
    except Exception:
        return None

def sane_price(v):
    if v is None:
        return False
    try:
        v = float(v)
        return (v >= MIN_PRICE) and (v <= MAX_PRICE)
    except:
        return False

def derive_date_for_file(path: Path, year_hint=None):
    """Derive YYYYMMDD from file header or filename."""
    # Try header line first
    try:
        with path.open('r', encoding=ENCODING, errors='replace') as f:
            for _ in range(3):
                line = f.readline()
                if not line:
                    break
                m = date_header_re.search(line)
                if m:
                    return m.group(1)
    except Exception:
        pass

    # Try filename pattern
    m = re.search(r'\((\d{3,8})\)', path.name)
    if m:
        tag = m.group(1).zfill(4)
        if len(tag) == 4 and year_hint and re.match(r'^\d{4}$', year_hint):
            return year_hint + tag
        elif len(tag) == 8:
            return tag

    if year_hint and re.match(r'^\d{4}$', year_hint):
        return year_hint + "0101"
    return "19700101"

def process_file(path: Path, date_tag, rows, debug_rows, verbose=False):
    """Read and parse stock quote file."""
    fname = path.name
    
    if verbose:
        print(f"\n{'='*60}")
        print(f"Processing: {fname}")
        print(f"Date tag: {date_tag}")
    
    try:
        with path.open('r', encoding=ENCODING, errors='replace') as f:
            lines = f.readlines()
            
            if verbose:
                print(f"Total lines in file: {len(lines)}")
                print(f"\nFirst 3 lines preview:")
                for i, line in enumerate(lines[:3], 1):
                    print(f"  Line {i} (len={len(line.rstrip())}): {line[:100]}")
            
            # Skip first line (header/timestamp)
            data_lines = lines[1:]
            
            sample_count = 0
            for lineno, raw in enumerate(data_lines, start=2):
                line = raw.rstrip("\r\n")
                if not line.strip():
                    continue
                
                # Ensure minimum length
                min_len = POS['change_flag'][1]
                if len(line) < min_len:
                    line = line.ljust(min_len)
                    debug_rows.append({
                        "file": fname, 
                        "line_no": lineno, 
                        "reason": "padded_short_line", 
                        "line_preview": raw[:200],
                        "close_raw": ""
                    })
                    continue
                
                # Extract fields
                try:
                    sid = line[POS['stock_id'][0]:POS['stock_id'][1]].strip()
                    sname = line[POS['stock_name'][0]:POS['stock_name'][1]].strip()
                    close_raw = line[POS['close_price_raw'][0]:POS['close_price_raw'][1]]
                    change_sym = line[POS['change_flag'][0]:POS['change_flag'][1]]
                except Exception as e:
                    debug_rows.append({
                        "file": fname, 
                        "line_no": lineno, 
                        "reason": f"slice_error:{e}", 
                        "line_preview": raw[:200],
                        "close_raw": ""
                    })
                    continue
                
                # Show first few samples for debugging
                if verbose and sample_count < 5:
                    print(f"\n  Sample line {lineno} (stock_id={sid}):")
                    print(f"    Full line length: {len(line)}")
                    print(f"    Line: {line}")
                    print(f"    [0:6] stock_id: '{sid}'")
                    print(f"    [6:22] stock_name: '{sname}'")
                    print(f"    [22:31] open_raw: '{line[22:31]}'")
                    print(f"    [31:40] high_raw: '{line[31:40]}'")
                    print(f"    [40:49] low_raw: '{line[40:49]}'")
                    print(f"    [49:58] close_raw: '{close_raw}'")
                    print(f"    [58:59] change_flag: '{change_sym}'")
                    sample_count += 1
                
                # Parse closing price
                close_val = parse_9digit_price(close_raw)
                
                if verbose and sample_count <= 5:
                    print(f"    -> parsed close: {close_val}")
                
                # Validation
                if not sid:
                    debug_rows.append({
                        "file": fname, 
                        "line_no": lineno, 
                        "reason": "empty_stock_id", 
                        "line_preview": raw[:200],
                        "close_raw": close_raw
                    })
                    continue
                
                if close_val is None:
                    debug_rows.append({
                        "file": fname, 
                        "line_no": lineno, 
                        "reason": "close_unparseable", 
                        "line_preview": raw[:200],
                        "close_raw": close_raw
                    })
                    continue
                
                if not sane_price(close_val):
                    debug_rows.append({
                        "file": fname, 
                        "line_no": lineno, 
                        "reason": f"close_out_of_bounds:{close_val}", 
                        "line_preview": raw[:200],
                        "close_raw": close_raw
                    })
                    continue
                
                # Valid row
                rows.append({
                    "date": date_tag,
                    "stock_id": sid,
                    "closing_price": f"{close_val:.4f}",
                    "change_flag": change_sym.strip(),
                    "stock_name": sname,
                    "source_file": fname,
                    "line_no": lineno
                })
                
    except Exception as e:
        debug_rows.append({
            "file": fname, 
            "line_no": 0, 
            "reason": f"file_read_error:{e}", 
            "line_preview": "",
            "close_raw": ""
        })

def main():
    root = Path(BASE_FOLDER)
    if not root.exists():
        print(f"Error: BASE_FOLDER not found: {root}")
        sys.exit(1)
    
    all_rows = []
    debug_rows = []
    files_found = 0
    
    # Process year folders
    for sub in sorted(root.iterdir()):
        if YEAR_ONLY and sub.is_dir() and sub.name != YEAR_ONLY:
            continue
        
        if sub.is_dir():
            year_hint = sub.name if re.match(r'^\d{4}$', sub.name) else None
            files = sorted([
                p for p in sub.iterdir() 
                if p.is_file() and (
                    p.name.upper().startswith("STKT2QUOTESN") or 
                    p.name.upper().startswith("STKWQUOTES")
                )
            ])
            
            if MAX_FILES_PER_YEAR:
                files = files[:MAX_FILES_PER_YEAR]
            
            for i, f in enumerate(files):
                files_found += 1
                date_tag = derive_date_for_file(f, year_hint=year_hint)
                # Show verbose output for first 2 files only
                verbose = (i < 2)
                process_file(f, date_tag, all_rows, debug_rows, verbose=verbose)
    
    if files_found == 0:
        print(f"No STKT2QUOTESN / STKWQUOTES files found under {root}")
        sys.exit(0)
    
    # Write main CSV
    out_path = Path.cwd() / OUT_CSV
    fieldnames = ["date", "stock_id", "closing_price", "change_flag", "stock_name", "source_file", "line_no"]
    with out_path.open('w', newline='', encoding='utf-8-sig') as outf:
        writer = csv.DictWriter(outf, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)
    
    # Write debug CSV
    dbg_path = Path.cwd() / OUT_DEBUG
    dbg_fields = ["file", "line_no", "reason", "line_preview", "close_raw"]
    with dbg_path.open('w', newline='', encoding='utf-8-sig') as dbgout:
        writer = csv.DictWriter(dbgout, fieldnames=dbg_fields)
        writer.writeheader()
        writer.writerows(debug_rows)
    
    # Summary
    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"{'='*60}")
    print(f"Processed files: {files_found}")
    print(f"Valid records: {len(all_rows)} -> {out_path}")
    print(f"Debug rows: {len(debug_rows)} -> {dbg_path}")
    
    if len(all_rows) > 0:
        debug_ratio = len(debug_rows) / (len(all_rows) + len(debug_rows)) * 100
        print(f"Debug ratio: {debug_ratio:.2f}%")
        
        # Show sample records
        print(f"\nFirst 5 valid records:")
        for i, row in enumerate(all_rows[:5]):
            print(f"  {i+1}. {row['date']} | {row['stock_id']:6s} | {row['closing_price']:>10s} | {row['change_flag']}")
        
        # Check for known example
        example_1240 = [r for r in all_rows if r['stock_id'] == '1240']
        if example_1240:
            print(f"\nFound {len(example_1240)} records for stock_id='1240':")
            for r in example_1240[:3]:
                print(f"  Date: {r['date']}, Close: {r['closing_price']}, Flag: {r['change_flag']}")
    
    if len(debug_rows) > 0:
        print(f"\nTop 5 debug reasons:")
        from collections import Counter
        reasons = Counter(d['reason'] for d in debug_rows)
        for reason, count in reasons.most_common(5):
            print(f"  {reason}: {count}")

if __name__ == "__main__":
    main()