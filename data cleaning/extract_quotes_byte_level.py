# extract_quotes_byte_level.py
# 使用 byte-level slicing 來正確解析 CP950 編碼的固定寬度檔案

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

# S38 BYTE positions (for CP950 encoded bytes)
BYTE_POS = {
    "stock_id": (0, 6),
    "stock_name": (6, 22),
    "open_price_raw": (22, 31),
    "high_price_raw": (31, 40),
    "low_price_raw": (40, 49),
    "close_price_raw": (49, 58),
    "change_flag": (58, 59)
}

date_header_re = re.compile(r'(\d{8})')

def slice_bytes(line_bytes, start, end):
    """Extract bytes from start to end position and decode to string."""
    try:
        return line_bytes[start:end].decode(ENCODING, errors='replace').strip()
    except:
        return ""

def parse_9digit_price(raw_str):
    """Parse S38 9-digit price (9(5)V9(4))."""
    if not raw_str:
        return None
    
    digits = re.sub(r'[^0-9]', '', raw_str)
    if not digits:
        return None
    
    # Pad or truncate to 9 digits
    if len(digits) < 9:
        digits = digits.zfill(9)
    else:
        digits = digits[-9:]
    
    try:
        whole = int(digits[:-4])
        frac = int(digits[-4:])
        value = whole + frac / 10000.0
        return round(value, 4)
    except:
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
    try:
        with path.open('r', encoding=ENCODING, errors='replace') as f:
            for _ in range(3):
                line = f.readline()
                if not line:
                    break
                m = date_header_re.search(line)
                if m:
                    return m.group(1)
    except:
        pass

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
    """Read and parse stock quote file using byte-level slicing."""
    fname = path.name
    
    if verbose:
        print(f"\n{'='*60}")
        print(f"Processing: {fname}")
        print(f"Date tag: {date_tag}")
    
    try:
        # Read file as binary first
        with path.open('rb') as f:
            lines_bytes = f.readlines()
        
        if verbose:
            print(f"Total lines in file: {len(lines_bytes)}")
        
        # Skip first line (header)
        sample_count = 0
        for lineno, line_bytes in enumerate(lines_bytes[1:], start=2):
            # Remove line endings
            line_bytes = line_bytes.rstrip(b'\r\n')
            
            if len(line_bytes) == 0:
                continue
            
            # Check minimum length
            min_len = BYTE_POS['change_flag'][1]
            if len(line_bytes) < min_len:
                debug_rows.append({
                    "file": fname,
                    "line_no": lineno,
                    "reason": "line_too_short",
                    "line_preview": line_bytes[:100].decode(ENCODING, errors='replace'),
                    "close_raw": ""
                })
                continue
            
            # Extract fields using byte positions
            try:
                sid = slice_bytes(line_bytes, *BYTE_POS['stock_id'])
                sname = slice_bytes(line_bytes, *BYTE_POS['stock_name'])
                close_raw = slice_bytes(line_bytes, *BYTE_POS['close_price_raw'])
                change_sym_bytes = line_bytes[BYTE_POS['change_flag'][0]:BYTE_POS['change_flag'][1]]
                change_sym = change_sym_bytes.decode(ENCODING, errors='replace')
            except Exception as e:
                debug_rows.append({
                    "file": fname,
                    "line_no": lineno,
                    "reason": f"slice_error:{e}",
                    "line_preview": line_bytes[:100].decode(ENCODING, errors='replace'),
                    "close_raw": ""
                })
                continue
            
            # Verbose output for first samples
            if verbose and sample_count < 5:
                line_str = line_bytes.decode(ENCODING, errors='replace')
                print(f"\n  Sample line {lineno} (stock_id={sid}):")
                print(f"    Line byte length: {len(line_bytes)}")
                print(f"    First 80 bytes: {line_bytes[:80]}")
                print(f"    Decoded line: {line_str[:80]}")
                print(f"    [0:6] stock_id: '{sid}'")
                print(f"    [6:22] stock_name: '{sname}'")
                
                open_raw = slice_bytes(line_bytes, *BYTE_POS['open_price_raw'])
                high_raw = slice_bytes(line_bytes, *BYTE_POS['high_price_raw'])
                low_raw = slice_bytes(line_bytes, *BYTE_POS['low_price_raw'])
                
                print(f"    [22:31] open_raw: '{open_raw}'")
                print(f"    [31:40] high_raw: '{high_raw}'")
                print(f"    [40:49] low_raw: '{low_raw}'")
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
                    "line_preview": line_bytes[:100].decode(ENCODING, errors='replace'),
                    "close_raw": close_raw
                })
                continue
            
            if close_val is None:
                debug_rows.append({
                    "file": fname,
                    "line_no": lineno,
                    "reason": "close_unparseable",
                    "line_preview": line_bytes[:100].decode(ENCODING, errors='replace'),
                    "close_raw": close_raw
                })
                continue
            
            if not sane_price(close_val):
                debug_rows.append({
                    "file": fname,
                    "line_no": lineno,
                    "reason": f"close_out_of_bounds:{close_val}",
                    "line_preview": line_bytes[:100].decode(ENCODING, errors='replace'),
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
                verbose = (i < 2)  # Show details for first 2 files
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
        
        print(f"\nFirst 10 valid records:")
        for i, row in enumerate(all_rows[:10]):
            print(f"  {i+1}. {row['date']} | {row['stock_id']:6s} | {row['stock_name']:10s} | {row['closing_price']:>10s} | {row['change_flag']}")
        
        # Check for known example (1240 should be around 39.80 on 2020/04/20)
        example_1240 = [r for r in all_rows if r['stock_id'] == '1240' and r['date'] == '20200420']
        if example_1240:
            print(f"\n✓ Found stock_id='1240' on 2020/04/20:")
            for r in example_1240:
                print(f"  Close: {r['closing_price']} (expected ~39.80)")
    
    if len(debug_rows) > 0:
        print(f"\nTop 5 debug reasons:")
        from collections import Counter
        reasons = Counter(d['reason'] for d in debug_rows)
        for reason, count in reasons.most_common(5):
            print(f"  {reason}: {count}")
        
        print(f"\nSample debug rows:")
        for d in debug_rows[:3]:
            print(f"  File: {d['file']}, Line: {d['line_no']}")
            print(f"  Reason: {d['reason']}")
            print(f"  Preview: {d['line_preview'][:60]}")

if __name__ == "__main__":
    main()