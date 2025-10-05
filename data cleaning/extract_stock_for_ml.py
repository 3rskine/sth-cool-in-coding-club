# extract_stock_for_ml.py
# 修正版：過濾權證/ETF，優化記憶體使用

import re
from pathlib import Path
import csv
import sys
from collections import defaultdict
import pandas as pd
from datetime import datetime

# ----------------- CONFIG -----------------
BASE_FOLDER = "C:/Users/a7385/OneDrive/Desktop/程式接案/StockInfo"
OUT_CSV = "stock_data_cleaned.csv"
OUT_DEBUG = "stock_processing_debug.csv"
ENCODING = "cp950"
MIN_PRICE = 0.01
MAX_PRICE = 1_000_000.0
MIN_VOLUME = 100  # 最小交易量（千股）
MIN_AMOUNT = 1000  # 最小交易金額（千元）

# 記憶體優化：分批寫入 CSV
BATCH_SIZE = 50000  # 每 5 萬筆寫入一次
# ------------------------------------------

# S38 byte positions
BYTE_POS = {
    "stock_id": (0, 6),
    "open_price_raw": (22, 31),
    "high_price_raw": (31, 40),
    "low_price_raw": (40, 49),
    "close_price_raw": (49, 58),
    "change_flag": (58, 59),
    "change_amount_raw": (59, 68),
    "volume_raw": (77, 86),
    "amount_raw": (95, 107),
}

date_header_re = re.compile(r'(\d{8})')

def slice_bytes(line_bytes, start, end):
    """Extract bytes and decode."""
    try:
        return line_bytes[start:end].decode(ENCODING, errors='replace').strip()
    except:
        return ""

def parse_9digit_price(raw_str):
    """Parse S38 9-digit price."""
    if not raw_str:
        return None
    digits = re.sub(r'[^0-9]', '', raw_str)
    if not digits:
        return None
    if len(digits) < 9:
        digits = digits.zfill(9)
    else:
        digits = digits[-9:]
    try:
        whole = int(digits[:-4])
        frac = int(digits[-4:])
        return round(whole + frac / 10000.0, 4)
    except:
        return None

def parse_volume(raw_str):
    """Parse volume (in thousands)."""
    if not raw_str:
        return None
    digits = re.sub(r'[^0-9]', '', raw_str)
    if not digits:
        return None
    try:
        return int(digits)
    except:
        return None

def is_valid_stock_id(stock_id):
    """
    只保留上市櫃普通股 (1000-9999)。
    排除：權證(7xxxxx)、ETF(00xxxx)。
    """
    if not stock_id:
        return False
    sid = stock_id.strip()
    if not re.match(r'^\d+$', sid):
        return False
    try:
        sid_int = int(sid)
    except:
        return False
    
    # 排除 ETF (< 1000) 和權證 (>= 700000)
    if sid_int < 1000 or sid_int >= 700000:
        return False
    
    # 保留 1000-9999
    return 1000 <= sid_int <= 9999

def derive_date_for_file(path, year_hint=None):
    """Derive YYYYMMDD."""
    try:
        with path.open('r', encoding=ENCODING, errors='replace') as f:
            for _ in range(3):
                line = f.readline()
                if not line:
                    break
                m = date_header_re.search(line)
                if m:
                    date_str = m.group(1)
                    try:
                        datetime.strptime(date_str, '%Y%m%d')
                        return date_str
                    except:
                        pass
    except:
        pass

    m = re.search(r'\((\d{3,8})\)', path.name)
    if m:
        tag = m.group(1).zfill(4)
        if len(tag) == 4 and year_hint:
            date_str = year_hint + tag
            try:
                datetime.strptime(date_str, '%Y%m%d')
                return date_str
            except:
                pass
        elif len(tag) == 8:
            try:
                datetime.strptime(tag, '%Y%m%d')
                return tag
            except:
                pass

    return year_hint + "0101" if year_hint else "19700101"

def process_file(path, date_tag, rows, debug_rows):
    """Process single file."""
    fname = path.name
    
    try:
        with path.open('rb') as f:
            lines_bytes = f.readlines()
        
        for lineno, line_bytes in enumerate(lines_bytes[1:], start=2):
            line_bytes = line_bytes.rstrip(b'\r\n')
            if len(line_bytes) == 0:
                continue
            
            required_len = max(end for _, end in BYTE_POS.values())
            if len(line_bytes) < required_len:
                debug_rows.append({
                    "file": fname, "line_no": lineno, "reason": "line_too_short",
                    "preview": line_bytes[:80].decode(ENCODING, errors='replace')
                })
                continue
            
            try:
                sid = slice_bytes(line_bytes, *BYTE_POS['stock_id'])
                open_raw = slice_bytes(line_bytes, *BYTE_POS['open_price_raw'])
                high_raw = slice_bytes(line_bytes, *BYTE_POS['high_price_raw'])
                low_raw = slice_bytes(line_bytes, *BYTE_POS['low_price_raw'])
                close_raw = slice_bytes(line_bytes, *BYTE_POS['close_price_raw'])
                change_flag = slice_bytes(line_bytes, *BYTE_POS['change_flag'])
                change_amt_raw = slice_bytes(line_bytes, *BYTE_POS['change_amount_raw'])
                volume_raw = slice_bytes(line_bytes, *BYTE_POS['volume_raw'])
                amount_raw = slice_bytes(line_bytes, *BYTE_POS['amount_raw'])
            except Exception as e:
                debug_rows.append({
                    "file": fname, "line_no": lineno, "reason": f"extraction_error:{e}",
                    "preview": line_bytes[:80].decode(ENCODING, errors='replace')
                })
                continue
            
            # 驗證 stock_id
            if not is_valid_stock_id(sid):
                debug_rows.append({
                    "file": fname, "line_no": lineno, "reason": "invalid_stock_id",
                    "preview": f"stock_id={sid}"
                })
                continue
            
            # 解析數值
            open_price = parse_9digit_price(open_raw)
            high_price = parse_9digit_price(high_raw)
            low_price = parse_9digit_price(low_raw)
            close_price = parse_9digit_price(close_raw)
            change_amount = parse_9digit_price(change_amt_raw)
            volume = parse_volume(volume_raw)
            amount = parse_volume(amount_raw)
            
            # 驗證收盤價
            if not close_price or not (MIN_PRICE <= close_price <= MAX_PRICE):
                debug_rows.append({
                    "file": fname, "line_no": lineno, 
                    "reason": f"invalid_close:{close_price}",
                    "preview": f"stock_id={sid}"
                })
                continue
            
            # 過濾低交易量
            if not volume or volume < MIN_VOLUME:
                debug_rows.append({
                    "file": fname, "line_no": lineno,
                    "reason": f"low_volume:{volume}",
                    "preview": f"stock_id={sid}"
                })
                continue
            
            # 過濾低交易金額
            if not amount or amount < MIN_AMOUNT:
                debug_rows.append({
                    "file": fname, "line_no": lineno,
                    "reason": f"low_amount:{amount}",
                    "preview": f"stock_id={sid}"
                })
                continue
            
            # 正規化 stock_id
            sid_normalized = sid.lstrip('0') or '0'
            
            # 記錄有效數據
            rows.append({
                "date": date_tag,
                "stock_id": sid_normalized,
                "open_price": open_price if open_price and open_price >= MIN_PRICE else None,
                "high_price": high_price if high_price and high_price >= MIN_PRICE else None,
                "low_price": low_price if low_price and low_price >= MIN_PRICE else None,
                "closing_price": close_price,
                "change_flag": change_flag,
                "change_amount": change_amount,
                "volume": volume,
                "amount": amount
            })
            
    except Exception as e:
        debug_rows.append({
            "file": fname, "line_no": 0, "reason": f"file_error:{e}", "preview": ""
        })

def write_batch_to_csv(rows, output_path, mode='a', header=False):
    """分批寫入 CSV 以節省記憶體。"""
    if not rows:
        return
    
    df = pd.DataFrame(rows)
    df.to_csv(output_path, mode=mode, header=header, index=False, encoding='utf-8-sig')

def compute_next_day_price_efficient(csv_path):
    """
    記憶體優化版：分批讀取 CSV，計算 next_day_closing_price。
    """
    print("\nComputing next_day_closing_price (memory-efficient)...")
    
    # 讀取整個 CSV（如果太大可改用 chunked reading）
    df = pd.read_csv(csv_path, dtype={'date': str, 'stock_id': str})
    
    # 排序
    df = df.sort_values(['stock_id', 'date']).reset_index(drop=True)
    
    # 計算 next_day_closing_price
    df['next_day_closing_price'] = df.groupby('stock_id')['closing_price'].shift(-1)
    
    # 計算衍生特徵
    print("Computing derived features...")
    
    # 振幅
    df['amplitude'] = None
    mask = df['high_price'].notna() & df['low_price'].notna() & (df['low_price'] > 0)
    df.loc[mask, 'amplitude'] = ((df.loc[mask, 'high_price'] - df.loc[mask, 'low_price']) / 
                                   df.loc[mask, 'low_price']).round(4)
    
    # 漲跌幅
    df['change_pct'] = None
    mask = df['change_amount'].notna() & df['closing_price'].notna()
    prev_price = df.loc[mask, 'closing_price'] - df.loc[mask, 'change_amount']
    mask = mask & (prev_price != 0)
    df.loc[mask, 'change_pct'] = (df.loc[mask, 'change_amount'] / prev_price).round(4)
    
    # 重新排序並儲存
    df = df.sort_values(['date', 'stock_id']).reset_index(drop=True)
    
    # 選擇最終欄位（記憶體優化：不保留 change_flag，用 change_pct 即可）
    output_cols = [
        'date', 'stock_id',
        'open_price', 'high_price', 'low_price', 'closing_price',
        'change_amount', 'change_pct',
        'volume', 'amount', 'amplitude',
        'next_day_closing_price'
    ]
    
    df_final = df[output_cols]
    
    # 覆寫原檔案
    print(f"Saving final version to {csv_path}...")
    df_final.to_csv(csv_path, index=False, encoding='utf-8-sig')
    
    return df_final

def main():
    root = Path(BASE_FOLDER)
    if not root.exists():
        print(f"Error: BASE_FOLDER not found: {root}")
        sys.exit(1)
    
    batch_rows = []
    debug_rows = []
    files_processed = 0
    total_records = 0
    
    out_path = Path.cwd() / OUT_CSV
    
    # 清空輸出檔案
    if out_path.exists():
        out_path.unlink()
    
    print("=" * 70)
    print("STOCK DATA EXTRACTION (MEMORY OPTIMIZED)")
    print("=" * 70)
    
    first_batch = True
    
    for sub in sorted(root.iterdir()):
        if not sub.is_dir():
            continue
        
        year_hint = sub.name if re.match(r'^\d{4}$', sub.name) else None
        if not year_hint:
            continue
        
        files = sorted([
            p for p in sub.iterdir() 
            if p.is_file() and (
                p.name.upper().startswith("STKT2QUOTESN") or 
                p.name.upper().startswith("STKWQUOTES")
            )
        ])
        
        if not files:
            continue
        
        print(f"\nProcessing {year_hint}: {len(files)} files")
        
        for f in files:
            files_processed += 1
            date_tag = derive_date_for_file(f, year_hint=year_hint)
            process_file(f, date_tag, batch_rows, debug_rows)
            
            # 分批寫入
            if len(batch_rows) >= BATCH_SIZE:
                write_batch_to_csv(batch_rows, out_path, 
                                   mode='w' if first_batch else 'a',
                                   header=first_batch)
                total_records += len(batch_rows)
                print(f"  Written {total_records:,} records...")
                batch_rows = []
                first_batch = False
    
    # 寫入最後一批
    if batch_rows:
        write_batch_to_csv(batch_rows, out_path,
                           mode='w' if first_batch else 'a',
                           header=first_batch)
        total_records += len(batch_rows)
    
    print(f"\nTotal records before filtering: {total_records:,}")
    
    if total_records == 0:
        print("No valid records found!")
        sys.exit(1)
    
    # 計算 next_day_closing_price
    df_final = compute_next_day_price_efficient(out_path)
    
    # 寫入 debug
    if debug_rows:
        dbg_path = Path.cwd() / OUT_DEBUG
        df_debug = pd.DataFrame(debug_rows)
        df_debug.to_csv(dbg_path, index=False, encoding='utf-8-sig')
        print(f"Debug file: {dbg_path}")
    
    # 統計
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Files processed: {files_processed}")
    print(f"Valid records: {len(df_final):,}")
    print(f"Debug records: {len(debug_rows):,}")
    print(f"Unique stocks: {df_final['stock_id'].nunique():,}")
    print(f"Date range: {df_final['date'].min()} to {df_final['date'].max()}")
    print(f"\nRecords with next_day_price: {df_final['next_day_closing_price'].notna().sum():,}")
    print(f"Records missing next_day_price: {df_final['next_day_closing_price'].isna().sum():,}")
    
    print("\nSample records:")
    print(df_final.head(10).to_string())
    
    print(f"\n✓ Output: {out_path}")

if __name__ == "__main__":
    main()