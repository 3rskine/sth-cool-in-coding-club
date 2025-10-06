# process_news_html_test.py
# 測試版：只處理前1000筆資料

import re
import json
import hashlib
import time
from pathlib import Path
from collections import Counter
from multiprocessing import Pool, cpu_count

import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm

# ==================== CONFIG ====================
NEWS_ROOT_FOLDER = r"C:\Users\a7385\OneDrive\Desktop\程式接案\news"
OUT_CSV = "news_data_TEST_1000.csv"
OUT_QC_REPORT = "news_qc_report_TEST.json"
OUT_SAMPLE_HTML = "sample_records_TEST.json"  # 儲存樣本用於檢查

TEST_LIMIT = 1000  # 測試筆數限制
NUM_WORKERS = max(1, cpu_count() - 1)

# 股票關鍵字
STOCK_KEYWORDS = [
    "股市", "股票", "台股", "上市", "上櫃", "櫃買", "證券",
    "財經", "股價", "投資", "法人", "融資", "融券", "營收",
    "獲利", "財報", "除權", "除息", "股東會", "盤勢", "漲跌",
    "外資", "投信", "自營商", "大盤", "加權指數"
]
STOCK_PATTERN = re.compile('|'.join(map(re.escape, STOCK_KEYWORDS)))

MIN_CONTENT_LENGTH = 50

# ==================== FUNCTIONS ====================

def quick_stock_check(content_bytes):
    """快速檢查是否包含股票關鍵字"""
    try:
        text_sample = content_bytes[:5000].decode('utf-8', errors='ignore')
        return bool(STOCK_PATTERN.search(text_sample))
    except:
        return False

def extract_date_fast(text):
    """快速日期提取"""
    if not text:
        return None
    
    patterns = [
        (r'(\d{4})-(\d{2})-(\d{2})', lambda m: m.group(0)),
        (r'(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日', 
         lambda m: f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"),
        (r'(\d{4})/(\d{2})/(\d{2})', lambda m: m.group(0).replace('/', '-')),
    ]
    
    for pattern, formatter in patterns:
        match = re.search(pattern, text[:500])
        if match:
            return formatter(match)
    
    return None

def clean_text_fast(text):
    """快速文字清理"""
    if not text:
        return ""
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'(分享到|分享至|返回列表|列印|上一篇|下一篇).*?(?=\s|$)', '', text)
    return text.strip()

def parse_html_fast(html_content, file_path):
    """快速 HTML 解析"""
    try:
        soup = BeautifulSoup(html_content, 'lxml')
        
        # 移除雜訊標籤
        for tag in soup(['script', 'style', 'nav', 'footer', 'iframe']):
            tag.decompose()
        
        # 標題
        title = None
        if soup.find('h1', class_='title_'):
            title = soup.find('h1', class_='title_').get_text(strip=True)
        elif soup.find('meta', property='og:title'):
            title = soup.find('meta', property='og:title').get('content', '')
        elif soup.find('title'):
            title = soup.find('title').get_text(strip=True)
        
        # 內容
        content = None
        content_div = soup.find('div', class_='edit')
        if content_div:
            section = content_div.find('section')
            if section:
                content = section.get_text(separator=' ', strip=True)
        
        if not content:
            paragraphs = [p.get_text(separator=' ', strip=True) 
                         for p in soup.find_all(['p', 'article']) 
                         if len(p.get_text(strip=True)) > 100]
            if paragraphs:
                content = max(paragraphs, key=len)
        
        # 日期
        date = None
        date_elem = soup.find('span', class_='date')
        if date_elem:
            date = extract_date_fast(date_elem.get_text())
        
        if not date:
            page_header = soup.get_text()[:1000]
            date = extract_date_fast(page_header)
        
        if not date:
            filename = file_path.name
            match = re.search(r'(\d{8})', filename)
            if match:
                d = match.group(1)
                date = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
        
        # 分類
        category = None
        breadcrumb = soup.find('ol', class_='bread_crumbs')
        if breadcrumb:
            cats = [li.get_text(strip=True) for li in breadcrumb.find_all('li')]
            if len(cats) > 1:
                category = cats[-2] if len(cats) > 2 else cats[-1]
        
        return {
            'title': clean_text_fast(title),
            'content': clean_text_fast(content),
            'date': date,
            'category': category
        }
    except Exception as e:
        return {'error': str(e)}

def process_single_file(file_path):
    """處理單個檔案"""
    try:
        # 讀取檔案
        with open(file_path, 'rb') as f:
            content_bytes = f.read()
        
        # 早期過濾：快速檢查是否包含股票關鍵字
        has_stock_keyword = quick_stock_check(content_bytes)
        
        # 解碼
        html_content = content_bytes.decode('utf-8', errors='ignore')
        
        # 解析
        parsed = parse_html_fast(html_content, file_path)
        
        if 'error' in parsed:
            return {
                'status': 'parse_error',
                'file_path': str(file_path),
                'error': parsed['error']
            }
        
        if not parsed.get('title') or not parsed.get('content'):
            return {
                'status': 'missing_data',
                'file_path': str(file_path),
                'has_title': bool(parsed.get('title')),
                'has_content': bool(parsed.get('content'))
            }
        
        # 長度檢查
        content_length = len(parsed['content'])
        if content_length < MIN_CONTENT_LENGTH:
            return {
                'status': 'too_short',
                'file_path': str(file_path),
                'content_length': content_length
            }
        
        # 精確股票關鍵字匹配
        combined = f"{parsed['title']} {parsed['content']} {parsed['category'] or ''}"
        matches = STOCK_PATTERN.findall(combined)
        
        # 計算hash
        content_hash = hashlib.sha1(parsed['content'].encode('utf-8')).hexdigest()
        
        return {
            'status': 'success',
            'stock_related': len(matches) > 0,
            'quick_check': has_stock_keyword,
            'date': parsed['date'] or '',
            'title': parsed['title'],
            'content': parsed['content'],
            'content_length': content_length,
            'category': parsed['category'] or '',
            'keyword_matches': ','.join(set(matches)),
            'keyword_count': len(matches),
            'content_hash': content_hash,
            'file_path': str(file_path)
        }
    except Exception as e:
        return {
            'status': 'exception',
            'file_path': str(file_path),
            'error': str(e)
        }

# ==================== MAIN PROCESSING ====================

def process_news_files_test():
    """測試版主處理函數"""
    start_time = time.time()
    root = Path(NEWS_ROOT_FOLDER)
    
    if not root.exists():
        print(f"❌ 找不到資料夾: {root}")
        return
    
    # 快速掃描檔案
    print("🔍 掃描檔案中...")
    all_files = [f for f in root.rglob("*") if f.is_file()]
    
    # 限制測試數量
    test_files = all_files[:TEST_LIMIT]
    print(f"✓ 總共找到 {len(all_files):,} 個檔案")
    print(f"📋 測試模式：只處理前 {len(test_files)} 個檔案\n")
    
    if not test_files:
        print("❌ 沒有找到任何檔案")
        return
    
    # 多進程處理
    print(f"🚀 使用 {NUM_WORKERS} 個處理器並行處理...")
    all_results = []
    
    with Pool(NUM_WORKERS) as pool:
        results = pool.imap_unordered(process_single_file, test_files, chunksize=50)
        
        with tqdm(total=len(test_files), desc="處理進度", unit="檔案") as pbar:
            for result in results:
                all_results.append(result)
                pbar.update(1)
    
    # 統計分析
    stats = {
        'total': len(all_results),
        'success': 0,
        'stock_related': 0,
        'parse_error': 0,
        'missing_data': 0,
        'too_short': 0,
        'exception': 0,
        'quick_check_match': 0,
        'keyword_counter': Counter(),
        'dates': []
    }
    
    valid_records = []
    sample_records = []  # 保存前10筆用於檢查
    
    for result in all_results:
        status = result.get('status')
        stats[status] = stats.get(status, 0) + 1
        
        if status == 'success':
            stats['success'] += 1
            
            if result['stock_related']:
                stats['stock_related'] += 1
                valid_records.append(result)
                
                # 統計
                if result['keyword_matches']:
                    keywords = result['keyword_matches'].split(',')
                    stats['keyword_counter'].update(keywords)
                if result['date']:
                    stats['dates'].append(result['date'])
                
                # 保存前10筆完整樣本
                if len(sample_records) < 10:
                    sample_records.append({
                        'file_path': result['file_path'],
                        'title': result['title'],
                        'content': result['content'][:500] + '...',  # 只保存前500字
                        'date': result['date'],
                        'category': result['category'],
                        'keywords': result['keyword_matches'],
                        'content_length': result['content_length']
                    })
            
            if result['quick_check']:
                stats['quick_check_match'] += 1
    
    # 儲存樣本檔案供檢查
    sample_path = Path.cwd() / OUT_SAMPLE_HTML
    with open(sample_path, 'w', encoding='utf-8') as f:
        json.dump(sample_records, f, ensure_ascii=False, indent=2)
    print(f"\n✓ 樣本記錄已儲存: {sample_path}")
    
    # 建立 DataFrame
    if valid_records:
        df = pd.DataFrame(valid_records)
        
        # 選擇要輸出的欄位
        output_columns = [
            'date', 'title', 'content', 'category', 
            'keyword_matches', 'keyword_count', 'content_length',
            'content_hash', 'file_path'
        ]
        df = df[output_columns]
        
        # 去重
        before = len(df)
        df = df.drop_duplicates(subset=['content_hash'], keep='first')
        after = len(df)
        
        # 排序
        df = df.sort_values(['date', 'title']).reset_index(drop=True)
        
        # 儲存 CSV
        out_csv = Path.cwd() / OUT_CSV
        df.to_csv(out_csv, index=False, encoding='utf-8-sig')
        print(f"✓ CSV 已儲存: {out_csv}")
        print(f"  - 去重前: {before} 筆")
        print(f"  - 去重後: {after} 筆")
    else:
        print("⚠️ 沒有找到符合條件的股票相關新聞")
    
    # 生成報告
    elapsed = time.time() - start_time
    qc_report = {
        'test_mode': True,
        'total_files_scanned': len(all_files),
        'test_sample_size': len(test_files),
        'processing_stats': {
            'success': stats['success'],
            'stock_related': stats['stock_related'],
            'parse_error': stats.get('parse_error', 0),
            'missing_data': stats.get('missing_data', 0),
            'too_short': stats.get('too_short', 0),
            'exception': stats.get('exception', 0)
        },
        'pass_rate': f"{stats['stock_related']/stats['total']*100:.2f}%" if stats['total'] > 0 else "0%",
        'after_dedup': len(df) if valid_records else 0,
        'quick_check_accuracy': f"{stats['quick_check_match']/stats['total']*100:.1f}%" if stats['total'] > 0 else "0%",
        'top_keywords': dict(stats['keyword_counter'].most_common(20)),
        'date_range': {
            'min': min(stats['dates']) if stats['dates'] else None,
            'max': max(stats['dates']) if stats['dates'] else None
        },
        'date_coverage': f"{len(stats['dates'])}/{stats['stock_related']} ({len(stats['dates'])/stats['stock_related']*100:.1f}%)" if stats['stock_related'] > 0 else "0/0",
        'elapsed_time': f"{elapsed:.1f} 秒",
        'processing_speed': f"{stats['total']/elapsed:.1f} 檔案/秒"
    }
    
    qc_path = Path.cwd() / OUT_QC_REPORT
    with open(qc_path, 'w', encoding='utf-8') as f:
        json.dump(qc_report, f, ensure_ascii=False, indent=2)
    
    # 顯示摘要
    print("\n" + "="*70)
    print("📈 測試結果摘要")
    print("="*70)
    print(f"測試檔案數: {len(test_files):,} / {len(all_files):,}")
    print(f"\n處理狀態:")
    print(f"  ✓ 成功解析: {stats['success']}")
    print(f"  ✓ 股票相關: {stats['stock_related']} ({stats['stock_related']/stats['total']*100:.1f}%)")
    print(f"  ✗ 解析錯誤: {stats.get('parse_error', 0)}")
    print(f"  ✗ 資料不全: {stats.get('missing_data', 0)}")
    print(f"  ✗ 內容太短: {stats.get('too_short', 0)}")
    print(f"  ✗ 其他錯誤: {stats.get('exception', 0)}")
    
    print(f"\n快速篩選準確度: {stats['quick_check_match']}/{stats['total']} ({stats['quick_check_match']/stats['total']*100:.1f}%)")
    
    if valid_records:
        print(f"\n去重後資料: {len(df)} 筆")
        
        if stats['dates']:
            date_coverage = len(stats['dates']) / stats['stock_related'] * 100
            print(f"日期覆蓋率: {len(stats['dates'])}/{stats['stock_related']} ({date_coverage:.1f}%)")
            print(f"日期範圍: {min(stats['dates'])} 至 {max(stats['dates'])}")
        
        print("\n🔑 前10大關鍵字:")
        for kw, count in stats['keyword_counter'].most_common(10):
            print(f"  {kw}: {count}")
        
        print("\n📄 樣本記錄 (前3筆):")
        for i, row in df.head(3).iterrows():
            print(f"\n  [{i+1}] {row['date']} - {row['title'][:50]}...")
            print(f"      內容長度: {row['content_length']} 字")
            print(f"      關鍵字: {row['keyword_matches']}")
    
    print(f"\n⏱️  處理時間: {elapsed:.1f} 秒 ({stats['total']/elapsed:.1f} 檔案/秒)")
    print(f"✓ 報告已儲存: {qc_path}")
    
    # 估算完整處理時間
    if len(all_files) > len(test_files):
        estimated_time = (elapsed / len(test_files)) * len(all_files)
        print(f"\n💡 預估完整處理時間: {estimated_time/3600:.1f} 小時")

if __name__ == "__main__":
    print(f"🖥️  CPU 核心數: {cpu_count()} (使用 {NUM_WORKERS} 個處理器)")
    print(f"🧪 測試模式：只處理前 {TEST_LIMIT} 個檔案\n")
    process_news_files_test()