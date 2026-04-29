import streamlit as st
st.set_page_config(page_title="BoatPredict Elite (Boaters JP)", layout="wide", initial_sidebar_state="auto")

import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime
import re
import warnings
import asyncio
import nest_asyncio
import os
import subprocess
import sys
import warnings
import asyncio
import nest_asyncio
from playwright.async_api import async_playwright
try:
    from venue_metadata import VENUES_METADATA
except ImportError:
    # フォールバック（万が一見つからない場合）
    VENUES_METADATA = {}

# Streamlit Cloud 用の Playwright インストール確認
def ensure_playwright_installed():
    try:
        import playwright
    except ImportError:
        subprocess.run([sys.executable, "-m", "pip", "install", "playwright"])
    
    if not os.path.exists(os.path.expanduser("~/.cache/ms-playwright")):
        with st.spinner("初回起動時のブラウザセットアップ中..."):
            subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"])

if 'playwright_checked' not in st.session_state:
    ensure_playwright_installed()
    st.session_state.playwright_checked = True

warnings.filterwarnings('ignore')
nest_asyncio.apply()

st.markdown("""
<style>
/* ボタン・カード等の基本デザイン */
.stButton>button { width: 100%; height: 60px; font-size: 20px !important; font-weight: bold; background-color: #212529; color: white; border-radius: 6px; border: 2px solid #005ce6; transition: 0.3s; }
.stButton>button:hover { background-color: #005ce6; color: white; transform: translateY(-2px); }
.metric-box { padding: 20px; border-radius: 10px; background-color: #ffffff; border-left: 8px solid #005ce6; margin-bottom: 20px; box-shadow: 0 4px 10px rgba(0,0,0,0.08); }

/* テーブルのモバイル横スクロール対応 */
div[data-testid="stTable"], div[data-testid="stDataFrame"], .stTableContainer {
    overflow-x: auto !important;
    display: block !important;
    width: 100%;
}

/* モバイル用フォントサイズ・余白調整 */
@media (max-width: 640px) {
    .stMetric { font-size: 14px !important; }
    .metric-box { padding: 10px; border-left-width: 4px; }
    h1 { font-size: 22px !important; }
    h2, h3 { font-size: 18px !important; }
}
</style>
""", unsafe_allow_html=True)

VENUES = {
    "桐生": "kiryu", "戸田": "toda", "江戸川": "edogawa", "平和島": "heiwajima", "多摩川": "tamagawa", 
    "浜名湖": "hamanako", "蒲郡": "gamagori", "常滑": "tokoname", "津": "tsu", "三国": "mikuni", 
    "びわこ": "biwako", "住之江": "suminoe", "尼崎": "amagasaki", "鳴門": "naruto", "丸亀": "marugame", 
    "児島": "kojima", "宮島": "miyajima", "徳山": "tokuyama", "下関": "shimonoseki", "若松": "wakamatsu", 
    "芦屋": "ashiya", "福岡": "fukuoka", "唐津": "karatsu", "大村": "omura"
}

# 過去1年間の統計ベースの「荒れる度（万舟率等）」
VENUE_ROUGHNESS_MAP = {
    "桐生": 16.2, "戸田": 19.8, "江戸川": 18.5, "平和島": 19.2, "多摩川": 16.5, 
    "浜名湖": 15.8, "蒲郡": 14.2, "常滑": 15.5, "津": 16.8, "三国": 16.3, 
    "びわこ": 17.5, "住之江": 13.8, "尼崎": 14.5, "鳴門": 18.8, "丸亀": 15.2, 
    "児島": 15.1, "宮島": 16.7, "徳山": 12.2, "下関": 13.5, "若松": 14.1, 
    "芦屋": 13.2, "福岡": 17.8, "唐津": 14.5, "大村": 11.2
}

def clean_float(val, fallback=0.0):
    try:
        nums = re.findall(r'([0-9]+\.[0-9]+|[0-9]+)', str(val).replace("%","").replace("F","").replace("L",""))
        return float(nums[0]) if nums else fallback
    except: return fallback

async def _headless_boaters_text_extraction(url):
    tab_texts = {}
    async with async_playwright() as p:
        # Streamlit Cloudなどの制限環境向けに args を追加
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]
        )
        page = await browser.new_page()
        try:
            await page.goto(url, wait_until="networkidle", timeout=30000)
            await page.wait_for_timeout(3000)
            
            for tab in ['出走表', '直前情報', '連対率・展開', 'モーター情報', 'オッズ']:
                try:
                    if tab == 'オッズ':
                        try:
                            # 1. Click main "オッズ" tab button/link
                            try:
                                await page.get_by_role("link", name="オッズ").nth(0).click(timeout=3000)
                            except:
                                try: await page.get_by_text("オッズ", exact=True).nth(0).click(timeout=3000)
                                except: pass
                            await page.wait_for_timeout(2000)
                            
                            # 2. Try to click "3連単" specifically if available (Boaters defaults to it but safe to check)
                            try: await page.get_by_text("3連単", exact=True).click(timeout=2000)
                            except: pass
                            await page.wait_for_timeout(1000)
                            
                            # 3. Iterate through boats 1-6 navigation tabs to capture the full grid
                            full_odds_text = ""
                            for b_no in range(1, 7):
                                try:
                                    # Boatersの1着タブは "1.選手名" 形式
                                    # text* は部分一致。例: "1." で始まる要素を探す
                                    tab_selectors = [
                                        f"xpath=//a[contains(text(), '{b_no}.')]",
                                        f"xpath=//button[contains(text(), '{b_no}.')]",
                                        f"text='{b_no}.'",
                                        f"text='{b_no}'"
                                    ]
                                    
                                    clicked = False
                                    for ts in tab_selectors:
                                        if await page.locator(ts).count() > 0:
                                            await page.locator(ts).nth(0).click(timeout=2000)
                                            clicked = True
                                            break
                                    
                                    if clicked:
                                        # タブ切り替えの反映を待機
                                        await page.wait_for_timeout(1000)
                                    
                                    try:
                                        # オッズの表（.css-11kbggr）が完全に表示されるまで待機
                                        await page.wait_for_selector('.css-11kbggr', timeout=5000)
                                    except:
                                        pass
                                    
                                    # HTML構造ごと取得してパースするため、ページ全体のHTMLを取得
                                    content = await page.content()
                                    
                                    full_odds_text += f"\nSTART_BOAT_{b_no}\n" + content
                                except Exception as e:
                                    full_odds_text += f"\nSTART_BOAT_{b_no}\nERROR: {str(e)}"
                            
                            tab_texts['オッズ'] = full_odds_text
                        except: pass
                    else:
                        await page.get_by_text(tab, exact=True).nth(0).click(timeout=3000)
                        await page.wait_for_timeout(1500)
                        tab_texts[tab] = await page.evaluate("() => document.body.innerText")
                        
                    if tab == '直前情報':
                        try:
                            await page.get_by_text("オリジナル展示", exact=True).click(timeout=3000)
                            await page.wait_for_timeout(1500)
                            tab_texts['オリジナル展示'] = await page.evaluate("() => document.body.innerText")
                        except: pass
                    
                    if tab == '連対率・展開':
                        try:
                            # CLICK AI 3-place rate
                            try: await page.get_by_text("AI３連対率", exact=True).nth(0).click(timeout=3000)
                            except: pass
                            await page.wait_for_timeout(1500)
                            tab_texts['連対率・展開'] = await page.evaluate("() => document.body.innerText")
                        except: pass

                except Exception:
                    pass
        except Exception as e:
            print(f"Browser error: {e}")
        await browser.close()
    return tab_texts

def scrape_full_boaters_workflow(date_str, venue_cd, race_no):
    # url mapping correctly uses literal venue code eg hamanako and date string
    url = f"https://boaters-boatrace.com/race/{venue_cd}/{date_str}/{race_no}R"
    
    tab_texts = asyncio.run(_headless_boaters_text_extraction(url))
    
    extracted = {
        "env": {"wind_spd": 0, "wind_dir": "無風", "wave": "-", "water_level": "-", "water_temp": "-"},
        "boats": [{"course": i+1, "name": "-", "class": "-", 
                   "top1_rate": 15.0, "top2_rate": 20.0, "top3_rate": 35.0,
                   "avg_st": 0.16, "avg_st_rank": 3.0, "course_avg_st": "-", "course_avg_st_rank": "-",
                   "kimarite_nige": 0.0, "kimarite_sashi": 0.0, "kimarite_makuri": 0.0,
                   "ex_st": "-", "motor_2ren": 30.0, "motor_3ren": 40.0,
                   "turn": "-", "straight": "-", "lap_time": "-", "ex_time": "-", "tilt": 0.0} 
                  for i in range(6)]
    }
    
    # Wait for all tabs to be collected then parse
    for text in tab_texts.values():
        ws = re.search(r'風速\s*([\d\.]+)[\s\n]*m', text)
        if ws: extracted["env"]["wind_spd"] = float(ws.group(1))
        wv = re.search(r'波高\s*([\d\.]+)[\s\n]*cm', text)
        if wv: extracted["env"]["wave"] = float(wv.group(1))
        wl = re.search(r'(?:潮位|水位)\s*([0-9\-]+)\s*cm', text)
        if wl: extracted["env"]["water_level"] = wl.group(1) + "cm"
        wt = re.search(r'水温\s*([0-9\.]+)\s*℃', text)
        if wt: extracted["env"]["water_temp"] = wt.group(1) + "℃"
        for d in ["北", "北北東", "北東", "東北東", "東", "東南東", "南東", "南南東", "南", "南南西", "南西", "西南西", "西", "西北西", "北西", "北北西", "追い風", "向かい風", "左横風", "右横風"]:
            if "風向\n" + d in text or d in text[:1500]: 
                extracted["env"]["wind_dir"] = d
                break

    # 1. 出走表
    if '出走表' in tab_texts:
        lines = [line.strip() for line in tab_texts['出走表'].split('\n') if line.strip()]
        for idx in range(6):
            b_idx = str(idx + 1)
            for i, line in enumerate(lines):
                if line == b_idx and i+2 < len(lines):
                    name = lines[i+1]
                    bclass = lines[i+2]
                    if bclass in ["A1", "A2", "B1", "B2"]:
                        extracted["boats"][idx]["name"] = name
                        extracted["boats"][idx]["class"] = bclass
                        # Extract Average ST
                        for j in range(i, min(i+50, len(lines))):
                            if lines[j].startswith(".") and len(lines[j]) == 3:
                                extracted["boats"][idx]["avg_st"] = float("0" + lines[j])
                                break
                        break

    # 2. 直前情報 (Extract ST and Tilt)
    if '直前情報' in tab_texts:
        lines = [line.strip() for line in tab_texts['直前情報'].split('\n') if line.strip()]
        for idx in range(6):
            b_idx = str(idx + 1)
            for i, line in enumerate(lines):
                # Search for boat number as a standalone line
                if line == b_idx and i+15 < len(lines):
                    # Find exhibition time first (like 6.85)
                    exh_idx = -1
                    for j in range(i+1, i+15):
                        if re.match(r'^\d\.\d{2}$', lines[j]):
                            exh_idx = j
                            break
                    
                    if exh_idx != -1:
                        # Tilt is usually right after exhibition time (like -0.5, 0.0, 0.5)
                        for j in range(exh_idx+1, exh_idx+5):
                            if re.match(r'^[+-]?\d\.[05]$', lines[j]): # Tilt is usually .0 or .5
                                extracted["boats"][idx]["tilt"] = float(lines[j])
                                break
                    break
        
        # Start info (ST)
        try:
            st_idx = lines.index("スタート情報")
            for idx in range(6):
                b_idx = str(idx + 1)
                for i in range(st_idx, min(st_idx+50, len(lines))):
                    if lines[i] == b_idx and i+1 < len(lines):
                        val = lines[i+1].replace("F", "").replace("L", "")
                        if re.match(r'^\.?\d+$', val):
                            extracted["boats"][idx]["ex_st"] = float("0" + val if val.startswith(".") else val)
                            break
        except: pass
                    
    # 2-B. オリジナル展示
    if 'オリジナル展示' in tab_texts:
        lines = [line.strip() for line in tab_texts['オリジナル展示'].split('\n') if line.strip()]
        for idx in range(6):
            b_idx = str(idx + 1)
            for i, line in enumerate(lines):
                if line == b_idx and i+6 < len(lines) and lines[i+2] in ["A1", "A2", "B1", "B2"]:
                    # In original display, values follow boat/name
                    # Find first time-like pattern or hyphen
                    found_data = False
                    for j in range(i+1, i+15):
                        val = lines[j]
                        if re.match(r'^\d{1,2}[\.·]\d{2}$', val) or val == "-":
                            def f_val(v): return 1.0 if v == "-" else float(v)
                            extracted["boats"][idx]["lap_time"] = f_val(val)
                            if j+3 < len(lines):
                                extracted["boats"][idx]["turn"] = f_val(lines[j+1])
                                extracted["boats"][idx]["straight"] = f_val(lines[j+2])
                                extracted["boats"][idx]["ex_time"] = f_val(lines[j+3])
                            found_data = True
                            break
                    if found_data: break

    # 3. 連対率・展開 (Including Boat 1 loss patterns)
    if '連対率・展開' in tab_texts:
        lines = [line.strip() for line in tab_texts['連対率・展開'].split('\n') if line.strip()]
        
        # Boat 1 loss characteristics
        try:
            loss_idx = -1
            for k, line in enumerate(lines):
                if line == "1" and lines[k+1] == extracted["boats"][0]["name"] and "逃げ" in lines[k-10:k+100]:
                    # Found boat 1 in决まり手率 section
                    for j in range(k+1, k+30):
                        if "まくられ" == lines[j] and j+1 < len(lines):
                            extracted["boats"][0]["loss_makurare_rate"] = clean_float(lines[j+1])
                        if "差され" == lines[j] and j+1 < len(lines):
                            extracted["boats"][0]["loss_sasare_rate"] = clean_float(lines[j+1])
                    break
        except: pass

        for idx in range(6):
            b_idx = str(idx + 1)
            for i, line in enumerate(lines):
                # Similar to previous logic but update fields
                # Flexible name check: handle trailing rank like "Name B1"
                match_name = extracted["boats"][idx]["name"]
                if line == b_idx and i+5 < len(lines) and (match_name in lines[i+1] or lines[i+1] in match_name):
                    percents = [clean_float(lines[j]) for j in range(i+3, i+20) if "%" in lines[j]]
                    if len(percents) >= 3:
                        extracted["boats"][idx]["top1_rate"] = percents[0]
                        extracted["boats"][idx]["top2_rate"] = percents[1]
                        extracted["boats"][idx]["top3_rate"] = percents[2]
                    break
                    
        # Course Average ST
        try:
            start_idx = lines.index("平均ST順位")
            for idx in range(6):
                b_idx = str(idx + 1)
                for i in range(start_idx, min(start_idx+150, len(lines))):
                    if lines[i] == b_idx and i+4 < len(lines) and lines[i+1] == extracted["boats"][idx]["name"]:
                        v_st = lines[i+2]
                        v_rank = lines[i+3].replace("位", "")
                        extracted["boats"][idx]["course_avg_st"] = float(v_st) if v_st.replace('.','').isdigit() else 0.16
                        extracted["boats"][idx]["course_avg_st_rank"] = float(v_rank) if v_rank.replace('.','').isdigit() else 3.5
                        break
        except: pass
                    

    # 4. モーター情報
    if 'モーター情報' in tab_texts:
        lines = [line.strip() for line in tab_texts['モーター情報'].split('\n') if line.strip()]
        for idx in range(6):
            b_idx = str(idx + 1)
            for i, line in enumerate(lines):
                # We expect: 1 -> No.66 -> 39 位 -> 0.0% -> (0回) -> 20.0% ...
                if line == b_idx and i+3 < len(lines) and lines[i+1].startswith("No.") and "位" in lines[i+2].replace(" ", ""):
                    val_2ren = lines[i+3]
                    if "%" in val_2ren:
                        extracted["boats"][idx]["motor_2ren"] = clean_float(val_2ren, 30.0)
                    break
        
    extracted["odds"] = {}
    if 'オッズ' in tab_texts:
        full_text = tab_texts['オッズ']
        
        # 1. バックアップ：明示的な「1-2-3 12.3」形式を抽出
        triplets = re.finditer(r'([1-6])[\s\-－]+([1-6])[\s\-－]+([1-6])[\s\n]+([\d\.]{2,10})', full_text)
        for m in triplets:
            b1, b2, b3, val = m.groups()
            if b1 != b2 and b1 != b3 and b2 != b3:
                ov = clean_float(val)
                if 1.0 < ov < 5000.0:
                    extracted["odds"][f"{b1}-{b2}-{b3}"] = ov

        # 2. セパレータ（START_BOAT_）で分割して各1着ごとに解析、および全体の一括解析
        # 実際のオッズ部分は全艇分が1つのマトリックスで表示される場合もあるため、まず全体をパースする
        soup = BeautifulSoup(full_text, 'html.parser')
        
        # 1着艇ごとのコンテナをすべて探す
        for b1_container in soup.find_all(class_='css-1r6pq8e'):
            b1_str = None
            for div in b1_container.children:
                if div.name == 'div' and '.' in div.get_text():
                    text = div.get_text(strip=True)
                    if text and text[0].isdigit():
                        b1_str = text[0]
                        break
            
            if not b1_str:
                continue
                
            # 2着艇の行(row)を探す
            for row in b1_container.find_all(class_='css-1hf8agc'):
                children = list(row.children)
                if len(children) < 2: continue
                
                # 色クラスに依存せず、要素の順序から取得
                b2_str = children[0].get_text(strip=True)
                b3_container = children[1]
                
                # 3着艇とオッズのセルを探す
                for cell in b3_container.find_all(class_='css-130bjmo'):
                    cell_children = list(cell.children)
                    if len(cell_children) < 2: continue
                    
                    b3_str = cell_children[0].get_text(strip=True)
                    odds_elem = cell.find(class_='css-11kbggr')
                    if odds_elem:
                        odds_val = odds_elem.get_text(strip=True)
                        try:
                            val = clean_float(odds_val)
                            if val > 1.0:
                                extracted["odds"][f"{b1_str}-{b2_str}-{b3_str}"] = val
                        except Exception:
                            pass

        # 上記のChakraUI専用パーサーでうまく取れなかった場合のテキストフォールバック
        tab_sections = re.split(r'START_BOAT_([1-6])', full_text)
        for idx in range(1, len(tab_sections), 2):
            b1 = tab_sections[idx]
            html_content = tab_sections[idx+1]
            found_keys = [k for k in extracted["odds"].keys() if k.startswith(f"{b1}-")]
            if len(found_keys) < 20:
                tokens = html_content.split()
                current_b2 = None
                i = 0
                while i < len(tokens):
                    t = tokens[i]
                    if t in [str(k) for k in range(1, 7)] and t != b1:
                        if i + 1 < len(tokens) and not tokens[i+1].replace(".","").replace("-","").isdigit():
                            current_b2 = t
                            i += 1
                            continue
                    if current_b2 and t in [str(k) for k in range(1, 7)] and t != b1 and t != current_b2:
                        if i + 1 < len(tokens):
                            val = clean_float(tokens[i+1])
                            if val > 1.0:
                                extracted["odds"][f"{b1}-{current_b2}-{t}"] = val
                                i += 2
                                continue
                    i += 1

        extracted["raw_text"] = full_text
        
    return extracted

def calculate_synthetic_odds(bets, odds_dict):
    valid_odds = []
    for b in bets:
        bet_str = b["bet"]
        val = odds_dict.get(bet_str)
        if isinstance(val, (int, float)) and val > 0:
            valid_odds.append(val)
    if not valid_odds:
        return 0.0
    return 1.0 / sum(1.0 / o for o in valid_odds)

def parse_time_with_rank(boats, key_name):
    times = []
    for b in boats:
        val = str(b.get(key_name, "-")).strip()
        if val == "-" or val == "" or val == "nan":
            times.append(0.0)
        else:
            try: times.append(float(re.findall(r'([0-9]+\.[0-9]+)', val)[0]))
            except: times.append(9.99)
    t_work = [x if x != 0.0 else 99.9 for x in times]
    ranks = pd.Series(t_work).rank(method='min').values
    return times, ranks

@st.cache_data
def load_exhibition_weights():
    try: return pd.read_csv("exhibition_correction_scores.tsv", sep='\t')
    except: return None

def calculate_dynamic_roughness(data, venue_name):
    # 開催地ごとのベース統計（万舟率等）
    base = VENUE_ROUGHNESS_MAP.get(venue_name, 15.0)
    roughness = base
    
    env = data.get("env", {})
    wind_spd = clean_float(env.get("wind_spd", 0.0))
    wave = clean_float(env.get("wave", 0.0))
    
    # 1. 気象条件による加算 (最大 +20%)
    if wind_spd >= 5.0: roughness += 10.0
    if wave >= 5.0: roughness += 10.0
    
    boats = data.get("boats", [])
    if not boats: return round(roughness, 1)
    
    # 2. 展示タイムの異常（外枠が一番時計など） (最大 +30%)
    ex_times = []
    for b in boats:
        t = clean_float(b.get("ex_time", 9.99))
        if t < 9.0: ex_times.append(t)
    
    if ex_times:
        best_ex = min(ex_times)
        best_boat_indices = [i for i, t in enumerate(ex_times) if t == best_ex]
        if any(idx >= 3 for idx in best_boat_indices):
            roughness += 15.0 # 外枠が速い
        if (max(ex_times) - min(ex_times)) <= 0.10:
            roughness += 15.0 # 展示差が少なく混戦
            
    # 3. 1号艇の不安要素 (最大 +15%)
    # 展示順位または1周タイムランクが下位
    try:
        if int(boats[0].get("ex_rank", 1)) >= 4 or int(boats[0].get("lap_rank", 1)) >= 4:
            roughness += 15.0
    except: pass
        
    # 4. オリジナル展示 (Vスコア) による波乱度 (最大 +15%)
    # 外枠に高いVスコアがある場合
    v_scores = []
    for b in boats:
        v_scores.append(clean_float(b.get("v_score", 0.0)))
    if any(v >= 0.7 for i, v in enumerate(v_scores) if i >= 3):
        roughness += 15.0

    return min(round(roughness, 1), 98.5)

def calculate_oracle(data: dict, venue: str) -> dict:
    """Implement the priority analysis algorithm."""
    venue_info = VENUES_METADATA.get(venue, {"type": "B", "water": "海水"})
    v_type = venue_info["type"]
    water = venue_info["water"]
    
    boats = data["boats"]
    env = data["env"]
    
    # Init scores for 1st, 2nd, 3rd place potential
    s1 = [100.0] * 6
    s2 = [100.0] * 6
    s3 = [100.0] * 6
    alerts = []
    
    # --- 1. Environment Layer ---
    wave = clean_float(env.get("wave", 0.0))
    wind_spd = clean_float(env.get("wind_spd", 0.0))
    wind_dir = env.get("wind_dir", "無風")
    
    # Wave Debuff
    if wave >= 3.0:
        base_debuff = (wave - 3.0 + 1)
        if water == "淡水":
            s1[0] -= (base_debuff * 4.0)
            alerts.append(f"【水質デバフ】淡水・波高{wave}cmにより1号艇勝率を減算")
        else:
            s1[0] -= (base_debuff * 2.0)
            alerts.append(f"【水質デバフ】海水・波高{wave}cmにより1号艇勝率を減算")
            
    if venue == "江戸川":
        alerts.append("【江戸川】会場実績（波乗り指数）を最優先評価")

    # Wind Correction
    if wind_spd >= 5.0:
        if "向かい風" in wind_dir or wind_dir in ["北", "北西", "北北西"]:
            s1[3] *= 1.8 # 4コースまくり
            s1[2] *= 1.5 # 3コースまくり
            alerts.append("【風向補正】向かい風5m以上：センター勢のまくり期待値アップ")
        elif "追い風" in wind_dir or wind_dir in ["南", "南東", "南南東"]:
            s1[1] *= 2.0 # 2コース差し
            s1[2] *= 2.0 # 3コースまくり差し
            alerts.append("【風向補正】追い風5m以上：2,3コースの差し・まくり差し期待値アップ")

    # --- 1. Metadata Preprocessing (Rank Recalculation) ---
    # Ensure all time ranks are correctly calculated within 1-6
    lap_times, lap_ranks = parse_time_with_rank(boats, "lap_time")
    ex_times, ex_ranks = parse_time_with_rank(boats, "ex_time")
    straight_times, straight_ranks = parse_time_with_rank(boats, "straight")
    turn_times, turn_ranks = parse_time_with_rank(boats, "turn")
    
    # Force recalculate course_avg_st_rank based on course_avg_st
    st_vals = [clean_float(b.get("course_avg_st", 0.16)) for b in boats]
    st_ranks = pd.Series([s if s > 0 else 0.99 for s in st_vals]).rank(method='min').values
    
    for i in range(6):
        boats[i]["lap_rank"] = int(lap_ranks[i])
        boats[i]["ex_rank"] = int(ex_ranks[i])
        boats[i]["straight_rank"] = int(straight_ranks[i])
        boats[i]["turn_rank"] = int(turn_ranks[i])
        boats[i]["course_avg_st_rank"] = int(st_ranks[i])

    # --- 2. V-Score calculation (Slit advantage) ---
    v_scores = [0.0] * 6
    for i in range(1, 6):
        prev_b = boats[i-1]
        b = boats[i]
        ext_diff = (prev_b.get("ex_time", 6.85) - b.get("ex_time", 6.85))
        ast_diff = (prev_b.get("avg_st", 0.16) - b.get("avg_st", 0.16))
        v_scores[i] = (ext_diff * 10 * 0.6) + (ast_diff * 10 * 0.4)
        if v_scores[i] >= 0.5:
            alerts.append(f"【V-Score】{i+1}号艇 直まくり優位性あり")

    # --- 3. Holistic Strength Score Calculation ---
    holistic_scores = [0.0] * 6
    for i in range(6):
        b = boats[i]
        
        # A. Machine Performance (Original Exhibition)
        # Lap: 37.0 is avg, lower is better. Turn/Straight: higher is better (but here they are times? No, Boaters uses ranks usually, but we got floats)
        # If they are times, lower is better. Let's assume they are times.
        m_perf = 0
        try:
            lap_score = max(0, (38.0 - clean_float(b.get("lap_time", 38.0))) * 40)
            turn_score = max(0, (6.0 - clean_float(b.get("turn", 6.0))) * 20)
            strt_score = max(0, (8.0 - clean_float(b.get("straight", 8.0))) * 20)
            m_perf = lap_score + turn_score + strt_score
        except: pass
        
        # B. Exhibition Time
        ex_perf = max(0, (7.0 - clean_float(b.get("ex_time", 7.0))) * 100)
        
        # C. Winning Records (Win Rates)
        win_perf = (b.get("top1_rate", 0) * 0.6 + b.get("top2_rate", 0) * 0.3 + b.get("top3_rate", 0) * 0.1)
        
        # D. Start Ability
        st_perf = max(0, (0.25 - b.get("course_avg_st", 0.18)) * 200)
        st_rank_bonus = (7 - b.get("course_avg_st_rank", 6)) * 5
        
        # E. Composite Score
        total = m_perf + ex_perf + win_perf + st_perf + st_rank_bonus
        
        # F. Adjustments (Environment/V-Score/Fraud)
        # Apply the logic that was previously in s1 but more broadly
        if i == 0 and wave >= 3.0:
            total -= (base_debuff * 5.0)
            
        if wind_spd >= 5.0:
            # course specific adjustment
            if i == 3 and ("向かい風" in wind_dir or wind_dir in ["北", "北西", "北北西"]): total += 20
            if i == 1 and ("追い風" in wind_dir or wind_dir in ["南", "南東", "南南東"]): total += 15
            
        if v_scores[i] >= 0.5: total += 15
        
        # Exhibition Fraud
        if b.get("tilt", 0.0) >= 0.5 and b.get("lap_rank", 1) >= 4:
            total *= 0.8
            
        holistic_scores[i] = total

    # Normalize Holistic Scores to 0-100 range for display
    # (Optional: Shift scores to be centered around 50-70)
    final_display_scores = []
    max_s = max(holistic_scores) if max(holistic_scores) > 0 else 1
    for s in holistic_scores:
        norm = (s / max_s) * 100
        final_display_scores.append(round(norm, 1))

    # Base s1 probabilities on holistic scores + inner course advantage
    # In-course weight: 1: +50, 2: +20, 3: +10, 4: +5, 5: 0, 6: -10
    course_weights = [50, 20, 10, 5, 0, -10]
    for i in range(6):
        s1[i] = holistic_scores[i] + course_weights[i]
        # Further boost for In-teppan (C-type)
        if v_type == "C" and i == 0: s1[i] += 40
        if v_type == "A" and i == 3: s1[i] += 20

    # Base s2/s3 probabilities (more balanced, less inner bias)
    s2 = [holistic_scores[i] + [20, 30, 25, 15, 10, 0][i] for i in range(6)]
    s3 = [holistic_scores[i] + [10, 20, 25, 25, 20, 10][i] for i in range(6)]

    # Softmax logic for probabilities
    def softmax(x, temp=10.0):
        e_x = np.exp((x - np.max(x)) / temp)
        return e_x / e_x.sum()

    return {
        "p1": softmax(np.array(s1), temp=15.0),
        "p2": softmax(np.array(s2), temp=20.0),
        "p3": softmax(np.array(s3), temp=25.0),
        "scores": final_display_scores,
        "alerts": alerts
    }

def analyze_kimarite_and_bets(oracle_results: dict, data: dict, venue: str, bet_count: int, prediction_mode="通常", special_odds_threshold=40.0, special_exclude_1_head=False) -> dict:
    p1 = oracle_results["p1"]
    env = data["env"]
    boats = data["boats"]
    venue_info = VENUES_METADATA.get(venue, {"type": "B"})
    
    # --- Confidence Score ---
    lap_ranks = [b.get("lap_rank", 6) for b in boats]
    in_perf = 1.0 if lap_ranks[0] <= 2 else 0.5
    env_stable = 1.0 if clean_float(env.get("wave", 0)) <= 2 and clean_float(env.get("wind_spd", 0)) <= 3 else 0.5
    venue_esc = 0.8 if venue_info["type"] == "C" else 0.5
    conf_score = (in_perf + env_stable + venue_esc) / 2.8 * 100
    conf_label = "C"
    if conf_score > 85: conf_label = "S"
    elif conf_score > 70: conf_label = "A"
    elif conf_score > 50: conf_label = "B"
    
    # --- Mode Selection ---
    manshu_active = (prediction_mode == "万舟的中")
    manshu_special = (prediction_mode == "万舟特化")
    
    if prediction_mode == "通常":
        if clean_float(env.get("wind_spd", 0)) >= 5.0 or clean_float(env.get("wave", 0)) >= 5.0:
            manshu_active = True
        for b in boats:
            if b.get("tilt", 0.0) >= 0.5 and b.get("lap_rank") == 1 and b.get("ex_st", 0.15) <= 0.10:
                manshu_active = True
                break
            
    top_boats = np.argsort(p1)[::-1]
    top_boat_idx = top_boats[0]
    
    # Determine Kimarite string
    kimarite_label = "イン逃げ"
    if top_boat_idx == 0:
        kimarite_label = "イン逃げ"
    elif top_boat_idx == 1:
        kimarite_label = "差し"
    elif p1[1] > 0.15:
        kimarite_label = "まくり差し" if clean_float(env.get("wind_spd", 0)) >= 5.0 and "追い風" in env.get("wind_dir", "") else "まくり"
    elif p1[2] > 0.15:
        kimarite_label = "二段まくり" if clean_float(env.get("wind_spd", 0)) >= 5.0 and "向かい風" in env.get("wind_dir", "") else "まくり"
    else:
        kimarite_label = "アウト展開"
        
    if manshu_special:
        kimarite_label = f"万舟特化（オッズ{int(special_odds_threshold)}倍以上厳選）"
    elif manshu_active:
        kimarite_label = "万舟波乱（展開待ち）"

    combinations = []
    all_120_combinations = []
    
    # Base candidates (probabilistic)
    candidates = []
    p1_stats = oracle_results["p1"]
    p2_stats = oracle_results["p2"]
    p3_stats = oracle_results["p3"]
    for i in range(6):
        for j in range(6):
            if i == j: continue
            for k in range(6):
                if i == k or j == k: continue
                prob_score = p1_stats[i] * p2_stats[j] * p3_stats[k]
                candidates.append({
                    "bet": f"{i+1}-{j+1}-{k+1}",
                    "score": prob_score,
                    "reason": f"AI推奨：{i+1}軸展開"
                })
    
    if manshu_special:
        filtered_bets = []
        odds_data = data.get("odds", {})
        for c in candidates:
            if special_exclude_1_head and c["bet"].startswith("1-"):
                continue
                
            odd_val = odds_data.get(c["bet"], 0.0)
            if isinstance(odd_val, (int, float)) and odd_val >= special_odds_threshold:
                c["reason"] = f"万舟特化：オッズ{int(special_odds_threshold)}倍以上"
                filtered_bets.append(c)
        all_120_combinations = sorted(filtered_bets, key=lambda x: x["score"], reverse=True)
        combinations = sorted(all_120_combinations[:bet_count], key=lambda x: x["bet"])
    elif manshu_active:
        # Generate all possible non-1-head combinations
        all_non_1_bets = []
        for h in range(1, 6): # Course 2 to 6
            others = [i for i in range(6) if i != h]
            for c2 in others:
                for c3 in others:
                    if c2 != c3:
                        h_score = oracle_results["scores"][h]
                        c2_score = oracle_results["scores"][c2]
                        c3_score = oracle_results["scores"][c3]
                        score = h_score * (c2_score * 0.5) * (c3_score * 0.3)
                        all_non_1_bets.append({
                            "bet": f"{h+1}-{c2+1}-{c3+1}",
                            "score": score,
                            "reason": f"万舟モード：{h+1}アタマ展開"
                        })
        all_120_combinations = sorted(all_non_1_bets, key=lambda x: x["score"], reverse=True)
        combinations = sorted(all_120_combinations[:bet_count], key=lambda x: x["bet"])
    else:
        all_120_combinations = sorted(candidates, key=lambda x: x["score"], reverse=True)
        combinations = sorted(all_120_combinations[:bet_count], key=lambda x: x["bet"])
    
    return {
        "kimarite": kimarite_label,
        "confidence": round(conf_score, 1),
        "confidence_label": conf_label,
        "bets": combinations,
        "all_120": all_120_combinations,
        "manshu": manshu_active or manshu_special,
        "alerts": oracle_results["alerts"]
    }

def calculate_profit_stats(history, next_invest=1000):
    if not history:
        return {
            "total_invest": 0, "total_payout": 0, "hit_rate": 0.0, "recovery_rate": 0.0, 
            "net_profit": 0, "required_odds": 0.0, "num_races": 0, "num_hits": 0
        }
    total_invest = sum(h["invest"] for h in history)
    total_payout = sum(h["payout"] for h in history)
    num_races = len(history)
    num_hits = sum(1 for h in history if h["payout"] > 0)
    
    hit_rate = (num_hits / num_races * 100)
    recovery_rate = (total_payout / total_invest * 100)
    net_profit = total_payout - total_invest
    
    loss = total_invest - total_payout
    required_odds = 0.0
    if next_invest > 0:
        required_odds = (next_invest + max(0, loss)) / next_invest
        
    return {
        "total_invest": total_invest,
        "total_payout": total_payout,
        "hit_rate": hit_rate,
        "recovery_rate": recovery_rate,
        "net_profit": net_profit,
        "required_odds": required_odds,
        "num_races": num_races,
        "num_hits": num_hits
    }

def main():
    if "history" not in st.session_state:
        st.session_state.history = []
    
    st.title("⛴️ BoatPredict Elite (Boaters JP)")
    st.markdown("🌐 Selenium搭載: SPA突破型フルオートスクレイピング＆オラクル予測")
    
    # --- 収益計算ダッシュボード (Top Section) ---
    st.markdown("<div class='metric-box' style='border-left: 8px solid #00d4ff;'>", unsafe_allow_html=True)
    st.markdown("#### 💰 収支管理ダッシュボード")
    
    # 次戦投資額の入力
    next_invest_val = st.number_input("次戦の予定投資額 (円)", min_value=100, step=100, value=1000, help="この金額を元に「捲るための必要オッズ」を計算します")
    
    stats = calculate_profit_stats(st.session_state.history, next_invest_val)
    
    col_a, col_b, col_c, col_d = st.columns([1, 1, 1.5, 2])
    col_a.metric("回収率", f"{stats['recovery_rate']:.1f}%")
    col_b.metric("的中率", f"{stats['hit_rate']:.1f}%")
    profit_color = "normal" if stats['net_profit'] >= 0 else "inverse"
    col_c.metric("合計損益", f"{stats['net_profit']:,}円", delta=stats['net_profit'], delta_color=profit_color)
    
    if stats['net_profit'] < 0:
        col_d.warning(f"🎯 **次戦必要オッズ**: **{stats['required_odds']:.2f}倍** 以上")
    else:
        col_d.success(f"📈 利益継続中！")
        
    with st.expander("📝 レース結果を記録 / 履歴管理"):
        with st.form("top_profit_form"):
            c1, c2 = st.columns(2)
            f_invest = c1.number_input("投資金額 (円)", min_value=0, step=100, value=1000)
            f_payout = c2.number_input("的中金額 (円)", min_value=0, step=10, value=0)
            if st.form_submit_button("収支を記録"):
                st.session_state.history.append({
                    "date": str(datetime.now().strftime("%H:%M")),
                    "invest": f_invest,
                    "payout": f_payout
                })
                st.rerun()

        if st.session_state.history:
            # 最新5件を表示
            st.dataframe(pd.DataFrame(st.session_state.history).tail(5), use_container_width=True, hide_index=True)
            if st.button("履歴をすべてリセット"):
                st.session_state.history = []
                st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)
    
    st.markdown("<div class='metric-box'>", unsafe_allow_html=True)
    col1, col2, col3, col4 = st.columns([2, 1, 1, 2])
    target_date = col1.date_input("日付")
    venue_name = col2.selectbox("会場", list(VENUES.keys()))
    race_no = col3.selectbox("レース番号", list(range(1, 13)))
    prediction_mode = col4.selectbox("🤖 モード", ["通常", "万舟的中", "万舟特化"])
    
    # Detailed Settings for Mobile
    with st.expander("⚙️ 詳細な予測設定（万舟モード・オッズ閾値など）", expanded=False):
        st.markdown("<div style='font-size: 14px; color: #666; margin-bottom: 10px;'>※モバイル端末でも設定しやすいようにこちらに配置しました。</div>", unsafe_allow_html=True)
        col_s1, col_s2 = st.columns(2)
        with col_s1:
            manshu_points = st.selectbox("万舟モード推奨点数", [20, 30], index=0)
            special_exclude_1_head = st.radio("特化モード 1号艇1着", ["1頭入り", "1頭切り"], horizontal=True) == "1頭切り"
        with col_s2:
            special_odds_threshold = st.radio("特化モード オッズ閾値", [40.0, 50.0], format_func=lambda x: f"{int(x)}倍以上", horizontal=True)
            debug_mode = st.checkbox("デバッグモード", value=False)
    
    # 荒れる度の表示
    roughness = VENUE_ROUGHNESS_MAP.get(venue_name, 15.0)
    roughness_color = "#ff4b4b" if roughness >= 17.5 else "#ffa500" if roughness >= 15.0 else "#005ce6"
    
    st.markdown(f"""
    <div style="background-color: #f8f9fa; padding: 10px; border-radius: 8px; border-left: 5px solid {roughness_color}; margin-top: 10px;">
        <span style="font-size: 14px; color: #666;">過去1年間の統計</span><br>
        <span style="font-size: 18px; font-weight: bold; color: {roughness_color};">📊 荒れる度: {roughness}%</span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    bet_points = st.radio("表示する推奨買い目（通常モードの3連単）", [6, 10], horizontal=True, format_func=lambda x: f"最強 {x} 点に絞る")
    st.markdown("</div>", unsafe_allow_html=True)

    if st.button("AI予想を生成（※裏でブラウザを立ち上げてデータ収集します。5〜10秒ほどお待ちください）", type="primary"):
        with st.spinner("ブラウザを起動し、ページ内の全てのタブ（出走・連体率・気象・モーター）をクリック巡回して取得中..."):
            data = scrape_full_boaters_workflow(str(target_date), VENUES[venue_name], race_no)
            oracle_results = calculate_oracle(data, venue_name)
            
            # If manshu mode, use selected points
            actual_bet_points = manshu_points if prediction_mode in ["万舟的中", "万舟特化"] else bet_points
            res_analysis = analyze_kimarite_and_bets(oracle_results, data, venue_name, actual_bet_points, prediction_mode=prediction_mode, special_odds_threshold=special_odds_threshold, special_exclude_1_head=special_exclude_1_head)
            
            st.session_state.result = {
                "data": data, "oracle": oracle_results, "analysis": res_analysis, "prediction_mode": prediction_mode
            }

    if "result" in st.session_state:
        res = st.session_state.result
        data = res["data"]
        
        if debug_mode and "raw_text" in data:
            with st.expander("🔍 取得データ（生テキスト）"):
                st.text(data["raw_text"])
        
        env = data["env"]
        ana = res["analysis"]
        
        st.markdown("---")
        
        # 動的荒れる度の算出
        dyn_roughness = calculate_dynamic_roughness(data, venue_name)
        roughness_color = "#ff4b4b" if dyn_roughness >= 45.0 else "#ffa500" if dyn_roughness >= 30.0 else "#005ce6"
        
        st.markdown(f"""
        <div style="background-color: #f8f9fa; padding: 15px; border-radius: 10px; border-left: 8px solid {roughness_color}; margin-bottom: 20px;">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <span style="font-size: 14px; color: #666;">リアルタイム解析：会場・気象・展示・戦績</span><br>
                    <span style="font-size: 26px; font-weight: bold; color: {roughness_color};">🔥 荒れる度: {dyn_roughness}%</span>
                </div>
                <div style="text-align: right; font-size: 12px; color: #777;">
                    ベース統計: {roughness}%<br>
                    気象・展示補正済
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        st.info(f"🍃 気象パネル： 風向 **{env.get('wind_dir','-')}** / 風速 **{env.get('wind_spd',0)}m** / 波高 **{env.get('wave',0)}cm** / 水温 **{env.get('water_temp', '-')}** / 水位 **{env.get('water_level', '-')}**")
        
        # Boat Detailed Stats (Moved to Top)
        st.markdown("### 🛥️ 各出場艇の最新解析スタッツ")
        df_list = []
        for i, b in enumerate(data["boats"]):
            def fmt(nm, rk): return f"{b[nm]} [{b.get(rk,99)}位]" if str(b[nm]) not in ["-",""] else "-"
            
            course_st_val = b.get("course_avg_st", "-")
            course_rk_val = b.get("course_avg_st_rank", "-")
            cm_str = f"{course_st_val} [{course_rk_val}位]" if course_st_val != "-" else "-"
            
            df_list.append({
                "枠": f"{i+1}号艇",
                "選手名(級)": b["name"],
                "枠番平均ST": cm_str,
                "展示タイム": fmt("ex_time", "ex_rank"),
                "1周タイム": fmt("lap_time", "lap_rank"),
                "周り足": b.get("turn", "-"),
                "直線足": b.get("straight", "-"),
                "1着率(AI)": f"{res['oracle']['p1'][i]*100:.1f}%",
                "2着率(AI)": f"{res['oracle']['p2'][i]*100:.1f}%",
                "3着率(AI)": f"{res['oracle']['p3'][i]*100:.1f}%",
                "総合スコア": round(res["oracle"]["scores"][i], 2),
            })
        st.dataframe(pd.DataFrame(df_list), use_container_width=True, hide_index=True)

        if ana["alerts"]:
            with st.expander("⚠️ AIからの警告メッセージ"):
                for a in ana["alerts"]:
                    st.warning(a)

        # Main Display with Tabs (2 tabs now)
        tab_rec, tab_all = st.tabs(["🎯 推奨買い目", "📊 全120通り解析"])
        
        with tab_rec:
            st.markdown(f"### 🔍 {ana['kimarite']} 展開 (自信度: {ana['confidence_label']} / 的中期待値: {ana['confidence']}%)")
            
            # 合成オッズの算出と表示
            pred_mode = res.get("prediction_mode", "通常")
            syn_odds = calculate_synthetic_odds(ana["bets"], data["odds"])
            if syn_odds > 0:
                syn_color = "#ff4b4b" if syn_odds >= 10.0 else "#ffa500" if syn_odds >= 3.0 else "#005ce6"
                mode_name = f"{pred_mode}モード" if pred_mode != "通常" else "最強買い目"
                st.markdown(f"""
                <div style="background-color: #f8f9fa; padding: 10px; border-radius: 8px; border-left: 5px solid {syn_color}; margin-bottom: 15px; display: inline-block; box-shadow: 1px 1px 4px rgba(0,0,0,0.1);">
                    <span style="font-size: 14px; color: #666;">🎯 {mode_name} ({len(ana['bets'])}点) の合成オッズ</span><br>
                    <span style="font-size: 20px; font-weight: bold; color: {syn_color};">{syn_odds:.2f} 倍</span>
                </div>
                """, unsafe_allow_html=True)

            if pred_mode in ["万舟的中", "万舟特化"]:
                st.markdown(f"<div style='background-color: #2b1d1d; color: #ff4b4b; padding: 10px; border-radius: 5px; margin-bottom: 15px; border: 1px solid #ff4b4b;'>🔥 {pred_mode}モード発動中：上位 **{len(ana['bets'])}** 点を表示</div>", unsafe_allow_html=True)
            
            # Display as cards
            for i in range(0, len(ana["bets"]), 2):
                cols = st.columns(2)
                for j in range(2):
                    if i + j < len(ana["bets"]):
                        bet = ana["bets"][i + j]
                        with cols[j]:
                            odds_val = data["odds"].get(bet["bet"], "取得中..")
                            odds_str = f"**{odds_val} 倍**" if isinstance(odds_val, (float, int)) else odds_val
                            
                            st.markdown(f"""
                            <div style="background-color: white; border: 2px solid #333; border-radius: 15px; padding: 15px; margin-bottom: 10px; box-shadow: 2px 2px 5px rgba(0,0,0,0.1);">
                                <div style="display: flex; justify-content: space-between; align-items: center;">
                                    <div style="font-size: 20px; font-weight: bold;">
                                        { ' '.join([f'<span style="background-color: {"#f8f9fa" if c=="1" else "#333" if c=="2" else "#ff4b4b" if c=="3" else "#005ce6" if c=="4" else "#ffa500" if c=="5" else "#28a745"}; color: {"#333" if c=="1" else "white"}; border-radius: 50%; width: 33px; height: 33px; display: inline-flex; align-items: center; justify-content: center; margin-right: 5px; border: 1px solid #ccc;">{c}</span>' for c in bet["bet"].split("-")]) }
                                    </div>
                                    <div style="color: #ff4b4b; font-size: 20px; font-weight: bold;">{odds_str}</div>
                                </div>
                                <div style="font-size: 13px; color: #666; margin-top: 10px; border-top: 1px solid #eee; padding-top: 5px;">{bet["reason"]}</div>
                            </div>
                            """, unsafe_allow_html=True)

        with tab_all:
            st.markdown("### 📊 3連単全120通り (期待値/確率順)")
            st.markdown("AIが算出した的中期待スコアの高い順に全ての組み合わせを表示しています。")
            
            all_df_data = []
            for item in ana["all_120"]:
                odds_v = data["odds"].get(item["bet"], "-")
                score_v = item["score"]
                # Display rounded scores for readability
                all_df_data.append({
                    "順位": len(all_df_data) + 1,
                    "買い目": item["bet"],
                    "オッズ": odds_v,
                    "期待スコア": round(score_v * 1000, 2), # Scale for readability
                    "解析根拠": item["reason"]
                })
            
            st.dataframe(pd.DataFrame(all_df_data), use_container_width=True, hide_index=True)

if __name__ == "__main__":
    main()
