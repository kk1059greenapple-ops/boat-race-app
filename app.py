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
import json
import itertools
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

async def _headless_boaters_text_extraction(url, venue_cd):
    """
    各サブページのURLへ直接ナビゲートしてテキスト／HTMLを取得する。
    タブクリックへの依存を完全に排除し、Chakra UIの動的クラス名変更に強くする。
    
    url 例: https://boaters-boatrace.com/race/mikuni/2026-05-14/1R
      → 出走表  : {url}/race-detail
      → 直前情報: {url}/last-minute
      → 連対率  : {url}/data
      → モーター: {url}/motor
      → オッズ  : {url}/odds?odds-content=3rentan
    """
    tab_texts = {}
    base_url = url  # e.g. https://boaters-boatrace.com/race/mikuni/2026-05-14/1R

    # テキスト取得対象のサブページ (名前: サフィックス)
    sub_pages = [
        ('出走表',     f"{base_url}/race-detail"),
        ('直前情報',   f"{base_url}/last-minute"),
        ('連対率・展開', f"{base_url}/data"),
        ('モーター情報', f"{base_url}/motor"),
    ]

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]
        )
        page = await browser.new_page()
        try:
            # ── テキストベースのサブページを順次取得 ──────────────────────
            for tab_name, sub_url in sub_pages:
                try:
                    await page.goto(sub_url, wait_until="domcontentloaded", timeout=30000)
                    await page.wait_for_timeout(2000)
                    tab_texts[tab_name] = await page.evaluate("() => document.body.innerText")

                    # 直前情報 → オリジナル展示サブタブも取得
                    if tab_name == '直前情報':
                        try:
                            await page.get_by_text("オリジナル展示", exact=True).click(timeout=3000)
                            await page.wait_for_timeout(1500)
                            tab_texts['オリジナル展示'] = await page.evaluate("() => document.body.innerText")
                        except:
                            pass

                    # 連対率・展開 → 直近6ヶ月 / 一般戦 / 当地 サブタブも取得
                    if tab_name == '連対率・展開':
                        for sub_tab in ["直近6ヶ月", "一般戦", "当地"]:
                            try:
                                await page.get_by_text(sub_tab, exact=True).nth(0).click(timeout=2000)
                                await page.wait_for_timeout(1000)
                                tab_texts[f"連対率・展開_{sub_tab}"] = await page.evaluate("() => document.body.innerText")
                            except:
                                pass

                except Exception as e:
                    print(f"[scraper] {tab_name} 取得エラー: {e}")

            # ── オッズ: HTML ごと取得して BeautifulSoup で構造解析する ────
            # URL直接ナビゲートで3連単・マトリクス表示を確実に開く
            # 動的クラス名のタブクリックは一切行わない
            try:
                odds_url = f"{base_url}/odds?odds-content=3rentan"
                await page.goto(odds_url, wait_until="domcontentloaded", timeout=30000)
                # 「締切時オッズ」は不変テキストなので待機条件として使用
                try:
                    await page.wait_for_selector("text='締切時オッズ'", timeout=10000)
                except:
                    # 発売中など「締切時」でない場合は「オッズ」テキストで待機
                    try:
                        await page.wait_for_selector("text='オッズ'", timeout=5000)
                    except:
                        await page.wait_for_timeout(3000)
                # HTMLを丸ごと取得（BeautifulSoupで解析）
                tab_texts['オッズ_html'] = await page.content()
            except Exception as e:
                print(f"[scraper] オッズHTML取得エラー: {e}")

        except Exception as e:
            print(f"[scraper] ブラウザエラー: {e}")
        await browser.close()
    return tab_texts


def parse_3rentan_odds_from_html(html_content: str) -> dict:
    """
    Boatersの3連単オッズページのHTMLをBeautifulSoupで構造解析してオッズ辞書を返す。
    動的に変化する css-xxx クラス名には一切依存しない。

    HTMLの安定した構造:
      hr[aria-orientation='vertical'] が各1着艇のセクション区切り
      各セクション:
        先頭div のテキスト = "N.選手名" (1着艇番)
        続くdiv (2着グループ):
          子div[0] のテキスト = 2着艇番
          子div[1] 以降に button 要素が含まれ、各 button の親div に 3着艇番divも存在
    """
    odds = {}
    if not html_content:
        return odds
    try:
        soup = BeautifulSoup(html_content, 'html.parser')

        # hr で区切られた1着セクションの親要素を特定
        dividers = soup.find_all('hr', attrs={'aria-orientation': 'vertical'})
        if not dividers:
            return odds
        parent = dividers[0].parent
        if not parent:
            return odds

        # 親要素の直接子要素を hr で分割 → 各1着ブロックを収集
        sections = []
        current = None
        for child in parent.children:
            if not hasattr(child, 'name') or child.name is None:
                continue
            if child.name == 'hr':
                if current is not None:
                    sections.append(current)
                current = None
            else:
                if current is None:
                    current = child
        if current is not None:
            sections.append(current)

        for section in sections:
            # 1着艇番: 先頭のdivのテキストが "N.選手名" 形式
            first_div = section.find('div', recursive=False)
            if not first_div:
                continue
            header_text = first_div.get_text(strip=True)
            m = re.match(r'^(\d)\.', header_text)
            if not m:
                continue
            b1 = m.group(1)

            # 2着グループ: 先頭div以降のすべての直接子div
            direct_children = [
                c for c in section.children
                if hasattr(c, 'name') and c.name == 'div'
            ]
            for b2_group in direct_children[1:]:  # 先頭(ヘッダー)をスキップ
                sub_children = [
                    c for c in b2_group.children
                    if hasattr(c, 'name') and c.name == 'div'
                ]
                if len(sub_children) < 2:
                    continue

                # 2着艇番: 最初の子divのテキスト（単一数字 1-6）
                b2_text = sub_children[0].get_text(strip=True)
                if not b2_text.isdigit() or not (1 <= int(b2_text) <= 6):
                    continue
                b2 = b2_text

                # 3着+オッズのペア: odds_container 内の全 button を走査
                odds_container = sub_children[1]
                buttons = odds_container.find_all('button')
                for btn in buttons:
                    odds_text = btn.get_text(strip=True).replace(',', '')
                    try:
                        odds_val = float(odds_text)
                    except:
                        continue

                    # ボタンの兄弟divが3着艇番
                    btn_parent = btn.parent
                    if not btn_parent:
                        continue
                    sibling_divs = [
                        c for c in btn_parent.children
                        if hasattr(c, 'name') and c.name == 'div'
                    ]
                    if not sibling_divs:
                        continue
                    b3_text = sibling_divs[0].get_text(strip=True)
                    if b3_text.isdigit() and 1 <= int(b3_text) <= 6:
                        b3 = b3_text
                        if b1 != b2 and b1 != b3 and b2 != b3 and 1.0 < odds_val < 10000.0:
                            odds[f"{b1}-{b2}-{b3}"] = odds_val

    except Exception as e:
        print(f"[parse_odds] 解析エラー: {e}")
    return odds

def scrape_full_boaters_workflow(date_str, venue_cd, race_no):
    # url mapping correctly uses literal venue code eg hamanako and date string
    url = f"https://boaters-boatrace.com/race/{venue_cd}/{date_str}/{race_no}R"
    
    tab_texts = asyncio.run(_headless_boaters_text_extraction(url, venue_cd))
    
    extracted = {
        "env": {"wind_spd": 0, "wind_dir": "無風", "wave": "-", "water_level": "-", "water_temp": "-", "anteiban": False},
        "boats": [{"course": i+1, "name": "-", "class": "-", 
                   "top1_rate": 15.0, "top2_rate": 20.0, "top3_rate": 35.0, "win_rate": 10.0,
                   "avg_st": 0.16, "avg_st_rank": 3.0, "course_avg_st": "-", "course_avg_st_rank": "-",
                   "kimarite_nige": 0.0, "kimarite_sashi": 0.0, "kimarite_makuri": 0.0,
                   "ex_st": "-", "motor_2ren": 30.0, "motor_3ren": 40.0, "f_count": "-",
                   "turn": "-", "straight": "-", "lap_time": "-", "ex_time": "-", "tilt": 0.0} 
                  for i in range(6)]
    }
    
    # オッズHTMLキーを除いたテキスト値のみで環境変数をパース
    for key, text in tab_texts.items():
        if key == 'オッズ_html':
            continue  # HTMLを誤ってテキストパースしない
        ws = re.search(r'風速\s*([\d\.]+)[\s\n]*m', text)
        if ws: extracted["env"]["wind_spd"] = float(ws.group(1))
        wv = re.search(r'波高\s*([\d\.]+)[\s\n]*cm', text)
        if wv: extracted["env"]["wave"] = float(wv.group(1))
        wl = re.search(r'(?:潮位|水位)\s*([0-9\-]+)\s*cm', text)
        if wl: extracted["env"]["water_level"] = wl.group(1) + "cm"
        wt = re.search(r'水温\s*([0-9\.]+)\s*℃', text)
        if wt: extracted["env"]["water_temp"] = wt.group(1) + "℃"
        if "安定板" in text or "安定板使用" in text:
            extracted["env"]["anteiban"] = True
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
                        # F情報の抽出 (F1, F2等)
                        for j in range(i, min(i+10, len(lines))):
                            if "F" in lines[j] and re.search(r'F[1-9]', lines[j]):
                                extracted["boats"][idx]["f_count"] = lines[j]
                                break
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

    # 3. モーター情報
    if 'モーター情報' in tab_texts:
        lines = [line.strip() for line in tab_texts['モーター情報'].split('\n') if line.strip()]
        for idx in range(6):
            b_idx = str(idx + 1)
            for i, line in enumerate(lines):
                # 枠番のあとにモーターNoや2連対率が続く構造
                if line == b_idx and i+5 < len(lines):
                    # 2連対率 (例: 32.8%) を探す
                    for j in range(i+1, i+10):
                        m = re.search(r'(\d+\.\d+)\s*%', lines[j])
                        if m:
                            extracted["boats"][idx]["motor_2ren"] = float(m.group(1))
                            break
                    break

    # 4. 連対率・展開 (Including Boat 1 loss patterns)
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

        # AI３連対率に依存せず、直近6ヶ月・一般戦・当地の全戦績テキストから過去1年間の傾向スコアも含めて総合抽出
        for idx in range(6):
            b_idx = str(idx + 1)
            match_name = extracted["boats"][idx]["name"]
            collected_percents = []
            
            for t_key, text_content in tab_texts.items():
                if any(k in t_key for k in ["連対率・展開", "出走表"]):
                    t_lines = [l.strip() for l in text_content.split('\n') if l.strip()]
                    for i, line in enumerate(t_lines):
                        if line == b_idx and i+5 < len(t_lines) and (match_name in t_lines[i+1] or t_lines[i+1] in match_name):
                            percents = [clean_float(t_lines[j]) for j in range(i+2, min(i+25, len(t_lines))) if "%" in t_lines[j]]
                            if len(percents) >= 3:
                                collected_percents.append(percents)
                            break
            
            if collected_percents:
                # 各モード（直近6ヶ月、一般戦、当地等）の戦績を平均化して総合勝率・連対率を算出
                avg_p1 = sum(p[0] for p in collected_percents) / len(collected_percents)
                avg_p2 = sum(p[1] for p in collected_percents) / len(collected_percents)
                avg_p3 = sum(p[2] for p in collected_percents) / len(collected_percents)
                
                extracted["boats"][idx]["top1_rate"] = avg_p1
                extracted["boats"][idx]["top2_rate"] = avg_p2
                extracted["boats"][idx]["top3_rate"] = avg_p3
                # 1着率は取得データ内で最も信頼のおける指標をベースにする
                extracted["boats"][idx]["win_rate"] = avg_p1 if len(collected_percents[0]) < 5 else sum(p[2] for p in collected_percents if len(p) >= 5) / sum(1 for p in collected_percents if len(p) >= 5)
                    
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
        
    # ── オッズ: BeautifulSoupによるHTML構造解析（動的クラス名に非依存）────
    extracted["odds"] = {}
    if 'オッズ_html' in tab_texts:
        parsed = parse_3rentan_odds_from_html(tab_texts['オッズ_html'])
        extracted["odds"].update(parsed)
        print(f"[scraper] オッズ取得件数: {len(parsed)} 件")

        # フォールバック: HTMLパースで取得数が著しく少ない場合のみ、
        # テキスト正規表現で「N-N-N 数値」形式を補完
        if len(parsed) < 30:
            raw_text = ""
            try:
                raw_text = BeautifulSoup(tab_texts['オッズ_html'], 'html.parser').get_text()
            except:
                pass
            triplets = re.finditer(
                r'([1-6])[\s\-－]+([1-6])[\s\-－]+([1-6])[\s\n]+([\d\.]{2,10})',
                raw_text
            )
            for m in triplets:
                b1, b2, b3, val = m.groups()
                key = f"{b1}-{b2}-{b3}"
                if b1 != b2 and b1 != b3 and b2 != b3 and key not in extracted["odds"]:
                    ov = clean_float(val)
                    if 1.0 < ov < 10000.0:
                        extracted["odds"][key] = ov

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

def calculate_dynamic_roughness(data, venue_name, oracle_results=None):
    # 開催地ごとのベース統計（万舟率等）
    base = VENUE_ROUGHNESS_MAP.get(venue_name, 15.0)
    roughness = base
    
    env = data.get("env", {})
    wind_spd = clean_float(env.get("wind_spd", 0.0))
    wave = clean_float(env.get("wave", 0.0))
    
    # 安定板装着補正（まくり万舟率が低下するため減算）
    if env.get("anteiban", False):
        roughness -= 15.0
        
    # 1. 気象条件による加算 (最大 +20%)
    if wind_spd >= 5.0: roughness += 10.0
    if wave >= 5.0: roughness += 10.0
    
    boats = data.get("boats", [])
    if not boats: return round(roughness, 1)
    
    # 2. 選手の戦績・実力格差 (最大 +25%)
    # 1号艇のクラス・勝率の不安
    b1_win = clean_float(boats[0].get("win_rate", 50.0))
    if boats[0].get("class") in ["B1", "B2"]:
        roughness += 12.0
    elif b1_win < 40.0:
        roughness += 10.0
        
    # 外枠（3〜6号艇）に1号艇より勝率の高い選手（A1級等）がいるか
    outside_win_rates = [clean_float(b.get("win_rate", 0.0)) for b in boats[2:]]
    if outside_win_rates and max(outside_win_rates) > b1_win + 5.0:
        roughness += 12.0

    # 3. オリジナル展示・各種タイムの異常（最大 +25%)
    # 1号艇の各種展示タイムが下位（4位以下）
    try:
        ex_r = int(boats[0].get("ex_rank", 1))
        lap_r = int(boats[0].get("lap_rank", 1))
        if ex_r >= 4 or lap_r >= 4:
            roughness += 12.0
    except: pass
    
    # 外枠勢が一番時計（直線足・周り足・1周タイムなど）
    ex_times = [clean_float(b.get("ex_time", 9.99)) for b in boats]
    lap_times = [clean_float(b.get("lap_time", 9.99)) for b in boats]
    straight_times = [clean_float(b.get("straight", 9.99)) for b in boats]
    
    def check_outside_best(times_list):
        valid = [t for t in times_list if t < 9.0]
        if valid:
            best_t = min(valid)
            best_idx = [i for i, t in enumerate(times_list) if t == best_t]
            if any(idx >= 2 for idx in best_idx): # 3〜6号艇
                return True
        return False
        
    if check_outside_best(ex_times) or check_outside_best(lap_times) or check_outside_best(straight_times):
        roughness += 15.0

    # 4. モーター情報（最大 +10%)
    m1_2ren = clean_float(boats[0].get("motor_2ren", 35.0))
    outside_motors = [clean_float(b.get("motor_2ren", 30.0)) for b in boats[2:]]
    if m1_2ren < 30.0 and outside_motors and max(outside_motors) >= 40.0:
        roughness += 10.0

    # 5. AI推しと市場オッズ推しの乖離（ギャップ）判定による大幅補正
    if oracle_results:
        odds_dict = data.get("odds", {})
        if odds_dict:
            min_all_odds = min([ov for ov in odds_dict.values() if ov > 1.0] or [10.0])
            market_fav_boat = int([k for k, v in odds_dict.items() if v == min_all_odds][0].split("-")[0])
            p1_list = list(oracle_results["p1"])
            market_fav_win = p1_list[market_fav_boat - 1]
            
            # 市場人気の艇が、AI予想でも十分に高い勝率（トップ2以内または勝率35%以上）を誇る場合は「市場とAIの合致」とみなす
            sorted_p1 = sorted(p1_list, reverse=True)
            is_ai_aligned = market_fav_win >= 0.35 or market_fav_win >= sorted_p1[1]
            
            if is_ai_aligned:
                if min_all_odds < 7.5:
                    roughness -= 25.0 # 合致しておりオッズも低い（本命・順当優勢）
                elif min_all_odds < 12.0:
                    roughness -= 15.0
            else:
                # AI推しと市場人気が全く異なる（乖離大・オッズ妙味や波乱の可能性大）
                roughness += 15.0

    return min(max(round(roughness, 1), 5.0), 98.5)

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
    anteiban = env.get("anteiban", False)
    
    # 安定板装着補正（過去1年の全国データ換算）
    if anteiban:
        s1[0] *= 1.35
        s2[0] *= 1.25
        s1[3] *= 0.5
        s1[4] *= 0.5
        s1[5] *= 0.5
        alerts.append("【安定板使用】過去1年データ換算：トップスピード抑制によりイン逃げ期待値上昇・外枠まくり率低下")

    # AI自己学習エンジン（過去のフィードバック実績に基づく動的スコア補正）
    learning_db = load_learning_db()
    venue_records = [r for r in learning_db if r.get("venue") == venue]
    if venue_records:
        learned_1_wins = sum(1 for r in venue_records if r.get("actual_result", "").startswith("1-"))
        win_rate_1 = learned_1_wins / len(venue_records)
        
        if win_rate_1 < 0.40 and len(venue_records) >= 3:
            s1[0] *= 0.85
            alerts.append(f"【AI自己学習エンジン】当会場({venue})の過去フィードバック実績(イン勝率{win_rate_1*100:.1f}%)に基づき、1号艇のスコアを下向補正しました。")
        elif win_rate_1 > 0.65 and len(venue_records) >= 3:
            s1[0] *= 1.15
            alerts.append(f"【AI自己学習エンジン】当会場({venue})の過去フィードバック実績(イン勝率{win_rate_1*100:.1f}%)に基づき、1号艇のスコアを上方補正しました。")
    
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
        ext_diff = (clean_float(prev_b.get("ex_time", 6.85), 6.85) - clean_float(b.get("ex_time", 6.85), 6.85))
        ast_diff = (clean_float(prev_b.get("avg_st", 0.16), 0.16) - clean_float(b.get("avg_st", 0.16), 0.16))
        v_scores[i] = (ext_diff * 10 * 0.6) + (ast_diff * 10 * 0.4)
        if v_scores[i] >= 0.5:
            alerts.append(f"【V-Score】{i+1}号艇 直まくり優位性あり")

    # --- 3. Holistic Strength Score Calculation ---
    holistic_scores = [0.0] * 6
    for i in range(6):
        b = boats[i]
        
        if venue == "江戸川":
            # 江戸川専用ロジック (オリジナル展示非公表対策)
            local_win = clean_float(b.get("win_rate", b.get("top3_rate", 0.0)))
            motor_2ren = clean_float(b.get("motor_2ren", 30.0))
            ex_time = clean_float(b.get("ex_time", 7.0), 7.0)
            
            ex_perf = max(0, (7.0 - ex_time) * 200)  # 通常展示タイムの比重を高める
            win_perf = local_win * 2.0               # 当地勝率の比重を高める
            motor_perf = motor_2ren * 1.5            # モーター連対率の比重を高める
            st_perf = max(0, (0.25 - clean_float(b.get("course_avg_st", 0.18))) * 100)
            
            total = ex_perf + win_perf + motor_perf + st_perf
        else:
            # A. Machine Performance (Original Exhibition)
            m_perf = 0
            try:
                lap_score = max(0, (38.0 - clean_float(b.get("lap_time", 38.0))) * 40)
                turn_score = max(0, (6.0 - clean_float(b.get("turn", 6.0))) * 20)
                strt_score = max(0, (8.0 - clean_float(b.get("straight", 8.0))) * 20)
                m_perf = lap_score + turn_score + strt_score
            except: pass
            
            # B. Exhibition Time
            ex_perf = max(0, (7.0 - clean_float(b.get("ex_time", 7.0), 7.0)) * 100)
            
            # C. Winning Records (Win Rates)
            win_perf = (b.get("top1_rate", 0) * 0.6 + b.get("top2_rate", 0) * 0.3 + b.get("top3_rate", 0) * 0.1)
            
            # D. Start Ability
            st_perf = max(0, (0.25 - clean_float(b.get("course_avg_st", 0.18))) * 200)
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
    manshu_special = (prediction_mode == "中穴・大穴的中")
    

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
        kimarite_label = f"中穴・大穴（オッズ{int(special_odds_threshold)}倍以上厳選）"
    elif manshu_active:
        kimarite_label = "万舟的中（オッズ100倍以上厳選）"

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
                # 1着確率(p1)を最重視しつつ、2着・3着確率の極端な格差を均すことで、勝率上位の複数艇の頭を自然に上位へランクインさせる
                prob_score = p1_stats[i] * (p2_stats[j]**0.5) * (p3_stats[k]**0.5)
                candidates.append({
                    "bet": f"{i+1}-{j+1}-{k+1}",
                    "score": prob_score,
                    "reason": f"AI推奨：{i+1}軸展開"
                })
                
    # --- 適正オッズ(理論オッズ)の計算 ---
    total_score = sum(c["score"] for c in candidates)
    for c in candidates:
        if total_score > 0 and c["score"] > 0:
            hit_prob = c["score"] / total_score
            c["fair_odds"] = round(1.0 / hit_prob, 1)
        else:
            c["fair_odds"] = 9999.9
    
    if manshu_special:
        filtered_bets = []
        odds_data = data.get("odds", {})
        for c in candidates:
            if special_exclude_1_head and c["bet"].startswith("1-"):
                continue
                
            odd_val = odds_data.get(c["bet"], 0.0)
            if isinstance(odd_val, (int, float)) and odd_val >= special_odds_threshold:
                c["reason"] = f"中穴・大穴：オッズ{int(special_odds_threshold)}倍以上"
                filtered_bets.append(c)
        all_120_combinations = sorted(filtered_bets, key=lambda x: x["score"], reverse=True)
        combinations = sorted(all_120_combinations[:bet_count], key=lambda x: x["bet"])
    elif manshu_active:
        filtered_bets = []
        odds_data = data.get("odds", {})
        for c in candidates:
            if special_exclude_1_head and c["bet"].startswith("1-"):
                continue
                
            odd_val = odds_data.get(c["bet"], 0.0)
            if isinstance(odd_val, (int, float)) and odd_val >= 100.0:
                c["reason"] = f"万舟的中：オッズ100倍以上"
                filtered_bets.append(c)
        all_120_combinations = sorted(filtered_bets, key=lambda x: x["score"], reverse=True)
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

HISTORY_FILE = "profit_history.json"
LEARNING_DB_FILE = "boat_learning_db.json"

def load_history():
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                # Ensure all users exist in the loaded data
                for u in ["T", "K", "H"]:
                    if u not in data:
                        data[u] = []
                return data
        except:
            return {"T": [], "K": [], "H": []}
    return {"T": [], "K": [], "H": []}

def save_history(data):
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def load_learning_db():
    if os.path.exists(LEARNING_DB_FILE):
        try:
            with open(LEARNING_DB_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return []
    return []

def save_learning_db(data):
    with open(LEARNING_DB_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def main():
    if "all_histories" not in st.session_state:
        st.session_state.all_histories = load_history()
    
    st.title("⛴️ BoatPredict Elite (Boaters JP)")
    st.markdown("🌐 Selenium搭載: SPA突破型フルオートスクレイピング＆オラクル予測")
    
    # --- 究極の万舟レーダー (Sidebar) ---
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 🔥 究極の万舟レーダー（直前展示解析）")
    search_date = st.sidebar.date_input("分析する日付を選択", datetime.now(), help="過去や明日のレースを分析できます。")
    st.sidebar.caption("開始15分前〜直前の全レースから、展示タイム異常や気象条件を加味して波乱候補を抽出します。")
    if st.sidebar.button("🔍 開始15分前の波乱レースを検索 (展示・直前情報取得)", use_container_width=True):
        st.session_state.run_rough_search = True
        st.session_state.run_weather_search = False
        st.session_state.target_search_date = search_date.strftime("%Y%m%d")
        
    if st.sidebar.button("🌪️ 全場の強風・高波会場を検索 (最新気象レーダー)", use_container_width=True):
        st.session_state.run_weather_search = True
        st.session_state.run_rough_search = False
        st.session_state.target_search_date = search_date.strftime("%Y%m%d")
        
    if st.session_state.get("run_rough_search", False):
        st.markdown("---")
        st.header("🔥 究極の万舟レーダー（直前展示解析） 解析結果")
        
        with st.status("🚀 解析フェーズ1: 開催中の直前レース（開始15分前）を抽出中...", expanded=True) as status:
            import rough_race_finder
            import importlib
            importlib.reload(rough_race_finder)
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            
            # Step 1: Lightweight Primary Scraping
            t_date = st.session_state.get("target_search_date")
            if loop.is_running():
                import nest_asyncio
                nest_asyncio.apply()
                future = asyncio.ensure_future(rough_race_finder.find_rough_races_today(t_date))
                candidates, date_hd = loop.run_until_complete(future)
            else:
                candidates, date_hd = loop.run_until_complete(rough_race_finder.find_rough_races_today(t_date))
            
            if not candidates:
                status.update(label="❌ 対象レースが見つかりませんでした。", state="error")
                st.session_state.run_rough_search = False
            else:
                status.write(f"✅ {len(candidates)}件の直前レース対象（フォールバック含む）を抽出しました。")
                status.update(label="🔍 解析フェーズ2: オリジナル展示・直前情報を深掘りパース中...", state="running")
                
                # Step 2: Deep Dive with Playwright
                final_results = []
                target_date_str = f"{date_hd[:4]}-{date_hd[4:6]}-{date_hd[6:8]}"
                
                # 特定会場の独占を防ぐため、会場ごとに候補を分散させて選出
                diverse_candidates = []
                temp_venue_counts = {}
                for cand in candidates:
                    v_name = cand["venue"]
                    v_count = temp_venue_counts.get(v_name, 0)
                    if v_count < 3: # 各場最大3件まで詳細解析へ
                        diverse_candidates.append(cand)
                        temp_venue_counts[v_name] = v_count + 1
                
                target_candidates = diverse_candidates[:30]
                
                status.write(f"📊 {len(temp_venue_counts)}会場から計{len(target_candidates)}レースを詳細展示解析対象に選出しました。")
                
                progress_bar = st.progress(0)
                for i, cand in enumerate(target_candidates):
                    v_name = cand["venue"]
                    v_cd = VENUES_METADATA.get(v_name, {}).get("cd", "unknown")
                    r_no = cand["race_no"]
                    deadline_str = cand.get("deadline", "-")
                    
                    status.write(f"⏳ 展示解析中: {v_name} {r_no}R (締切予定: {deadline_str})...")
                    
                    try:
                        # Boatersの詳細データを取得
                        detail = scrape_full_boaters_workflow(target_date_str, v_cd, r_no)
                        
                        # 万舟スコアリング
                        m_score = 0
                        m_reasons = cand["reasons"].copy()
                        
                        s1_boats = cand.get("boats", [])
                        b1_s1 = s1_boats[0] if s1_boats else {}
                        
                        # 1. 1号艇の実力不安 (大幅加点)
                        b1_wr = b1_s1.get("win_rate", 0)
                        if b1_wr > 0:
                            if b1_wr < 4.5: m_score += 60 # 激アツ
                            elif b1_wr < 5.5: m_score += 40
                            elif b1_wr < 6.5: m_score += 20

                        # 2. 混戦度 (実力均衡) 判定
                        win_rates = [b.get("win_rate", 0) for b in s1_boats if b.get("win_rate", 0) > 0]
                        if len(win_rates) >= 4:
                            avg_wr = sum(win_rates) / len(win_rates)
                            wr_std = (sum([(w - avg_wr)**2 for w in win_rates]) / len(win_rates))**0.5
                            if wr_std < 0.8: # 全員の実力が近い
                                m_score += 30
                                m_reasons.append(f"実力伯仲(混戦度:{round(wr_std, 2)})")

                        # 3. 外枠の逆転要素
                        for b in s1_boats[2:]: # 3-6
                            wr = b.get("win_rate", 0)
                            if wr >= 6.8: m_score += 25
                            elif wr >= 6.2: m_score += 15

                        # 4. 会場補正
                        v_meta = VENUES_METADATA.get(v_name, {})
                        v_manshu_score_raw = v_meta.get("manshu_score", 0)
                        v_bonus = int(v_manshu_score_raw * 0.35)
                        m_score += v_bonus
                        if v_bonus > 0: m_reasons.append(f"会場特性({v_name})")
                        elif v_bonus < 0: m_reasons.append(f"イン堅調場({v_name})")

                        # 5. 特殊条件 (F, モーター, 風)
                        f_count = detail["boats"][0].get("f_count", "-")
                        if cand.get("boat1_f") or (f_count != "-" and re.search(r'F[1-9]', f_count)):
                            m_score += 30
                            m_reasons.append(f"1号艇F({f_count if f_count != '-' else 'F1+'})")
                        
                        m_2ren = detail["boats"][0].get("motor_2ren", 0)
                        if 0 < m_2ren < 28.0:
                            m_score += 25
                            m_reasons.append(f"1号艇モーター不安({m_2ren}%)")

                        w_spd = detail["env"].get("wind_spd", 0)
                        if w_spd >= 5.0:
                            m_score += 25
                            m_reasons.append(f"強風({w_spd}m)")
                            
                        if detail["env"].get("anteiban", False):
                            m_score -= 15
                            m_reasons.append("安定板使用(まくり抑制)")
                            
                        # 6. オリジナル展示・直前情報による波乱要素 (展示タイム異常など)
                        ex_times = []
                        for b in detail["boats"]:
                            t_val = clean_float(b.get("ex_time", 9.99))
                            if t_val < 9.0: ex_times.append(t_val)
                            
                        if ex_times:
                            min_ex = min(ex_times)
                            # 外枠(4,5,6号艇)が一番時計の場合
                            for idx, b in enumerate(detail["boats"][3:], 3):
                                if clean_float(b.get("ex_time", 9.99)) == min_ex:
                                    m_score += 35
                                    m_reasons.append(f"外枠展示トップ({idx+1}号艇 {min_ex}秒)")
                                    break
                                    
                            # 1号艇の展示タイム劣勢
                            b1_ex = clean_float(detail["boats"][0].get("ex_time", 9.99))
                            if b1_ex > min_ex + 0.08:
                                m_score += 30
                                m_reasons.append(f"1号艇展示劣勢(差+{round(b1_ex - min_ex, 2)}秒)")

                        # チルト跳ね上げ選手
                        for idx, b in enumerate(detail["boats"]):
                            tilt = clean_float(b.get("tilt", 0.0))
                            if tilt >= 0.5:
                                m_score += 25
                                m_reasons.append(f"{idx+1}号艇チルト跳ね({tilt})")
                                break

                        final_results.append({
                            "締切予定": deadline_str,
                            "会場・レース": f"{v_name} {r_no}R",
                            "万舟スコア": m_score,
                            "波乱予測の理由": " / ".join(m_reasons)
                        })
                    except Exception as e:
                        status.write(f"⚠️ {v_name} {r_no}R の詳細解析に失敗しました。")
                    
                    progress_bar.progress((i + 1) / len(target_candidates))
                
                status.update(label="✅ 全候補の展示・直前情報解析が完了しました！", state="complete", expanded=False)
                
                if final_results:
                    final_results.sort(key=lambda x: x["万舟スコア"], reverse=True)
                    
                    filtered_results = []
                    venue_counts = {}
                    for res in final_results:
                        v_name = res["会場・レース"].split(" ")[0]
                        count = venue_counts.get(v_name, 0)
                        if count < 3:
                            filtered_results.append(res)
                            venue_counts[v_name] = count + 1
                        
                        if len(filtered_results) >= 15:
                            break
                            
                    st.dataframe(pd.DataFrame(filtered_results), use_container_width=True, hide_index=True)
                    st.success("🔥 スコアが高いほど、直前展示から判断した万舟券（100倍以上）の発生確率が高まっています！")
                else:
                    st.warning("詳細展示解析の結果、推奨できるレースがありませんでした。")

        if st.button("レーダー表示を閉じる"):
            st.session_state.run_rough_search = False
            st.rerun()

    if st.session_state.get("run_weather_search", False):
        st.markdown("---")
        st.header("🌪️ 全場荒天・高波レーダー 解析結果")
        
        with st.status("📡 全開催場の直近レースから水面気象情報を収集中...", expanded=True) as status:
            import rough_race_finder
            import importlib
            importlib.reload(rough_race_finder)
            try: loop = asyncio.get_event_loop()
            except RuntimeError: loop = asyncio.new_event_loop(); asyncio.set_event_loop(loop)
            
            t_date = st.session_state.get("target_search_date")
            if loop.is_running():
                import nest_asyncio; nest_asyncio.apply()
                future = asyncio.ensure_future(rough_race_finder.find_rough_weather_venues(t_date))
                weather_data = loop.run_until_complete(future)
            else:
                weather_data = loop.run_until_complete(rough_race_finder.find_rough_weather_venues(t_date))
                
            if not weather_data:
                status.update(label="❌ 気象情報の取得に失敗しました。", state="error")
            else:
                status.update(label="✅ 全会場の気象データ取得完了！", state="complete")
                
        if weather_data:
            alert_venues = [w for w in weather_data if w["wind_raw"] >= 5.0 or w["wave_raw"] >= 5.0]
            if alert_venues:
                st.markdown("""
                <div style="background-color: #ffeaea; padding: 15px; border-radius: 10px; border-left: 8px solid #ff4b4b; margin-bottom: 20px;">
                    <span style="font-size: 18px; font-weight: bold; color: #ff4b4b;">⚠️ 荒天アラート発令中！強風・高波による万舟注意報</span><br>
                    <span style="font-size: 14px; color: #333;">以下の会場は風速5m以上、または波高5cm以上の荒水面コンディションです。波乱展開の期待値が大幅に上昇しています。</span>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown("""
                <div style="background-color: #eaf6ff; padding: 15px; border-radius: 10px; border-left: 8px solid #005ce6; margin-bottom: 20px;">
                    <span style="font-size: 18px; font-weight: bold; color: #005ce6;">⛅ 全会場 穏やかコンディション</span><br>
                    <span style="font-size: 14px; color: #333;">現在、風速・波高ともに基準値（5m/5cm）を超える荒天会場はありません。水面状況の比較としてご覧ください。</span>
                </div>
                """, unsafe_allow_html=True)
                
            display_df = []
            for w in weather_data:
                d = w.copy()
                del d["wind_raw"]
                del d["wave_raw"]
                display_df.append(d)
                
            st.dataframe(pd.DataFrame(display_df), use_container_width=True, hide_index=True)
            
        if st.button("気象レーダー表示を閉じる"):
            st.session_state.run_weather_search = False
            st.rerun()
    
    # --- 収益計算ダッシュボード (Top Section) ---
    st.markdown("<div class='metric-box' style='border-left: 8px solid #00d4ff;'>", unsafe_allow_html=True)
    st.markdown("#### 💰 収支管理ダッシュボード")
    
    current_user = st.radio("👤 利用者を選択", ["T", "K", "H"], horizontal=True)
    
    # 次戦投資額の入力
    next_invest_val = st.number_input("次戦の予定投資額 (円)", min_value=100, step=100, value=1000, help="この金額を元に「捲るための必要オッズ」を計算します")
    
    current_history = st.session_state.all_histories.get(current_user, [])
    stats = calculate_profit_stats(current_history, next_invest_val)
    
    col_a, col_b, col_c, col_d = st.columns([1, 1, 1.5, 2])
    col_a.metric("回収率", f"{stats['recovery_rate']:.1f}%")
    col_b.metric("的中率", f"{stats['hit_rate']:.1f}%")
    profit_color = "normal" if stats['net_profit'] >= 0 else "inverse"
    col_c.metric("合計損益", f"{stats['net_profit']:,}円", delta=stats['net_profit'], delta_color=profit_color)
    
    if stats['net_profit'] < 0:
        col_d.warning(f"🎯 **次戦必要オッズ**: **{stats['required_odds']:.2f}倍** 以上")
    else:
        col_d.success(f"📈 利益継続中！")
        
    with st.expander(f"📝 {current_user} のレース結果を記録 / 履歴管理"):
        with st.form("top_profit_form"):
            c1, c2 = st.columns(2)
            f_invest = c1.number_input("投資金額 (円)", min_value=0, step=100, value=1000)
            f_payout = c2.number_input("的中金額 (円)", min_value=0, step=10, value=0)
            if st.form_submit_button("収支を記録"):
                st.session_state.all_histories[current_user].append({
                    "date": str(datetime.now().strftime("%Y/%m/%d %H:%M")),
                    "invest": f_invest,
                    "payout": f_payout
                })
                save_history(st.session_state.all_histories)
                st.rerun()

        if current_history:
            # 最新5件を表示
            st.dataframe(pd.DataFrame(current_history).tail(5), use_container_width=True, hide_index=True)
            if st.button("履歴をすべてリセット"):
                st.session_state.all_histories[current_user] = []
                save_history(st.session_state.all_histories)
                st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)
    
    st.markdown("<div class='metric-box'>", unsafe_allow_html=True)
    col1, col2, col3, col4 = st.columns([2, 1, 1, 2])
    target_date = col1.date_input("日付")
    venue_name = col2.selectbox("会場", list(VENUES.keys()))
    race_no = col3.selectbox("レース番号", list(range(1, 13)))

    # 荒れる度に応じてモードのデフォルトを自動設定
    _pre_roughness = VENUE_ROUGHNESS_MAP.get(venue_name, 15.0)
    if _pre_roughness >= 18.5:
        _default_mode_idx = 1  # 万舟的中
        _mode_hint = "🔴 万舟推奨"
    elif _pre_roughness >= 16.0:
        _default_mode_idx = 2  # 中穴・大穴的中
        _mode_hint = "🟡 中穴推奨"
    else:
        _default_mode_idx = 0  # 通常
        _mode_hint = ""

    _mode_label = f"🤖 モード {'  ' + _mode_hint if _mode_hint else ''}"
    prediction_mode = col4.selectbox(
        _mode_label,
        ["通常", "万舟的中", "中穴・大穴的中"],
        index=_default_mode_idx
    )
    
    # Detailed Settings for Mobile
    is_manshu_mode = prediction_mode in ["万舟的中", "中穴・大穴的中"]
    with st.expander("⚙️ 詳細な予測設定（万舟モード・オッズ閾値など）", expanded=is_manshu_mode):
        st.markdown("<div style='font-size: 14px; color: #666; margin-bottom: 10px;'>※モバイル端末でも設定しやすいようにこちらに配置しました。</div>", unsafe_allow_html=True)
        
        # デフォルト値
        manshu_points = 20
        special_exclude_1_head = False
        special_odds_threshold = 40.0
        
        col_s1, col_s2 = st.columns(2)
        with col_s1:
            if prediction_mode == "万舟的中":
                manshu_points = st.selectbox("万舟的中モード：推奨点数", [20, 30, 40], index=0)
                st.markdown("<div style='margin-bottom: 5px;'></div>", unsafe_allow_html=True)
                special_exclude_1_head = st.radio("万舟的中モード：1号艇1着", ["1頭入り", "1頭切り"], horizontal=True) == "1頭切り"
            elif prediction_mode == "中穴・大穴的中":
                manshu_points = st.selectbox("中穴・大穴的中モード：推奨点数", [10, 20, 30, 40], index=0)
                st.markdown("<div style='margin-bottom: 5px;'></div>", unsafe_allow_html=True)
                special_exclude_1_head = st.radio("中穴・大穴的中モード：1号艇1着", ["1頭入り", "1頭切り"], horizontal=True) == "1頭切り"
            else:
                st.write("通常モードではこの設定は使用されません")
                
        with col_s2:
            if prediction_mode == "中穴・大穴的中":
                special_odds_threshold = st.radio("中穴・大穴的中モード：オッズ閾値", [20.0, 30.0, 40.0, 50.0], index=2, format_func=lambda x: f"{int(x)}倍以上", horizontal=True)
            debug_mode = st.checkbox("デバッグモード", value=False)
    
    # 荒れる度の表示
    roughness = VENUE_ROUGHNESS_MAP.get(venue_name, 15.0)
    roughness_color = "#ff4b4b" if roughness >= 17.5 else "#ffa500" if roughness >= 15.0 else "#005ce6"
    
    # --- データ取得前の事前統計予測パネル ---
    # --- データ取得前の事前統計予測パネル ---
    if roughness >= 18.5:
        rec_mode = "万舟的中"
        rec_payout = "10,000円〜 (オッズ100倍以上・大穴万舟帯)"
        rec_color = "#ff4b4b"
        if roughness >= 20.0:
            rec_exclude = "「1頭切り（1号艇1着除外）」推奨 ✂️"
            strat_desc = f"過去1年の統計から荒れる度が{roughness}%と極めて高い超難水面です。イン崩れの波乱が多発するため「1頭切り」で高配当を広範囲に狙ってください。"
        else:
            rec_exclude = "「1頭入り（1号艇1着含む）」推奨 ⛵"
            strat_desc = f"荒れる度が{roughness}%と高い波乱水面ですが、1号艇逃げからのヒモ荒れ万舟も頻出します。「1頭入り」で1号艇アタマ万舟もカバーしてください。"
        rec_reason = f"過去統計からイン逃げ率が低く、万舟特化での高配当狙いが推奨される会場です。"
        rec_points = "30点 〜 40点"
        rec_odds = "100倍 以上 (万舟狙い)"
        rec_strategy = strat_desc
    elif 16.0 <= roughness < 18.5:
        rec_mode = "中穴・大穴的中"
        rec_payout = "3,000円〜9,990円 (オッズ30倍〜99倍・中穴帯)"
        rec_color = "#ffa500"
        if roughness >= 17.0:
            rec_exclude = "「1頭切り（1号艇1着除外）」推奨 ✂️"
            strat_desc = f"荒れる度が{roughness}%とやや高めです。センター勢のまくり展開を想定し「1頭切り」でオッズ30〜50倍以上を効率よく狙ってください。"
        else:
            rec_exclude = "「1頭入り（1号艇1着含む）」推奨 ⛵"
            strat_desc = f"イン逃げ率が一定ある会場です。「1頭入り」を選択し、1号艇頭からのヒモ荒れ中穴配当もしっかり押さえてください。"
        rec_reason = f"過去統計から中穴傾向のある会場です。妙味ある配当を狙うのが最も回収期待値が高まります。"
        rec_points = "20点 〜 30点"
        rec_odds = "30倍 〜 50倍 以上"
        rec_strategy = strat_desc
    else:
        rec_mode = "通常"
        rec_color = "#005ce6"
        rec_exclude = "設定不要（通常モード）"
        if roughness < 14.0:
            rec_payout = "500円〜1,500円 (オッズ5倍〜15倍・超堅守帯)"
            rec_reason = f"過去統計から荒れる度が{roughness}%とイン逃げ率が非常に高い鉄板会場（大村・芦屋など）です。点数を極限まで絞り込む必要があります。"
            rec_points = "4点 〜 6点"
            rec_odds = "5倍 〜 15倍 (本命鉄板)"
            rec_strategy = "イン逃げ超鉄板展開。1号艇頭からの上位4〜6点に資金を集中させ、トリガミを避けて確実に利益を出してください。"
        else:
            rec_payout = "1,000円〜2,990円 (オッズ10倍〜29倍・本命〜中穴手前)"
            rec_reason = f"過去統計から荒れる度が{roughness}%とイン逃げ率が安定している会場です。堅実な本命〜絞り込み買い目で手堅く的中を重ねるのが推奨されます。"
            rec_points = "6点 〜 10点"
            rec_odds = "10倍 〜 25倍 (本命〜絞り込み)"
            rec_strategy = "イン逃げ優勢展開。1号艇頭からの本命買い目に資金を集中させて着実に回収してください。"
        
    diag_title = "🤖 AI配当予測 ＆ おすすめモード診断 (事前統計予測)"
        
    st.markdown(f"""
    <div style="background-color: #f8f9fa; padding: 10px; border-radius: 8px; border-left: 5px solid {roughness_color}; margin-top: 10px; margin-bottom: 15px;">
        <span style="font-size: 14px; color: #666;">過去1年間の統計</span><br>
        <span style="font-size: 18px; font-weight: bold; color: {roughness_color};">📊 荒れる度: {roughness}%</span>
    </div>
    <div style="background-color: #fff9e6; padding: 15px; border-radius: 10px; border-left: 8px solid {rec_color}; margin-bottom: 20px; box-shadow: 0 2px 5px rgba(0,0,0,0.05);">
        <span style="font-size: 14px; color: {rec_color}; font-weight: bold;">{diag_title}</span><br>
        <span style="font-size: 18px; font-weight: bold; color: #333;">👑 おすすめモード：【 {rec_mode} 】</span> <span style="font-size: 15px; color: #555;">(予想配当: {rec_payout})</span><br>
        <div style="margin-top: 5px; font-size: 14px; color: #444; line-height: 1.4;">
            💡 <b>診断理由</b>: {rec_reason}
        </div>
        <div style="margin-top: 10px; padding-top: 10px; border-top: 1px dashed #ddccaa; font-size: 14px; color: #333;">
            🎯 <b>推奨点数</b>: <span style="color: #e60000; font-weight: bold;">{rec_points}</span> / <b>推奨オッズ閾値</b>: <span style="color: #e60000; font-weight: bold;">{rec_odds}</span><br>
            ⛵ <b>1号艇1着の扱い</b>: <span style="color: #005ce6; font-weight: bold;">{rec_exclude}</span><br>
            🏷️ <b>設定アドバイス</b>: {rec_strategy}
        </div>
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
            actual_bet_points = manshu_points if prediction_mode in ["万舟的中", "中穴・大穴的中"] else bet_points
            res_analysis = analyze_kimarite_and_bets(oracle_results, data, venue_name, actual_bet_points, prediction_mode=prediction_mode, special_odds_threshold=special_odds_threshold, special_exclude_1_head=special_exclude_1_head)
            
            st.session_state.result = {
                "data": data, "oracle": oracle_results, "analysis": res_analysis, "prediction_mode": prediction_mode
            }

    if "result" in st.session_state:
        res = st.session_state.result
        data = res["data"]
        oracle_results = res["oracle"]
        
        # ユーザーがUI上でモードや設定を変更した際、即座に最新設定で再解析して反映する
        actual_bet_points = manshu_points if prediction_mode in ["万舟的中", "中穴・大穴的中"] else bet_points
        ana = analyze_kimarite_and_bets(oracle_results, data, venue_name, actual_bet_points, prediction_mode=prediction_mode, special_odds_threshold=special_odds_threshold, special_exclude_1_head=special_exclude_1_head)
        res["analysis"] = ana
        res["prediction_mode"] = prediction_mode
        
        if debug_mode and "raw_text" in data:
            with st.expander("🔍 取得データ（生テキスト）"):
                st.text(data["raw_text"])
        
        env = data["env"]
        
        st.markdown("---")
        
        # 動的荒れる度の算出（AI予想と市場オッズの乖離評価を含む）
        dyn_roughness = calculate_dynamic_roughness(data, venue_name, oracle_results=oracle_results)
        roughness_color = "#ff4b4b" if dyn_roughness >= 45.0 else "#ffa500" if dyn_roughness >= 30.0 else "#005ce6"
        
        # 実際の全通りオッズデータを加味したAI配当予測とおすすめモード判定
        odds_dict = data.get("odds", {})
        min_all_odds = min([ov for ov in odds_dict.values() if ov > 1.0] or [10.0])
        manshu_count = sum(1 for ov in odds_dict.values() if ov >= 100.0)
        ana_count = sum(1 for ov in odds_dict.values() if 30.0 <= ov < 100.0)
        
        p1_main_list = list(oracle_results["p1"])
        ai_fav_idx = p1_main_list.index(max(p1_main_list))
        ai_fav_win = oracle_results["p1"][ai_fav_idx]
        ai_fav_boat = str(ai_fav_idx + 1)
        market_fav_boat = [k for k, v in odds_dict.items() if v == min_all_odds][0].split("-")[0] if odds_dict else "1"
        
        # 直前荒れる度、AI勝率、全通り最低オッズのバランスを総合評価
        market_fav_ai_win = p1_main_list[int(market_fav_boat)-1]
        
        # 最低オッズが7.5倍未満で、かつAI予想でも十分に高い勝率（40%以上）を誇る場合は「本命〜中穴手前の通常モード決着」とみなす
        if min_all_odds < 7.5 and market_fav_ai_win >= 0.40 and dyn_roughness < 30.0:
            rec_mode = "通常"
            rec_color = "#005ce6"
            rec_exclude = "設定不要（通常モード）"
            if market_fav_ai_win >= 0.65 or (min_all_odds < 4.0 and dyn_roughness < 30.0):
                rec_payout = "500円〜1,500円 (オッズ5倍〜15倍・超堅守帯)"
                rec_reason = f"市場最低オッズが{min_all_odds}倍（{market_fav_boat}号艇頭）と人気が集中しており、AI予想（勝率{market_fav_ai_win*100:.1f}%）とも強力に合致している鉄板レースです。波乱要素は極めて低いため少数点での本命決着狙いが必須です。"
                rec_points = "4点 〜 6点"
                rec_odds = "5倍 〜 15倍 (本命鉄板)"
                rec_strategy = f"本命超鉄板展開。{market_fav_boat}号艇頭からの上位4〜6点に資金を集中させ、トリガミを避けて確実に利益を出してください。"
            else:
                rec_payout = "1,000円〜2,990円 (オッズ10倍〜29倍・本命〜中穴手前)"
                rec_reason = f"市場最低オッズが{min_all_odds}倍（{market_fav_boat}号艇頭）と支持が明確で、AI予想（勝率{market_fav_ai_win*100:.1f}%）とも強力に合致しています。無理な穴狙いは避け、本命〜中穴手前を確実に仕留めるのがベストです。"
                rec_points = "6点 〜 10点"
                rec_odds = "10倍 〜 25倍 (本命〜絞り込み)"
                rec_strategy = f"本命優勢展開。{market_fav_boat}号艇頭から相手を絞り込み、確実な的中と回収を両立させてください。"
        elif dyn_roughness >= 55.0 or (ai_fav_win < 0.40 and min_all_odds >= 12.0):
            # 真の万舟レース（波乱要素・オッズともに大混戦濃厚）
            rec_mode = "万舟的中"
            rec_payout = "10,000円〜 (オッズ100倍以上・大穴万舟帯)"
            rec_color = "#ff4b4b"
            if ai_fav_win < 0.35 or dyn_roughness >= 60.0 or min_all_odds >= 14.0:
                rec_exclude = "「1頭切り（1号艇1着除外）」推奨 ✂️"
                strat_desc = f"直前荒れる度が{dyn_roughness}%と極めて高く、大波乱要素が満載です。「1頭切り」を選択し、外枠勢同士の特大万舟を広範囲に網羅してください。"
            else:
                rec_exclude = "「1頭入り（1号艇1着含む）」推奨 ⛵"
                strat_desc = f"波乱要素が高い({dyn_roughness}%)ものの、1号艇が残る展開でも相手次第で万舟が発生します。「1頭入り」を選択し、1頭万舟も含めてカバーしてください。"
            rec_reason = f"直前荒れる度が{dyn_roughness}%に達しており、最低オッズ{min_all_odds}倍と支持も割れています。オッズ100倍以上の組み合わせが{manshu_count}通り存在し、万舟決着の期待値が極めて高いレースです。"
            rec_points = "30点 〜 40点"
            rec_odds = "100倍 以上 (万舟狙い)"
            rec_strategy = strat_desc
        elif dyn_roughness >= 40.0 or (5.0 <= min_all_odds < 10.0) or (0.40 <= ai_fav_win < 0.55):
            # 中穴・大穴狙いレース（オッズ妙味・ヒモ荒れ濃厚）
            rec_mode = "中穴・大穴的中"
            rec_payout = "3,000円〜9,990円 (オッズ30倍〜99倍・中穴帯)"
            rec_color = "#ffa500"
            if p1_main_list[0] < 0.45 or dyn_roughness >= 48.0 or min_all_odds >= 8.0:
                rec_exclude = "「1頭切り（1号艇1着除外）」推奨 ✂️"
                strat_desc = f"波乱度がやや高め({dyn_roughness}%)です。「1頭切り」を選択し、センター・外枠勢の頭からオッズ30〜50倍以上の中穴を効率よく狙ってください。"
            else:
                rec_exclude = "「1頭入り（1号艇1着含む）」推奨 ⛵"
                strat_desc = f"1号艇の勝率がまずまず維持({p1_main_list[0]*100:.1f}%)されています。「1頭入り」を選択し、1号艇頭からのヒモ荒れ中穴配当もしっかり押さえてください。"
            rec_reason = f"直前荒れる度が{dyn_roughness}%あり、最低オッズ{min_all_odds}倍（{market_fav_boat}号艇頭）と中穴帯のオッズ妙味が非常に高いレースです。無理な大穴は避けつつ、中穴帯を狙い撃つのがベストです。"
            rec_points = "20点 〜 30点"
            rec_odds = "30倍 〜 50倍 以上"
            rec_strategy = strat_desc
        else:
            # 本命・堅守レース（波乱要素が少なくAIと市場オッズが一致）
            rec_mode = "通常"
            rec_color = "#005ce6"
            rec_exclude = "設定不要（通常モード）"
            rec_payout = "1,000円〜2,990円 (オッズ10倍〜29倍・本命〜中穴手前)"
            rec_reason = f"直前荒れる度が{dyn_roughness}%と安定しており、AI推し({ai_fav_boat}号艇)と市場オッズ推し({market_fav_boat}号艇)が合致（最低オッズ{min_all_odds}倍）しています。無理な穴狙いは避け、本命〜中穴手前を確実に仕留めるのがベストです。"
            rec_points = "6点 〜 10点"
            rec_odds = "10倍 〜 25倍 (本命〜絞り込み)"
            rec_strategy = f"本命優勢展開。{ai_fav_boat}号艇頭から相手を絞り込み、確実な的中と回収を両立させてください。"
            
        diag_title = "🤖 AI配当予測 ＆ おすすめモード診断 (直前データ・オッズ解析済)"
        
        st.markdown(f"""
        <div style="background-color: #f8f9fa; padding: 15px; border-radius: 10px; border-left: 8px solid {roughness_color}; margin-bottom: 15px;">
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
        <div style="background-color: #fff9e6; padding: 15px; border-radius: 10px; border-left: 8px solid {rec_color}; margin-bottom: 20px; box-shadow: 0 2px 5px rgba(0,0,0,0.05);">
            <span style="font-size: 14px; color: {rec_color}; font-weight: bold;">{diag_title}</span><br>
            <span style="font-size: 18px; font-weight: bold; color: #333;">👑 おすすめモード：【 {rec_mode} 】</span> <span style="font-size: 15px; color: #555;">(予想配当: {rec_payout})</span><br>
            <div style="margin-top: 5px; font-size: 14px; color: #444; line-height: 1.4;">
                💡 <b>診断理由</b>: {rec_reason}
            </div>
            <div style="margin-top: 10px; padding-top: 10px; border-top: 1px dashed #ddccaa; font-size: 14px; color: #333;">
                🎯 <b>推奨点数</b>: <span style="color: #e60000; font-weight: bold;">{rec_points}</span> / <b>推奨オッズ閾値</b>: <span style="color: #e60000; font-weight: bold;">{rec_odds}</span><br>
                ⛵ <b>1号艇1着の扱い</b>: <span style="color: #005ce6; font-weight: bold;">{rec_exclude}</span><br>
                🏷️ <b>設定アドバイス</b>: {rec_strategy}
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

            if pred_mode in ["万舟的中", "中穴・大穴的中"]:
                st.markdown(f"<div style='background-color: #2b1d1d; color: #ff4b4b; padding: 10px; border-radius: 5px; margin-bottom: 15px; border: 1px solid #ff4b4b;'>🔥 {pred_mode}モード発動中：上位 **{len(ana['bets'])}** 点を表示</div>", unsafe_allow_html=True)
            
            # Display as cards
            for i in range(0, len(ana["bets"]), 2):
                cols = st.columns(2)
                for j in range(2):
                    if i + j < len(ana["bets"]):
                        bet = ana["bets"][i + j]
                        with cols[j]:
                            odds_val = data["odds"].get(bet["bet"], "取得中..")
                            fair_odds = bet.get("fair_odds", 9999.9)
                            
                            is_delicious = False
                            if isinstance(odds_val, (float, int)):
                                is_delicious = odds_val > fair_odds
                                odds_text = f"{odds_val}倍 (適正: {fair_odds}倍)"
                            else:
                                odds_text = f"取得中.. (適正: {fair_odds}倍)"
                                
                            if is_delicious:
                                odds_html = f"<div style='color: #ff4b4b; font-size: 18px; font-weight: bold;'>🔥 {odds_text}</div>"
                            else:
                                odds_html = f"<div style='color: #333; font-size: 16px; font-weight: bold;'>{odds_text}</div>"
                            
                            st.markdown(f"""
                            <div style="background-color: white; border: 2px solid #333; border-radius: 15px; padding: 15px; margin-bottom: 10px; box-shadow: 2px 2px 5px rgba(0,0,0,0.1);">
                                <div style="display: flex; justify-content: space-between; align-items: center;">
                                    <div style="font-size: 20px; font-weight: bold;">
                                        { ' '.join([f'<span style="background-color: {"#f8f9fa" if c=="1" else "#333" if c=="2" else "#ff4b4b" if c=="3" else "#005ce6" if c=="4" else "#ffa500" if c=="5" else "#28a745"}; color: {"#333" if c=="1" else "white"}; border-radius: 50%; width: 33px; height: 33px; display: inline-flex; align-items: center; justify-content: center; margin-right: 5px; border: 1px solid #ccc;">{c}</span>' for c in bet["bet"].split("-")]) }
                                    </div>
                                    {odds_html}
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
                fair_odds_v = item.get("fair_odds", 9999.9)
                score_v = item["score"]
                
                is_umami = False
                if isinstance(odds_v, (float, int)) and odds_v > fair_odds_v:
                    is_umami = True
                    
                odds_display = f"{odds_v} (適正: {fair_odds_v})"
                if is_umami:
                    odds_display = f"🔥 {odds_display}"
                    
                all_df_data.append({
                    "順位": len(all_df_data) + 1,
                    "買い目": item["bet"],
                    "オッズ": odds_display,
                    "期待スコア": round(score_v * 1000, 2), # Scale for readability
                    "解析根拠": item["reason"]
                })
            
            st.dataframe(pd.DataFrame(all_df_data), use_container_width=True, hide_index=True)

        # AIフィードバック学習エンジンUI
        st.markdown("<br>", unsafe_allow_html=True)
        with st.expander("🎓 AIフィードバック学習エンジン（レース結果の入力と自動学習）", expanded=False):
            st.markdown("""
            <div style='background-color: #f8f9fa; padding: 10px; border-radius: 5px; margin-bottom: 15px;'>
                実際のレース結果を入力することでAIがデータベースに記録し、次回以降の同会場・同気象条件での予測スコアを自動調整（自己学習）します。
            </div>
            """, unsafe_allow_html=True)
            
            c_res1, c_res2 = st.columns(2)
            all_combos_3tan = [f"{a}-{b}-{c}" for a, b, c in itertools.permutations(["1","2","3","4","5","6"], 3)]
            actual_3tan = c_res1.selectbox("実際の3連単結果を選択", all_combos_3tan, index=0)
            actual_payout = c_res2.number_input("払戻金 (円)", min_value=100, max_value=1000000, value=1500, step=100)
            
            if st.button("💾 結果を保存してAIに学習させる", use_container_width=True):
                ldb = load_learning_db()
                ldb.append({
                    "date": str(target_date),
                    "venue": venue_name,
                    "race_no": race_no,
                    "actual_result": actual_3tan,
                    "payout": actual_payout,
                    "predicted_p1": [round(p*100, 1) for p in oracle_results["p1"]],
                    "timestamp": datetime.now().isoformat()
                })
                save_learning_db(ldb)
                st.success(f"✅ 【{venue_name} {race_no}R】の結果（{actual_3tan} / {actual_payout}円）を学習データベースに登録しました！次回以降のスコア計算に自動反映されます。")

if __name__ == "__main__":
    main()
