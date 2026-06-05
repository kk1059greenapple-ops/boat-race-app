import asyncio
import aiohttp
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import time
import math
import logging
import random

# ロギングの設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 1. 定数とマスターデータ ---

VENUE_NAMES = {
    "01": "桐生", "02": "戸田", "03": "江戸川", "04": "平和島", "05": "多摩川",
    "06": "浜名湖", "07": "蒲郡", "08": "常滑", "09": "津", "10": "三国",
    "11": "びわこ", "12": "住之江", "13": "尼崎", "14": "鳴門", "15": "丸亀",
    "16": "児島", "17": "宮島", "18": "徳山", "19": "下関", "20": "若松",
    "21": "芦屋", "22": "福岡", "23": "唐津", "24": "大村"
}

# 各競艇場の水面レイアウト（1マークから2マークへ向かうホームストレッチの方角を度数で定義）
# ※この角度は例です。正確な相対風向を出すには、Googleマップ等で各場のスタンドと水面の角度を正確に測る必要があります。
# 北=0度, 東=90度, 南=180度, 西=270度
VENUE_ANGLES = {
    "01": 315, # 桐生 (例: 北西向き)
    "02": 90,  # 戸田 (例: 東向き)
    "03": 180, # 江戸川 (例: 南向き)
    # ... 他の21場も同様に正確な角度を定義する
}
# ※デモ用にデフォルト値を設定
from collections import defaultdict
venue_angles_safe = defaultdict(lambda: 0, VENUE_ANGLES)

# 16方位を角度（度数法）に変換するマッピング
WIND_DIR_TO_ANGLE = {
    "北": 0, "北北東": 22.5, "北東": 45, "東北東": 67.5,
    "東": 90, "東南東": 112.5, "南東": 135, "南南東": 157.5,
    "南": 180, "南南西": 202.5, "南西": 225, "西南西": 247.5,
    "西": 270, "西北西": 292.5, "北西": 315, "北北西": 337.5,
    "無風": None
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


# --- 2. 風向・風速の変換ロジック ---

def get_relative_wind_direction(venue_cd, wind_dir_text):
    """
    公式の16方位の風向を、各場のホームストレッチに対する相対的な風向に変換する
    """
    if wind_dir_text == "無風" or wind_dir_text not in WIND_DIR_TO_ANGLE:
        return "無風"
    
    wind_angle = WIND_DIR_TO_ANGLE[wind_dir_text]
    home_stretch_angle = venue_angles_safe[venue_cd]
    
    # 風向と水面の角度の差を計算（-180 〜 180度に正規化）
    diff = (wind_angle - home_stretch_angle + 180) % 360 - 180
    
    # 差分から相対風向を判定
    # 0度付近: 向かい風 (1マークから吹いてくる)
    # 180度/-180度付近: 追い風 (2マークから吹いてくる)
    # 90度付近: 右横風 (スタンド側などから吹く)
    # -90度付近: 左横風
    
    if -45 <= diff <= 45:
        return "向かい風"
    elif diff > 45 and diff < 135:
        return "右横風"
    elif diff < -45 and diff > -135:
        return "左横風"
    else:
        return "追い風"

def categorize_wind_speed(speed):
    """風速をカテゴリ分けする"""
    if speed <= 1.0:
        return "無風(0-1m)"
    elif 2.0 <= speed <= 4.0:
        return "弱風(2-4m)"
    else:
        return "強風(5m以上)"


# --- 3. データ取得ロジック（スクレイピング） ---

async def fetch_html(session, url, semaphore):
    """サーバー負荷を抑えつつHTMLを取得する"""
    async with semaphore:
        # ベストプラクティス: 連続アクセスを避けるためのランダムなSleep
        await asyncio.sleep(random.uniform(1.0, 3.0)) 
        
        for attempt in range(3):
            try:
                async with session.get(url, headers=HEADERS, timeout=15) as response:
                    if response.status == 200:
                        return await response.text()
                    elif response.status == 404:
                        return None # レース中止等
                    elif response.status == 403:
                        logging.warning(f"403 Forbidden: {url}. Waiting longer...")
                        await asyncio.sleep(10) # ブロックされたら長めに待つ
            except Exception as e:
                pass
            await asyncio.sleep(2)
        return None

async def scrape_race_data(session, date_str, venue_cd, rno, semaphore):
    """1つのレースの結果と直前情報（風況）を取得する"""
    # 結果ページ
    result_url = f"https://www.boatrace.jp/owpc/pc/race/raceresult?rno={rno}&jcd={venue_cd}&hd={date_str}"
    # 直前情報ページ（風データ）
    before_url = f"https://www.boatrace.jp/owpc/pc/race/beforeinfo?rno={rno}&jcd={venue_cd}&hd={date_str}"
    
    result_html, before_html = await asyncio.gather(
        fetch_html(session, result_url, semaphore),
        fetch_html(session, before_url, semaphore)
    )
    
    if not result_html or not before_html:
        return None
        
    # --- 勝ったコース（進入コース）の取得 ---
    soup_res = BeautifulSoup(result_html, 'html.parser')
    # 決まり手や結果テーブルから1着の艇の情報を探す
    # (※注: 公式サイトの結果表構造に合わせて抽出。ここでは1着の「進入コース」を抽出する想定)
    # 実装例: <div class="numberSet1_row"> 等から1着艇の番号を取得し、進入コース情報と照らし合わせる
    winning_course = None
    try:
        # 簡易的な例: 結果テーブルの1行目（1着）の進入コース（枠番とは異なる点に注意）を取得
        # 本格的な実装では、払戻金テーブルや進入コース一覧から正確に紐付ける必要があります
        res_table = soup_res.select_one('table.is-w495 tbody')
        if res_table:
            first_row = res_table.select('tr')[0]
            # 艇番
            boat_no = int(first_row.select('td')[0].text.strip())
            # 進入コースが結果ページにない場合、事前情報等から類推するか、別の詳細ページを見る必要がありますが
            # 本スクリプトでは簡単のため「枠なり進入」と仮定してboat_no=courseとします。（実運用では修正必須）
            winning_course = boat_no 
    except Exception as e:
        return None
        
    if not winning_course:
        return None

    # --- 直前の風況の取得 ---
    soup_bef = BeautifulSoup(before_html, 'html.parser')
    wind_dir_raw = "無風"
    wind_spd = 0.0
    
    try:
        w_div = soup_bef.select_one('div.weather1')
        if w_div:
            # 風速
            wind_el = w_div.select_one('.is-wind .weather1_bodyUnitLabelData')
            if wind_el:
                wind_spd = float(wind_el.text.replace('m', '').strip())
            
            # 風向
            head_el = w_div.select_one('.is-windDirection p')
            if head_el and 'is-direction' in head_el.get('class', []):
                # クラス名から方位を特定（例: is-direction1 は北など）
                # ここではテキストから直接取る例を想定
                pass
            
            # 簡易的にテキストから抽出
            wind_p = w_div.select('.weather1_bodyUnitLabelTitle')
            for p in wind_p:
                if p.text == "風向":
                    # 次の要素が風向のテキスト
                    wind_dir_raw = p.find_next_sibling('p').text.strip()
                    break
    except Exception:
        pass

    # 相対風向への変換とカテゴリ化
    rel_wind = get_relative_wind_direction(venue_cd, wind_dir_raw)
    spd_cat = categorize_wind_speed(wind_spd)
    
    return {
        "Venue": VENUE_NAMES[venue_cd],
        "Course": winning_course, # 1着になったコース
        "Wind_Direction": rel_wind,
        "Wind_Speed": spd_cat,
    }


# --- 4. メイン処理と集計 ---

async def main():
    start_date = datetime.now() - timedelta(days=365) # 過去1年
    end_date = datetime.now() - timedelta(days=1)
    
    date_list = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]
    
    all_results = []
    semaphore = asyncio.Semaphore(5) # サーバー負荷を抑えるため同時接続数を絞る
    
    async with aiohttp.ClientSession() as session:
        # ※注意※ 1年分を回すと膨大な時間がかかるため、デモとして最初の1日のみ実行します。
        # 実際に1年分回す場合は date_list をすべてループさせてください。
        demo_dates = date_list[-3:] # 直近3日間に絞る
        
        for dt in demo_dates:
            date_str = dt.strftime("%Y%m%d")
            logging.info(f"Processing date: {date_str}")
            
            # その日の開催場を調べる (indexページ等から取得)
            # ここでは簡単のため、固定の場リストで回す想定
            active_venues = ["01", "02"] # テスト用
            
            tasks = []
            for venue_cd in active_venues:
                for rno in range(1, 13):
                    tasks.append(scrape_race_data(session, date_str, venue_cd, rno, semaphore))
            
            day_results = await asyncio.gather(*tasks)
            valid_results = [r for r in day_results if r is not None]
            all_results.extend(valid_results)
            
            # 1日ごとに少し休憩
            await asyncio.sleep(5)

    # --- 集計とCSV出力 ---
    if not all_results:
        logging.warning("No data collected.")
        return

    df = pd.DataFrame(all_results)
    
    # 1. 競艇場、風向、風速でグループ化し、各レースのカウントを取得（分母）
    race_counts = df.groupby(['Venue', 'Wind_Direction', 'Wind_Speed']).size().reset_index(name='Total_Races')
    
    # 2. 競艇場、風向、風速、コースでグループ化し、1着回数を取得（分子）
    win_counts = df.groupby(['Venue', 'Wind_Direction', 'Wind_Speed', 'Course']).size().unstack(fill_value=0)
    win_counts.columns = [f'Course_{c}_Wins' for c in win_counts.columns]
    win_counts = win_counts.reset_index()
    
    # 3. マージして勝率を計算
    master_df = pd.merge(race_counts, win_counts, on=['Venue', 'Wind_Direction', 'Wind_Speed'])
    
    for c in range(1, 7):
        col_wins = f'Course_{c}_Wins'
        col_rate = f'Course_{c}_WinRate'
        if col_wins in master_df.columns:
            # 勝率（パーセンテージ、小数第1位まで）
            master_df[col_rate] = (master_df[col_wins] / master_df['Total_Races'] * 100).round(1)
        else:
            master_df[col_rate] = 0.0
            
    # 必要なカラムだけを抽出して保存
    output_cols = ['Venue', 'Wind_Direction', 'Wind_Speed'] + [f'Course_{c}_WinRate' for c in range(1, 7)]
    final_df = master_df[output_cols]
    
    final_df.to_csv("wind_direction_master.csv", index=False, encoding='utf-8-sig')
    logging.info("Successfully generated wind_direction_master.csv")
    print(final_df.head())

if __name__ == "__main__":
    asyncio.run(main())
