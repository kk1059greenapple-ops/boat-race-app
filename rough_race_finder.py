import asyncio
import aiohttp
from bs4 import BeautifulSoup
import re
from datetime import datetime

# 開催場コードマッピング
VENUE_NAMES = {
    "01": "桐生", "02": "戸田", "03": "江戸川", "04": "平和島", "05": "多摩川", 
    "06": "浜名湖", "07": "蒲郡", "08": "常滑", "09": "津", "10": "三国", 
    "11": "びわこ", "12": "住之江", "13": "尼崎", "14": "鳴門", "15": "丸亀", 
    "16": "児島", "17": "宮島", "18": "徳山", "19": "下関", "20": "若松", 
    "21": "芦屋", "22": "福岡", "23": "唐津", "24": "大村"
}

HARD_VENUES = ["江戸川", "戸田", "平和島"]

async def fetch_html(session, url, semaphore):
    async with semaphore:
        try:
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    return await response.text()
                return None
        except Exception as e:
            return None

def parse_racelist(html, venue_name, rno):
    if not html:
        return None
        
    soup = BeautifulSoup(html, 'html.parser')
    rows = soup.select('tbody.is-fs12')
    
    if len(rows) < 6:
        return None
        
    boats = []
    for i, row in enumerate(rows):
        # 級別取得
        class_span = row.select('div.is-fs11 span')
        racer_class = "B1" # デフォルト
        for span in class_span:
            text = span.text.strip()
            if text in ["A1", "A2", "B1", "B2"]:
                racer_class = text
                break
                
        # 勝率・モーター取得
        tds = row.select('td.is-lineH2')
        local_win_rate = 0.0
        motor_2ren = 0.0
        national_win_rate = 0.0
        
        if len(tds) >= 4:
            try:
                nat_strings = list(tds[1].stripped_strings)
                if len(nat_strings) > 0:
                    national_win_rate = float(nat_strings[0])
                    
                local_strings = list(tds[2].stripped_strings)
                if len(local_strings) > 0:
                    local_win_rate = float(local_strings[0])
                    
                motor_strings = list(tds[3].stripped_strings)
                if len(motor_strings) > 1:
                    motor_2ren = float(motor_strings[1])
            except ValueError:
                pass
                
        boats.append({
            "number": i + 1,
            "class": racer_class,
            "local_win_rate": local_win_rate,
            "motor_2ren": motor_2ren,
            "national_win_rate": national_win_rate
        })
        
    if len(boats) != 6:
        return None
        
    # スコア計算
    score = 0
    reasons = []
    
    boat1 = boats[0]
    
    # パターンC: 難水面
    if venue_name in HARD_VENUES:
        score += 20
        reasons.append(f"難水面({venue_name})")
        
    # パターンA: 1号艇がB級で3-6号艇にA級
    if boat1["class"] in ["B1", "B2"]:
        has_a_class_outside = any(b["class"] in ["A1", "A2"] for b in boats[2:6]) # 3〜6号艇
        if has_a_class_outside:
            score += 50
            reasons.append(f"1号艇{boat1['class']}に対して外枠にA級あり")
        else:
            score += 10
            reasons.append(f"1号艇が{boat1['class']}級")
            
    # パターンB: 1号艇がA級だが、当地勝率かモーターが低い
    if boat1["class"] in ["A1", "A2"]:
        is_fake_favorite = False
        fake_reason = []
        if boat1["local_win_rate"] > 0 and boat1["local_win_rate"] < 5.0:
            is_fake_favorite = True
            fake_reason.append(f"当地勝率{boat1['local_win_rate']:.2f}")
        if boat1["motor_2ren"] > 0 and boat1["motor_2ren"] < 30.0:
            is_fake_favorite = True
            fake_reason.append(f"モーター{boat1['motor_2ren']:.1f}%")
            
        if is_fake_favorite:
            score += 30
            reasons.append(f"1号艇A級だが不安要素({', '.join(fake_reason)})")
            
    # モーターオバケの存在
    for b in boats[1:6]:
        if b["motor_2ren"] >= 45.0:
            score += 10
            reasons.append(f"{b['number']}号艇が超抜モーター({b['motor_2ren']:.1f}%)")
            
    # ランク判定
    if score >= 60:
        rank = "S"
    elif score >= 40:
        rank = "A"
    elif score >= 20:
        rank = "B"
    else:
        rank = "C"
        
    return {
        "venue": venue_name,
        "race_no": rno,
        "score": score,
        "rank": rank,
        "reasons": reasons,
        "boat1_class": boat1["class"]
    }

async def find_rough_races_today():
    """本日の波乱レースを一括検索してスコア順に返す"""
    url_index = "https://www.boatrace.jp/owpc/pc/race/index"
    
    async with aiohttp.ClientSession() as session:
        # 1. 開催場の取得
        async with session.get(url_index, timeout=10) as resp:
            if resp.status != 200:
                return []
            html_index = await resp.text()
            
        soup_index = BeautifulSoup(html_index, 'html.parser')
        links = soup_index.select('a[href^="/owpc/pc/race/raceindex"]')
        
        active_venues = []
        date_hd = ""
        for link in links:
            href = link.get('href', '')
            match = re.search(r'jcd=(\d{2})&hd=(\d{8})', href)
            if match:
                jcd = match.group(1)
                hd = match.group(2)
                if jcd not in [v['jcd'] for v in active_venues]:
                    active_venues.append({'jcd': jcd, 'hd': hd})
                    date_hd = hd # 最初に見つかった日付を使用

        if not active_venues:
            return []
            
        # 2. 全レースのURLを生成
        urls = []
        for v in active_venues:
            for rno in range(1, 13):
                url = f"https://www.boatrace.jp/owpc/pc/race/racelist?rno={rno}&jcd={v['jcd']}&hd={v['hd']}"
                urls.append((url, VENUE_NAMES.get(v['jcd'], "不明"), rno))
                
        # 3. 非同期で全ページ取得 (並列数制限)
        semaphore = asyncio.Semaphore(20)
        tasks = [fetch_html(session, u[0], semaphore) for u in urls]
        htmls = await asyncio.gather(*tasks)
        
        # 4. 解析とスコアリング
        results = []
        for (url, venue_name, rno), html in zip(urls, htmls):
            race_info = parse_racelist(html, venue_name, rno)
            if race_info and race_info["score"] > 0:
                results.append(race_info)
                
        # スコア順にソート
        results.sort(key=lambda x: x["score"], reverse=True)
        return results

if __name__ == "__main__":
    # テスト用
    res = asyncio.run(find_rough_races_today())
    for r in res[:10]:
        print(f"[{r['rank']}ランク] {r['venue']} {r['race_no']}R | スコア:{r['score']} | 理由: {', '.join(r['reasons'])}")
