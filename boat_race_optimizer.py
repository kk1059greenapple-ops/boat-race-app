import os
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sklearn.linear_model import LogisticRegression

# ==========================================
# 1. データ取得（ウェブスクレイピング部）
# ==========================================
def fetch_race_data(start_date_str, end_date_str, output_csv="boat_race_raw_data.csv"):
    """
    指定期間のボートレース公式からレース結果をスクレイピングする
    ※注意: 1年分(約5.5万レース)を実行すると数時間〜十数時間かかり、
    段階的に取得しないとサーバーにブロックされる可能性があります。
    """
    start_date = datetime.strptime(start_date_str, "%Y%m%d")
    end_date = datetime.strptime(end_date_str, "%Y%m%d")
    
    # 24場のコード（01=桐生 〜 24=大村）
    venues = {str(i).zfill(2): i for i in range(1, 25)}
    venue_names = {
        "01": "桐生", "02": "戸田", "03": "江戸川", "04": "平和島", "05": "多摩川", "06": "浜名湖",
        "07": "蒲郡", "08": "常滑", "09": "津", "10": "三国", "11": "びわこ", "12": "住之江",
        "13": "尼崎", "14": "鳴門", "15": "丸亀", "16": "児島", "17": "宮島", "18": "徳山",
        "19": "下関", "20": "若松", "21": "芦屋", "22": "福岡", "23": "唐津", "24": "大村"
    }
    
    all_data = []
    current_date = start_date
    
    print("--- スクレイピング開始 ---")
    while current_date <= end_date:
        hd = current_date.strftime("%Y%m%d")
        
        for jcd, jcd_name in venue_names.items():
            for rno in range(1, 13):
                url = f"https://www.boatrace.jp/owpc/pc/race/raceresult?rno={rno}&jcd={jcd}&hd={hd}"
                
                try:
                    res = requests.get(url, timeout=5)
                    res.encoding = 'utf-8'
                    soup = BeautifulSoup(res.text, "html.parser")
                    
                    if "データがありません" in soup.text or "お探しのページが見つかりません" in soup.text:
                        time.sleep(0.5)
                        continue
                        
                    # 天候・風速の取得
                    weather_div = soup.find("div", class_="weather1")
                    if not weather_div:
                        time.sleep(0.5)
                        continue
                        
                    wind_speed = 0
                    for label in weather_div.find_all("span", class_="weather1_bodyUnitLabelTitle"):
                        if "m" in label.text:
                            try:
                                wind_speed = int(label.text.replace("m", "").strip())
                                break
                            except:
                                pass
                    
                    # 風向きの判定
                    # ※注意：公式サイトは16方位（北、北西など）で提供しており、コースに対して追い風か向かい風かは
                    # 本来「各競艇場の水面レイアウト（1マークの向いている方角）」との交差計算が必要です。
                    # ここでは実装デモとして、ランダムに「追い風」「向かい風」などを割り当てています。
                    # 厳密な判定が必要な場合は、マクール等の情報サイトからスクレイピングするか、方位変換マトリクスが必要です。
                    mock_wind_direction = np.random.choice(["追い風", "向かい風", "左横風", "右横風"]) 
                    
                    # 着順の取得
                    tbody = soup.find("tbody", class_="is-p3-0")
                    if not tbody:
                        continue
                    rows = tbody.find_all("tr")
                    ranks = []
                    for row in rows[:3]: # 1~3着
                        boat_td = row.find("td", class_="is-fs14")
                        if boat_td:
                            ranks.append(int(boat_td.text.strip()))
                            
                    if len(ranks) < 3:
                        continue
                        
                    # 配当金（3連単）
                    payout = 0
                    tds = soup.find_all("td")
                    for i, td in enumerate(tds):
                        if "3連単" in td.text:
                            # 金額の抽出（カンマ除去）
                            price_text = tds[i+2].text.replace("¥", "").replace(",", "").strip()
                            if price_text.isdigit():
                                payout = int(price_text)
                            break
                    
                    all_data.append({
                        "日付": hd,
                        "会場": jcd_name,
                        "レース番号": rno,
                        "風速": wind_speed,
                        "風向き": mock_wind_direction,
                        "1着": ranks[0],
                        "2着": ranks[1],
                        "3着": ranks[2],
                        "配当金": payout
                    })
                    
                except Exception as e:
                    print(f"Error fetching {url}: {e}")
                
                time.sleep(1) # サーバー負荷軽減のため必ずウェイトを入れる
                
        print(f"{hd} のデータ取得完了")
        current_date += timedelta(days=1)
        
    df = pd.DataFrame(all_data)
    df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    return df

def generate_mock_data(n=30000):
    """
    1年分のスクレイピングは時間がかかるため、
    即座にアルゴリズムを実行して確認するためのダミーデータ生成器です。
    """
    print(f"✅ テスト用のモックデータを {n} 件生成中...")
    np.random.seed(123)
    venues = ["桐生","戸田","江戸川","平和島","多摩川","浜名湖","蒲郡","常滑","津","三国",
              "びわこ","住之江","尼崎","鳴門","丸亀","児島","宮島","徳山","下関","若松",
              "芦屋","福岡","唐津","大村"]
    conditions = ["追い風", "向かい風", "左横風", "右横風"]
    
    data = []
    for _ in range(n):
        v = np.random.choice(venues)
        w_s = np.random.randint(0, 9)
        w_d = np.random.choice(conditions) if w_s > 0 else "無風"
        
        boats = [1, 2, 3, 4, 5, 6]
        # アルゴリズムが効果的に働くのを検証するため、意図的に「戸田×向かい風」の配分を変える
        if v == "戸田" and w_d == "向かい風" and w_s >= 5:
            # 1号艇が弱くなり、3・4号艇が強くなる
            p = [0.25, 0.20, 0.20, 0.20, 0.10, 0.05]
        elif v == "大村" and w_d == "追い風" and w_s >= 5:
            # 1号艇が少し弱くなり、2号艇の差しが決まりやすくなる
            p = [0.50, 0.25, 0.10, 0.08, 0.04, 0.03]
        else:
            # 全国平均に近い
            p = [0.55, 0.15, 0.12, 0.08, 0.06, 0.04]
            
        ranks = np.random.choice(boats, size=3, replace=False, p=p)
        
        data.append({
            "日付": "20230101",
            "会場": v,
            "風速": w_s,
            "風向き": w_d,
            "1着": ranks[0],
            "2着": ranks[1],
            "3着": ranks[2],
            "配当金": np.random.randint(1000, 30000)
        })
    return pd.DataFrame(data)


# ==========================================
# 2. 条件ごとの勝率・連対率の算出
# ==========================================
def preprocess_conditions(df):
    """風速と風向きを組み合わせて『風条件』カテゴリを作成"""
    def categorize_wind(row):
        speed = row["風速"]
        direction = row["風向き"]
        
        if speed <= 2 or direction == "無風":
            return "無風〜微風"
        elif speed >= 5 and "追い風" in direction:
            return "追い風5m以上"
        elif speed >= 5 and "向かい風" in direction:
            return "向かい風5m以上"
        else:
            return f"{direction} (3-4m等)"
            
    df["風条件"] = df.apply(categorize_wind, axis=1)
    df["分析キー"] = df["会場"] + " × " + df["風条件"]
    return df

def calculate_win_rates(df):
    """各条件での1着率・3連対率（3着以内に入る確率）を計算"""
    results = []
    
    for key, group in df.groupby("分析キー"):
        total = len(group)
        if total < 20: 
            continue # 統計的に意味を持たせるため、サンプルが少なすぎる条件はスキップ
            
        row = {"条件": key, "対象レース数": total}
        for boat in range(1, 7):
            win = len(group[group["1着"] == boat])
            top3 = len(group[(group["1着"]==boat) | (group["2着"]==boat) | (group["3着"]==boat)])
            
            row[f"{boat}号艇_1着率"] = round(win / total, 3)
            row[f"{boat}号艇_3連対率"] = round(top3 / total, 3)
            
        results.append(row)
        
    return pd.DataFrame(results)

# ==========================================
# 3. 最適な「補正数値（スコア）」の導出（機械学習）
# ==========================================
def optimize_correction_scores(df):
    """
    『ロジスティック回帰（Logistic Regression）』を使用して、
    各条件が「1着になる確率（ロジット）」に与える純粋な影響度（偏回帰係数）を抽出します。
    それをスプレッドシート上の「加減算ポイント」として使いやすいスケールに調整します。
    """
    print("✅ ロジスティック回帰で最適な補正数値を算出中...")
    
    # 目的変数を予測するための特徴量（ダミー変数化）
    X = pd.get_dummies(df["分析キー"], drop_first=False)
    
    scores_df = pd.DataFrame({"条件": X.columns})
    
    # 全体のスプレッドシートポイントの重み調整。
    # ここを大きくすると、出力される点数の振れ幅（+1.0 ~ -1.0など）が大きくなります。
    SCALING_FACTOR = 0.5 
    
    for boat in range(1, 7):
        # ターゲット：対象の号艇が1着であったか（1 or 0）
        y = (df["1着"] == boat).astype(int)
        
        # フィッティング（L2正則化で過学習を防ぎつつ重みを抽出）
        model = LogisticRegression(fit_intercept=False, C=1.0, max_iter=1000)
        model.fit(X, y)
        
        coefs = model.coef_[0]
        
        # モデルが算出した係数の全体平均を「ベース（±0点）」とする
        baseline = coefs.mean()
        
        # ベースからどれだけ乖離しているかを補正値として抽出
        adjusted_scores = (coefs - baseline) * SCALING_FACTOR
        
        scores_df[f"{boat}号艇_補正点"] = np.round(adjusted_scores, 3)
        
    return scores_df

# ==========================================
# 4. スプレッドシート用（24場×6艇）のデータ整形
# ==========================================
def extract_venue_base_scores(df):
    """
    A22:G45の領域に貼り付けるための、「風などの条件を抜いた会場単体の地力補正値」を算出。
    """
    print("✅ スプレッドシート貼付用（会場別ベース補正値）を構築中...")
    venues_order = [
        "桐生", "戸田", "江戸川", "平和島", "多摩川", "浜名湖",
        "蒲郡", "常滑", "津", "三国", "びわこ", "住之江",
        "尼崎", "鳴門", "丸亀", "児島", "宮島", "徳山",
        "下関", "若松", "芦屋", "福岡", "唐津", "大村"
    ]
    
    # ここでは風などに依存しない、「会場名だけ」を特徴量として再度回帰を行います
    X = pd.get_dummies(df["会場"], drop_first=False)
    
    boat_coefs = {}
    SCALING_FACTOR = 0.5
    for boat in range(1, 7):
        y = (df["1着"] == boat).astype(int)
        model = LogisticRegression(fit_intercept=False, C=1.0, max_iter=1000)
        model.fit(X, y)
        coefs = pd.Series(model.coef_[0], index=X.columns)
        baseline = coefs.mean()
        boat_coefs[boat] = np.round((coefs - baseline) * SCALING_FACTOR, 3)

    tsv_data = []
    for v in venues_order:
        row = {"会場": v}
        for b in range(1, 7):
            val = boat_coefs[b][v] if v in boat_coefs[b].index else 0.0
            row[f"{b}号艇"] = val
        tsv_data.append(row)
        
    return pd.DataFrame(tsv_data)

# ==========================================
# メイン実行処理
# ==========================================
if __name__ == "__main__":
    print("=========================================================")
    print(" ボートレース予想アルゴリズム 最適補正値AI算出プログラム")
    print("=========================================================\n")
    
    # 1. データの用意 (本番利用時はここで fetch_race_data() を呼び出します)
    # df_raw = fetch_race_data("20230101", "20231231")
    df_raw = generate_mock_data(n=50000)
    
    # 2. 前処理・集計
    df_processed = preprocess_conditions(df_raw)
    stats_df = calculate_win_rates(df_processed)
    
    # 3. 補正値の算出
    scores_df = optimize_correction_scores(df_processed)
    
    # 4. 結合して詳細版CSVに出力 (風の条件込みの詳細データ)
    final_output = pd.merge(stats_df, scores_df, on="条件", how="inner")
    output_filename = "boat_race_full_analysis.csv"
    final_output.to_csv(output_filename, index=False, encoding="utf-8-sig")
    
    # 5. スプレッドシートA22:G45貼付用TSVの出力 (会場別・タブ区切り)
    venue_scores_df = extract_venue_base_scores(df_processed)
    tsv_filename = "spreadsheet_paste_A22_G45.tsv"
    # header=Falseにすることで「会場名 1号艇 2号艇...」のラベル行を消し、A22セルからいきなりコピペできるようにします
    venue_scores_df.to_csv(tsv_filename, index=False, sep='\t', header=False) 
    
    print("\n=========================================================")
    print(f"🎉 分析が完了しました！")
    print(f"  ■1. 詳細データ(風条件含む): '{output_filename}'")
    print(f"  ■2. スプレッドシート直貼り用: '{tsv_filename}'")
    print("=========================================================")
