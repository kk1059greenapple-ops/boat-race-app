import os
import time
import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.impute import SimpleImputer
import warnings

warnings.filterwarnings('ignore')

# ==========================================
# 1. データ生成・取得部（モック）
# ==========================================
def generate_exhibition_mock_data(n=30000):
    """
    指定のデータ特性を持たせた展示タイム・オリジナル展示のモックデータを生成します。
    """
    print("✅ 分析用ダミー展示データを生成中...")
    np.random.seed(42)
    
    venues = [
        "桐生", "戸田", "江戸川", "平和島", "多摩川", "浜名湖",
        "蒲郡", "常滑", "津", "三国", "びわこ", "住之江",
        "尼崎", "鳴門", "丸亀", "児島", "宮島", "徳山",
        "下関", "若松", "芦屋", "福岡", "唐津", "大村"
    ]
    
    data = []
    for _ in range(n):
        v = np.random.choice(venues)
        
        # レース全体のベースタイム
        base_ex_time = np.random.normal(6.70, 0.05)
        base_st_time = np.random.normal(0.15, 0.05)
        
        race_boats = []
        for boat in range(1, 7):
            # タイム生成
            ex_time = base_ex_time + np.random.normal(0, 0.03)
            turn_time = np.random.normal(5.00, 0.05)
            straight_time = np.random.normal(6.50, 0.05)
            
            # ハイフン（欠損）のシミュレーション（約5%の確率で欠損）
            ex_time_str = str(round(ex_time, 2)) if np.random.rand() > 0.05 else "-"
            turn_time_str = str(round(turn_time, 2)) if np.random.rand() > 0.05 else "-"
            straight_time_str = str(round(straight_time, 2)) if np.random.rand() > 0.05 else "-"
            
            # 勝ちやすさのベース確率（1号艇が圧倒的有利）
            win_prob = 10 if boat == 1 else (7 - boat)
            
            # 会場特性のシミュレーション
            # 1: イン最強場（大村等）は、1号艇の展示タイムが悪くても逃げる
            if v == "大村" and boat == 1:
                win_prob += 15 # 無条件で強力
            
            # 2: まわり足重視の場（住之江など）は、turn_timeの良さが着順に直結
            if v == "住之江" and turn_time < 4.98:
                win_prob += 5
                
            # 3: 展示タイム重視の場（戸田・平和島でのダッシュ勢など）
            if v in ["戸田", "平和島"] and boat >= 3 and ex_time < base_ex_time - 0.02:
                win_prob += 8
            
            # タイムがマイナス（良い）ほど確率アップ
            win_prob += (base_ex_time - ex_time) * 100
            
            avg_st_val = round(base_st_time + np.random.normal(0, 0.02) - (0.01 * (7 - boat)), 2)
            motor_2ren_val = round(max(0, np.random.normal(35.0, 10.0) + (win_prob * 10)), 1)
            race_boats.append({
                "会場": v,
                "号艇": boat,
                "展示タイム": ex_time_str,
                "まわり足": turn_time_str,
                "直線タイム": straight_time_str,
                "平均ST": str(avg_st_val),
                "モーター2連率": str(motor_2ren_val),
                "win_prob_score": max(1, win_prob - (avg_st_val * 10) + (motor_2ren_val / 20)) # 勝率補正
            })
            
        # 勝者の抽選
        probs = [b["win_prob_score"] for b in race_boats]
        total_prob = sum(probs)
        norm_probs = [p / total_prob for p in probs]
        winner = np.random.choice(range(1, 7), p=norm_probs)
        
        for b in race_boats:
            b["1着"] = 1 if b["号艇"] == winner else 0
            del b["win_prob_score"]
            data.append(b)
            
    df = pd.DataFrame(data)
    
    # タイム順位を計算するためにハイフン処理を先行するが、ここでは生データを返す
    return df

# ==========================================
# 2. 特殊ケース「-（ハイフン）」の補完とタイム順位算定
# ==========================================
def preprocess_exhibition_times(df):
    print("✅ 欠損データ（-）の統計的補完およびタイム順位を計算中...")
    
    time_cols = ["展示タイム", "まわり足", "直線タイム", "一週タイム", "平均ST", "モーター2連率"]
    for col in time_cols:
        if col not in df.columns: df[col] = "-"
        df[col] = pd.to_numeric(df[col].replace("-", np.nan), errors='coerce')
    
    df['race_id'] = np.repeat(np.arange(len(df) // 6), 6)
    
    for col in time_cols:
        df[col] = df.groupby('race_id')[col].transform(lambda x: x.fillna(x.mean()))
        df[col].fillna(df[col].mean(), inplace=True)
        
    for col in time_cols:
        df[f"{col}順位"] = df.groupby('race_id')[col].rank(method='min', ascending=True)
    
    return df

def optimize_exhibition_scores(df):
    print("✅ 全オリジナル展示項目の補正スコアを最適化中...")
    
    results = {}
    time_cols = ["展示タイム", "まわり足", "直線タイム", "一週タイム", "平均ST", "モーター2連率"]
    
    for col in time_cols:
        df['順位カテゴリ'] = df[f'{col}順位'].fillna(4).astype(int).astype(str) + "位"
        df['解析キー'] = df['会場'] + "_" + df['号艇'].astype(str) + "号艇_" + df['順位カテゴリ']
        
        X = pd.get_dummies(df['解析キー'])
        y = df['1着']
        
        from sklearn.linear_model import LogisticRegression
        model = LogisticRegression(fit_intercept=False, C=0.5, max_iter=1000)
        model.fit(X, y)
        coefs = pd.Series(model.coef_[0], index=X.columns)
        
        SCALING_FACTOR = 0.5 
        venues_order = [
            "桐生", "戸田", "江戸川", "平和島", "多摩川", "浜名湖",
            "蒲郡", "常滑", "津", "三国", "びわこ", "住之江",
            "尼崎", "鳴門", "丸亀", "児島", "宮島", "徳山",
            "下関", "若松", "芦屋", "福岡", "唐津", "大村"
        ]
        
        col_results = []
        for v in venues_order:
            for b in range(1, 7):
                row = {"会場": v, "号艇": f"{b}号艇"}
                boat_scores = [coefs.get(f"{v}_{b}号艇_{rank}位", 0.0) for rank in range(1, 7)]
                base_score = np.mean(boat_scores) if len(boat_scores) > 0 else 0
                
                for rank in range(1, 7):
                    key = f"{v}_{b}号艇_{rank}位"
                    raw_coef = coefs.get(key, base_score)
                    adj_score = np.round((raw_coef - base_score) * SCALING_FACTOR, 3)
                    row[f"{col}{rank}位"] = adj_score
                    
                col_results.append(row)
        results[col] = pd.DataFrame(col_results)
        
    # Merge all 4 dataframes safely
    final_df = results["展示タイム"]
    for col in ["まわり足", "直線タイム", "一週タイム", "平均ST", "モーター2連率"]:
        final_df = pd.merge(final_df, results[col], on=["会場", "号艇"], how="inner")
        
    return final_df


# ==========================================
# メイン実行処理
# ==========================================
if __name__ == "__main__":
    print("=========================================================")
    print(" 🛥️ 展示タイム・オリジナル展示 最適補正値AI算出プログラム")
    print("=========================================================\n")
    
    # 1. データの用意
    df_raw = generate_exhibition_mock_data(n=30000)
    
    # 2. 前処理（ハイフンの補完処理・順位算出）
    df_processed = preprocess_exhibition_times(df_raw)
    
    # 3. 補正値の算出
    scores_df = optimize_exhibition_scores(df_processed)
    
    # 4. 出力
    output_filename = "exhibition_correction_scores.tsv"
    
    # スプレッドシート貼付用にタブ区切りで出力
    scores_df.to_csv(output_filename, index=False, sep='\t', encoding="utf-8-sig")
    
    print("\n=========================================================")
    print(f"🎉 分析が完了しました！結果を '{output_filename}' に保存しました。")
    print("=========================================================")
