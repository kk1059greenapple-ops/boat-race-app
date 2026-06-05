# 展示タイム・オリジナル展示タイムの場別・コース別補正値（秒）マスタ
# 補正計算式: 補正後タイム = 原タイム - 補正値
# 1コース等の窮屈な旋回によるロス（プラス補正）を減算して引き算、
# 6コース等の大外旋回によるスピード乗り（マイナス補正）を足算することで、純粋な機力をフェアに比較。

# ユーザーより提供された13場の補正データを定義
VENUE_EX_MODIFIERS = {
    "桐生": {
        1: {"turn": 0.04, "straight": 0.01, "lap": 0.25, "ex": 0.02},
        2: {"turn": 0.02, "straight": 0.00, "lap": 0.15, "ex": 0.01},
        3: {"turn": -0.01, "straight": -0.01, "lap": -0.05, "ex": -0.01},
        4: {"turn": -0.01, "straight": -0.02, "lap": -0.08, "ex": -0.02},
        5: {"turn": -0.02, "straight": -0.03, "lap": -0.12, "ex": -0.03},
        6: {"turn": -0.03, "straight": -0.03, "lap": -0.15, "ex": -0.04}
    },
    "戸田": {
        1: {"turn": 0.05, "straight": 0.02, "lap": 0.28, "ex": 0.03},
        2: {"turn": 0.03, "straight": 0.01, "lap": 0.18, "ex": 0.01},
        3: {"turn": 0.00, "straight": 0.00, "lap": -0.02, "ex": -0.01},
        4: {"turn": -0.02, "straight": -0.03, "lap": -0.12, "ex": -0.02},
        5: {"turn": -0.03, "straight": -0.04, "lap": -0.16, "ex": -0.04},
        6: {"turn": -0.04, "straight": -0.04, "lap": -0.20, "ex": -0.05}
    },
    "江戸川": {
        1: {"turn": 0.03, "straight": 0.01, "lap": 0.20, "ex": 0.02},
        2: {"turn": 0.01, "straight": 0.00, "lap": 0.10, "ex": 0.01},
        3: {"turn": 0.00, "straight": -0.01, "lap": -0.05, "ex": 0.00},
        4: {"turn": -0.01, "straight": -0.01, "lap": -0.08, "ex": -0.01},
        5: {"turn": -0.02, "straight": -0.02, "lap": -0.12, "ex": -0.02},
        6: {"turn": -0.03, "straight": -0.02, "lap": -0.15, "ex": -0.03}
    },
    "平和島": {
        1: {"turn": 0.04, "straight": 0.01, "lap": 0.26, "ex": 0.03},
        2: {"turn": 0.02, "straight": 0.00, "lap": 0.14, "ex": 0.01},
        3: {"turn": -0.01, "straight": -0.01, "lap": -0.06, "ex": -0.01},
        4: {"turn": -0.02, "straight": -0.02, "lap": -0.10, "ex": -0.02},
        5: {"turn": -0.03, "straight": -0.03, "lap": -0.14, "ex": -0.03},
        6: {"turn": -0.04, "straight": -0.03, "lap": -0.18, "ex": -0.04}
    },
    "多摩川": {
        1: {"turn": 0.04, "straight": 0.01, "lap": 0.24, "ex": 0.02},
        2: {"turn": 0.02, "straight": 0.00, "lap": 0.12, "ex": 0.01},
        3: {"turn": -0.01, "straight": -0.01, "lap": -0.04, "ex": -0.01},
        4: {"turn": -0.02, "straight": -0.02, "lap": -0.08, "ex": -0.02},
        5: {"turn": -0.03, "straight": -0.03, "lap": -0.12, "ex": -0.03},
        6: {"turn": -0.04, "straight": -0.03, "lap": -0.16, "ex": -0.04}
    },
    "浜名湖": {
        1: {"turn": 0.05, "straight": 0.01, "lap": 0.25, "ex": 0.03},
        2: {"turn": 0.02, "straight": 0.00, "lap": 0.15, "ex": 0.01},
        3: {"turn": 0.00, "straight": -0.01, "lap": -0.05, "ex": -0.01},
        4: {"turn": -0.02, "straight": -0.02, "lap": -0.10, "ex": -0.02},
        5: {"turn": -0.03, "straight": -0.03, "lap": -0.15, "ex": -0.03},
        6: {"turn": -0.04, "straight": -0.03, "lap": -0.18, "ex": -0.04}
    },
    "蒲郡": {
        1: {"turn": 0.04, "straight": 0.01, "lap": 0.22, "ex": 0.02},
        2: {"turn": 0.02, "straight": 0.00, "lap": 0.12, "ex": 0.01},
        3: {"turn": -0.01, "straight": -0.01, "lap": -0.05, "ex": -0.01},
        4: {"turn": -0.01, "straight": -0.02, "lap": -0.08, "ex": -0.02},
        5: {"turn": -0.02, "straight": -0.03, "lap": -0.12, "ex": -0.03},
        6: {"turn": -0.03, "straight": -0.03, "lap": -0.15, "ex": -0.04}
    },
    "常滑": {
        1: {"turn": 0.04, "straight": 0.01, "lap": 0.23, "ex": 0.02},
        2: {"turn": 0.02, "straight": 0.00, "lap": 0.13, "ex": 0.01},
        3: {"turn": -0.01, "straight": -0.01, "lap": -0.05, "ex": -0.01},
        4: {"turn": -0.02, "straight": -0.02, "lap": -0.09, "ex": -0.02},
        5: {"turn": -0.03, "straight": -0.03, "lap": -0.13, "ex": -0.03},
        6: {"turn": -0.04, "straight": -0.03, "lap": -0.16, "ex": -0.04}
    },
    "津": {
        1: {"turn": 0.05, "straight": 0.02, "lap": 0.26, "ex": 0.03},
        2: {"turn": 0.03, "straight": 0.01, "lap": 0.15, "ex": 0.01},
        3: {"turn": 0.00, "straight": 0.00, "lap": -0.04, "ex": -0.01},
        4: {"turn": -0.02, "straight": -0.02, "lap": -0.10, "ex": -0.02},
        5: {"turn": -0.03, "straight": -0.03, "lap": -0.15, "ex": -0.03},
        6: {"turn": -0.04, "straight": -0.04, "lap": -0.18, "ex": -0.04}
    },
    "三国": {
        1: {"turn": 0.04, "straight": 0.01, "lap": 0.25, "ex": 0.02},
        2: {"turn": 0.02, "straight": 0.00, "lap": 0.14, "ex": 0.01},
        3: {"turn": -0.01, "straight": -0.01, "lap": -0.05, "ex": -0.01},
        4: {"turn": -0.02, "straight": -0.02, "lap": -0.09, "ex": -0.02},
        5: {"turn": -0.03, "straight": -0.03, "lap": -0.14, "ex": -0.03},
        6: {"turn": -0.04, "straight": -0.03, "lap": -0.17, "ex": -0.04}
    },
    "びわこ": {
        1: {"turn": 0.05, "straight": 0.02, "lap": 0.28, "ex": 0.03},
        2: {"turn": 0.03, "straight": 0.01, "lap": 0.16, "ex": 0.01},
        3: {"turn": 0.00, "straight": 0.00, "lap": -0.03, "ex": -0.01},
        4: {"turn": -0.02, "straight": -0.03, "lap": -0.12, "ex": -0.02},
        5: {"turn": -0.03, "straight": -0.04, "lap": -0.16, "ex": -0.03},
        6: {"turn": -0.04, "straight": -0.04, "lap": -0.20, "ex": -0.04}
    },
    "住之江": {
        1: {"turn": 0.04, "straight": 0.01, "lap": 0.22, "ex": 0.02},
        2: {"turn": 0.02, "straight": 0.00, "lap": 0.11, "ex": 0.01},
        3: {"turn": -0.01, "straight": -0.01, "lap": -0.04, "ex": -0.01},
        4: {"turn": -0.01, "straight": -0.02, "lap": -0.08, "ex": -0.02},
        5: {"turn": -0.02, "straight": -0.03, "lap": -0.12, "ex": -0.03},
        6: {"turn": -0.03, "straight": -0.03, "lap": -0.15, "ex": -0.04}
    },
    "尼崎": {
        1: {"turn": 0.04, "straight": 0.01, "lap": 0.24, "ex": 0.02},
        2: {"turn": 0.02, "straight": 0.00, "lap": 0.12, "ex": 0.01}
    }
}

# ユーザー指定の13場の平均値をベースにした高品質デフォルト補正（未定義の場および尼崎の後半コース用）
DEFAULT_EX_MODIFIERS = {
    1: {"turn": 0.04, "straight": 0.01, "lap": 0.24, "ex": 0.02},
    2: {"turn": 0.02, "straight": 0.00, "lap": 0.13, "ex": 0.01},
    3: {"turn": -0.01, "straight": -0.01, "lap": -0.04, "ex": -0.01},
    4: {"turn": -0.02, "straight": -0.02, "lap": -0.09, "ex": -0.02},
    5: {"turn": -0.03, "straight": -0.03, "lap": -0.14, "ex": -0.03},
    6: {"turn": -0.04, "straight": -0.03, "lap": -0.17, "ex": -0.04}
}

def get_exhibition_correction(venue: str, course: int, metric_type: str) -> float:
    """
    指定された競艇場・コース・計測指標の補正値（秒）を取得する。
    metric_type: 'turn' | 'straight' | 'lap' | 'ex'
    データが存在しない場やコースは、DEFAULT_EX_MODIFIERS より平均的な補正値を適用する。
    """
    # 競艇場の完全一致または部分一致（「江戸川」と「江戸川競艇」などの揺れ対策）
    target_venue = None
    for k in VENUE_EX_MODIFIERS.keys():
        if k in venue:
            target_venue = k
            break
            
    if target_venue:
        course_data = VENUE_EX_MODIFIERS[target_venue].get(course)
        if course_data and metric_type in course_data:
            return course_data[metric_type]
            
    # フォールバック
    fallback_data = DEFAULT_EX_MODIFIERS.get(course, {metric_type: 0.0})
    return fallback_data.get(metric_type, 0.0)
