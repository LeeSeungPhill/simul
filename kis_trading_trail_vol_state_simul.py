"""
kis_trading_trail_vol_state_simul.py
kis_trading_trail_vol_state.py 와 동일한 구조.
trading_trail_simul 테이블 대상으로 안전마진/이탈가 시뮬레이션 처리.
실제 KIS 매도 주문 없이 DB 업데이트만 수행.
"""
from datetime import datetime, timedelta
from datetime import time as dt_time
from dateutil.relativedelta import relativedelta
import requests
import pandas as pd
import psycopg2 as db
import json
import time
import threading
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import kis_api_resp as resp

# ─────────────────────────────────────────
# 설정
# ─────────────────────────────────────────
SIMUL_TABLE       = "public.trading_trail_simul"
SIMUL_ACCT        = "SIMUL"
SIMUL_DLY_ACCT    = "74346047"         # dly_*_simul 의 acct
API_NICK          = "phills2"          # 시장 데이터(분봉·일봉) 조회에만 사용하는 계좌
TOTAL_INVEST_BASE = 25_000_000         # 전체투자금 기준 (원)
# 시장비율 리밸런싱(kis_market_ratio_rebalance.py simulate_rebalance 복제) 튜닝 포인트
TOP_CUT      = 70    # sell_priority(100 - strength) 이 값 이상이면 종목당 전량 매도 허용, 아니면 일 최대 70%
PER_NAME_CAP = 0.5    # 종목당 1일 최대 매도 비중 (평가금액 대비)
W_CHART       = 0.5    # strength = W_CHART*차트 

BASE_URL    = "https://openapi.koreainvestment.com:9443"
conn_string = "dbname='fund_risk_mng' host='192.168.50.81' port='5432' user='postgres' password='asdf1234'"

today = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y%m%d")

# 일봉 캐시
_daily_cache_lock       = threading.Lock()
_daily_chart_full_cache = {}
_prev_day_info_cache    = {}

# 종목 시장구분 캐시
_stock_market_cache_lock = threading.Lock()
_stock_market_cache      = {}


# ─────────────────────────────────────────
# 인증 / 계좌
# ─────────────────────────────────────────
def auth(APP_KEY, APP_SECRET):
    headers = {"content-type": "application/json"}
    body    = {"grant_type": "client_credentials", "appkey": APP_KEY, "appsecret": APP_SECRET}
    res     = requests.post(f"{BASE_URL}/oauth2/tokenP", headers=headers,
                            data=json.dumps(body), verify=False, timeout=10)
    data = res.json()
    if "access_token" not in data:
        raise ValueError(f"KIS 인증 실패: {data.get('msg1', data)}")
    return data["access_token"]


def account(nickname, conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT acct_no, access_token, app_key, app_secret, token_publ_date,
               substr(token_publ_date, 0, 9) AS token_day, bot_token1, bot_token2, chat_id
        FROM "stockAccount_stock_account" WHERE nick_name = %s
    """, (nickname,))
    row = cur.fetchone()
    cur.close()
    acct_no, access_token, app_key, app_secret, token_publ_date, token_day, bt1, bt2, chat_id = row
    _real_today = datetime.now().strftime("%Y%m%d")
    if (datetime.now() - datetime.strptime(token_publ_date, '%Y%m%d%H%M%S')).days >= 1 or token_day != _real_today:
        access_token    = auth(app_key, app_secret)
        token_publ_date = datetime.now().strftime('%Y%m%d%H%M%S')
        cur2 = conn.cursor()
        cur2.execute("""
            UPDATE "stockAccount_stock_account"
            SET access_token=%s, token_publ_date=%s, last_chg_date=%s WHERE acct_no=%s
        """, (access_token, token_publ_date, datetime.now(), acct_no))
        conn.commit()
        cur2.close()
    return {'acct_no': acct_no, 'access_token': access_token,
            'app_key': app_key, 'app_secret': app_secret,
            'bot_token1': bt1, 'bot_token2': bt2, 'chat_id': chat_id}


# ─────────────────────────────────────────
# 영업일 유틸
# ─────────────────────────────────────────
def get_previous_business_day(day, conn):
    cur = conn.cursor()
    cur.execute("SELECT prev_business_day_char(%s)", (day,))
    res = cur.fetchone()[0]
    cur.close()
    return res


def is_business_day(check_date, conn) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT is_business_day(%s)", (check_date,))
    res = cur.fetchone()[0]
    cur.close()
    return bool(res)


# ─────────────────────────────────────────
# KIS API : 일봉·분봉
# ─────────────────────────────────────────
def get_excg_id():
    t = datetime.now().strftime('%H%M')
    return "KRX" if '0900' <= t < '1530' else "NXT"


_ITEM_CHART_URL = f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"


def _kis_itemchart_headers(access_token, app_key, app_secret):
    return {"Content-Type": "application/json",
            "authorization": f"Bearer {access_token}",
            "appkey": app_key, "appsecret": app_secret,
            "tr_id": "FHKST03010100", "custtype": "P"}


def get_kis_daily_chart(stock_code, trade_date, access_token, app_key, app_secret,
                        market_code="J", period="D", adjust_price="1", verbose=True):
    """단일 날짜 일봉 조회 (inquire-daily-itemchartprice, output2 사용)."""
    d_from = (datetime.strptime(trade_date, "%Y%m%d") - timedelta(days=10)).strftime("%Y%m%d")
    params = {"FID_COND_MRKT_DIV_CODE": market_code,
              "FID_INPUT_ISCD":         stock_code,
              "FID_INPUT_DATE_1":       d_from,
              "FID_INPUT_DATE_2":       trade_date,
              "FID_PERIOD_DIV_CODE":    period,
              "FID_ORG_ADJ_PRC":        adjust_price}
    for attempt in range(3):
        try:
            res  = requests.get(_ITEM_CHART_URL,
                                headers=_kis_itemchart_headers(access_token, app_key, app_secret),
                                params=params, timeout=10)
            data = res.json()
            if data.get('rt_cd') == '1' and attempt < 2:
                time.sleep(1); continue
            break
        except Exception as e:
            if attempt < 2: time.sleep(0.5 * (attempt + 1))
            else:
                if verbose: print(f"⚠️ 일봉 조회 실패 ({stock_code}): {e}")
                return None
    if not data.get("output2"):
        return None
    df     = pd.DataFrame(data["output2"])
    day_df = df[df["stck_bsop_date"] == trade_date]
    if day_df.empty:
        return None
    return {
        "low_price":   int(day_df.iloc[0]["stck_lwpr"]),
        "high_price":  int(day_df.iloc[0]["stck_hgpr"]),
        "close_price": int(day_df.iloc[0]["stck_clpr"]),
        "volume":      int(day_df.iloc[0]["acml_vol"]),
    }


def get_kis_daily_chart_full(stock_code, trade_date, access_token, app_key, app_secret):
    """전체 일봉 조회 (최근 ~140일, inquire-daily-itemchartprice output2 사용, 캐시 우선)."""
    with _daily_cache_lock:
        if stock_code in _daily_chart_full_cache:
            return _daily_chart_full_cache[stock_code]
    d_from = (datetime.strptime(trade_date, "%Y%m%d") - timedelta(days=140)).strftime("%Y%m%d")
    params = {"FID_COND_MRKT_DIV_CODE": "J",
              "FID_INPUT_ISCD":         stock_code,
              "FID_INPUT_DATE_1":       d_from,
              "FID_INPUT_DATE_2":       trade_date,
              "FID_PERIOD_DIV_CODE":    "D",
              "FID_ORG_ADJ_PRC":        "1"}
    try:
        for attempt in range(3):
            try:
                res  = requests.get(_ITEM_CHART_URL,
                                    headers=_kis_itemchart_headers(access_token, app_key, app_secret),
                                    params=params, timeout=10)
                data = res.json()
                if data.get('rt_cd') == '1' and attempt < 2:
                    time.sleep(1); continue
                break
            except Exception:
                if attempt < 2: time.sleep(0.5 * (attempt + 1))
                else: raise
        if not data.get("output2"):
            return []
        result = [
            {"date":        item["stck_bsop_date"],
             "high_price":  int(item["stck_hgpr"]),
             "low_price":   int(item["stck_lwpr"]),
             "close_price": int(item["stck_clpr"]),
             "volume":      int(item["acml_vol"])}
            for item in data["output2"]
            if item.get("stck_bsop_date") and int(item.get("acml_vol", 0)) > 0
        ]
        result = sorted(result, key=lambda x: x["date"])
        if result:
            with _daily_cache_lock:
                _daily_chart_full_cache[stock_code] = result
        return result
    except Exception as e:
        print(f"일봉 전체 조회 오류: {e}")
        return []


def get_prev_day_info(stock_code, trade_date, access_token, app_key, app_secret, conn):
    cache_key = (stock_code, trade_date)
    with _daily_cache_lock:
        if cache_key in _prev_day_info_cache:
            return _prev_day_info_cache[cache_key]
    prev_date = get_previous_business_day(
        (datetime.strptime(trade_date, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d"), conn
    )
    result = get_kis_daily_chart(stock_code, prev_date, access_token, app_key, app_secret)
    if result is not None:
        with _daily_cache_lock:
            _prev_day_info_cache[cache_key] = result
    return result


def get_kis_1min_dailychart(stock_code, trade_date, trade_time, access_token, app_key, app_secret,
                            market_code="J", include_past="Y", include_fake_tick="N", verbose=True):
    url     = f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-time-dailychartprice"
    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appkey": app_key, "appsecret": app_secret,
               "tr_id": "FHKST03010230", "custtype": "P"}
    params  = {"FID_COND_MRKT_DIV_CODE": market_code,
               "FID_INPUT_ISCD": stock_code,
               "FID_INPUT_DATE_1": trade_date,
               "FID_INPUT_HOUR_1": trade_time,
               "FID_PW_DATA_INCU_YN": include_past,
               "FID_FAKE_TICK_INCU_YN": include_fake_tick}
    for attempt in range(3):
        try:
            res  = requests.get(url, headers=headers, params=params, timeout=10)
            data = res.json()
            if data.get('rt_cd') == '1' and attempt < 2:
                time.sleep(1); continue
            break
        except Exception as e:
            if attempt < 2: time.sleep(0.5 * (attempt + 1))
            else:
                if verbose: print(f"⚠️ 분봉 조회 실패 ({stock_code}, {trade_time}): {e}")
                return pd.DataFrame()
    if "output2" not in data or not data["output2"]:
        return pd.DataFrame()
    df       = pd.DataFrame(data["output2"])
    acml_vol = data.get("output1", {}).get("acml_vol", "0")
    if df.empty:
        return df
    df = df.rename(columns={"stck_bsop_date":"일자","stck_cntg_hour":"시간",
                             "stck_oprc":"시가","stck_hgpr":"고가",
                             "stck_lwpr":"저가","stck_prpr":"종가","cntg_vol":"거래량"})
    df["누적거래량"] = acml_vol
    df["시간"] = df["시간"].str[:2] + ":" + df["시간"].str[2:4]
    df = df.sort_values(["일자","시간"])
    return df[["일자","시간","시가","고가","저가","종가","거래량","누적거래량"]]


def get_kis_1min_full_day(stock_code, trade_date, start_time, access_token, app_key, app_secret, verbose=False):
    all_df          = []
    current_time    = start_time
    prev_oldest_dt  = None
    while True:
        df = get_kis_1min_dailychart(stock_code, trade_date, current_time,
                                     access_token, app_key, app_secret, verbose=verbose)
        if df.empty:
            break
        df          = df.sort_values("시간")
        oldest_time = df.iloc[0]["시간"].replace(":", "")
        oldest_dt   = datetime.strptime(trade_date + oldest_time, "%Y%m%d%H%M")
        if prev_oldest_dt is not None and oldest_dt >= prev_oldest_dt:
            break
        prev_oldest_dt = oldest_dt
        all_df.append(df)
        if trade_date.endswith("0102") or trade_date.endswith("1119"):
            if oldest_time <= "100000": break
        else:
            if oldest_time <= "090000": break
        dt           = oldest_dt - timedelta(minutes=1)
        current_time = dt.strftime("%H%M%S")
        if len(df) < 120:
            break
    if not all_df:
        return pd.DataFrame()
    df_all = pd.concat(all_df, ignore_index=True)
    df_all["dt"] = pd.to_datetime(
        df_all["일자"] + df_all["시간"].str.replace(":", ""), format="%Y%m%d%H%M"
    )
    return df_all.drop_duplicates("dt").sort_values("dt").reset_index(drop=True)


# ─────────────────────────────────────────
# 10분봉 유틸
# ─────────────────────────────────────────
def get_10min_key(dt: datetime):
    return dt.replace(minute=(dt.minute // 10) * 10, second=0)

def get_completed_10min_key(dt: datetime):
    base = (dt.minute // 10) * 10
    return dt.replace(minute=base, second=0, microsecond=0)

def get_next_completed_10min_dt(dt: datetime) -> datetime:
    base = (dt.minute // 10) * 10
    return dt.replace(minute=base, second=0, microsecond=0) + timedelta(minutes=10)


# ─────────────────────────────────────────
# ATR
# ─────────────────────────────────────────
def calculate_atr(daily_data, period=14):
    if len(daily_data) < period + 1:
        return None
    trs = []
    for i in range(1, len(daily_data)):
        h, l, prev_c = daily_data[i]['high_price'], daily_data[i]['low_price'], daily_data[i-1]['close_price']
        trs.append(max(h - l, abs(h - prev_c), abs(l - prev_c)))
    return int(sum(trs[-period:]) / period)


# ─────────────────────────────────────────
# 호가 단위
# ─────────────────────────────────────────
def get_valid_sell_price(price: int) -> int:
    if price < 1000:        tick = 1
    elif price < 5000:      tick = 5
    elif price < 10000:     tick = 10
    elif price < 50000:     tick = 50
    elif price < 100000:    tick = 100
    elif price < 500000:    tick = 500
    else:                   tick = 1000
    return (price // tick) * tick


# ─────────────────────────────────────────
# 거래량 조건
# ─────────────────────────────────────────
def volume_rate_chk(current_time, vol_ratio, trade_date=""):
    is_ok       = False
    late_open   = trade_date.endswith("0102") or trade_date.endswith("1119")
    late_close  = trade_date.endswith("1119")
    if late_open:
        if   1000 <= int(current_time) <= 1020: is_ok = vol_ratio >= 20
        elif 1021 <= int(current_time) <= 1030: is_ok = vol_ratio >= 25
        elif int(current_time) < 1100:          is_ok = vol_ratio >= 50
        elif late_close and 1600 <= int(current_time) <= 1630: is_ok = vol_ratio >= 25
        elif not late_close and 1500 <= int(current_time) <= 1530: is_ok = vol_ratio >= 25
        else: is_ok = True
    else:
        if   900 <= int(current_time) <= 920:   is_ok = vol_ratio >= 20
        elif 921 <= int(current_time) <= 930:   is_ok = vol_ratio >= 25
        elif int(current_time) < 1000:          is_ok = vol_ratio >= 50
        elif 1500 <= int(current_time) <= 1530: is_ok = vol_ratio >= 25
        else: is_ok = True
    return is_ok


# ─────────────────────────────────────────
# 알림 키 (trading_trail_simul 에서 읽고 씀)
# ─────────────────────────────────────────
def _read_alert_keys_db(conn, acct_no, stock_code, trail_day, trail_dtm):
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT COALESCE(last_alert_keys, '{{}}')
            FROM {SIMUL_TABLE}
            WHERE acct_no = %s AND code = %s AND trail_day = %s AND trail_dtm = %s
            LIMIT 1
        """, (acct_no, stock_code, trail_day, trail_dtm))
        row = cur.fetchone()
        cur.close()
        return row[0] if row else {}
    except Exception as e:
        print(f"알림 상태 조회 실패: {e}")
        return {}


def _write_alert_key_db(conn, acct_no, stock_code, trail_day, trail_dtm, key_name, key_value):
    try:
        cur = conn.cursor()
        cur.execute(f"""
            UPDATE {SIMUL_TABLE}
            SET last_alert_keys = COALESCE(last_alert_keys, '{{}}') || %s::jsonb
            WHERE acct_no = %s AND code = %s AND trail_day = %s AND trail_dtm = %s
        """, (json.dumps({key_name: key_value}), acct_no, stock_code, trail_day, trail_dtm))
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"알림 상태 저장 실패: {e}")


# ─────────────────────────────────────────
# 시장 흐름 매도 헬퍼
# ─────────────────────────────────────────
def _get_dly_mkt_trend(trade_date: str, conn) -> dict | None:
    """dly_acct_balance 에서 시장 흐름 지표 조회."""
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT kospi_short, kosdak_short, kospi_mid, kosdak_mid,
                   kospi_long,  kosdak_long,  market_ratio, dnca_tot_amt
            FROM public.dly_acct_balance_simul
            WHERE dt = %s AND acct = '74346047'
        """, (trade_date,))
        row = cur.fetchone()
        cur.close()
        if row:
            return {
                'kospi_short': row[0], 'kosdak_short': row[1],
                'kospi_mid':   row[2], 'kosdak_mid':   row[3],
                'kospi_long':  row[4], 'kosdak_long':  row[5],
                'market_ratio': row[6], 'dnca_tot_amt': row[7],
            }
    except Exception as e:
        print(f"[시뮬] dly_acct_balance_simul 조회 오류: {e}")
    return None


def _calc_invest_ratio(mkt_data: dict, market_type: str) -> int:
    """시장 중기·장기 흐름 조합으로 투자가능비율(%) 반환.
    '03'=중기상승, '04'=중기하락, '05'=장기상승, '06'=장기하락.
    """
    if not mkt_data:
        return 100
    if market_type == 'KOSPI':
        mid, long_ = mkt_data.get('kospi_mid', ''), mkt_data.get('kospi_long', '')
    else:
        mid, long_ = mkt_data.get('kosdak_mid', ''), mkt_data.get('kosdak_long', '')
    if   mid == '03' and long_ == '05': return 100  # 중기↑ 장기↑
    elif mid == '03' and long_ == '06': return  50  # 중기↑ 장기↓
    elif mid == '04' and long_ == '05': return  70  # 중기↓ 장기↑
    elif mid == '04' and long_ == '06': return  30  # 중기↓ 장기↓
    return 100


def _get_total_invested(trail_day: str, conn) -> int:
    """trail_tp IN ('1','2','3','L') 기준 총 투자금액(basic_amt 합계) 조회."""
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT COALESCE(SUM(basic_amt), 0)
            FROM {SIMUL_TABLE}
            WHERE acct_no = 'SIMUL' AND trail_day = %s
              AND trail_tp IN ('1','2','3','L')
        """, (trail_day,))
        row = cur.fetchone()
        cur.close()
        return int(row[0]) if row else 0
    except Exception as e:
        print(f"[시뮬] 총투자금액 조회 오류: {e}")
        return 0


def _get_stock_market_type(stock_code: str, access_token: str,
                           app_key: str, app_secret: str) -> str:
    """종목코드의 시장구분 반환 (KOSPI/KOSDAQ). 모듈 캐시 우선."""
    with _stock_market_cache_lock:
        if stock_code in _stock_market_cache:
            return _stock_market_cache[stock_code]
    try:
        res = requests.get(
            f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price",
            headers={
                "Content-Type": "application/json",
                "authorization": f"Bearer {access_token}",
                "appkey": app_key, "appsecret": app_secret,
                "tr_id": "FHKST01010100", "custtype": "P",
            },
            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": stock_code},
            verify=False, timeout=10
        )
        d = res.json()
        if d.get('rt_cd') == '0' and d.get('output'):
            mkt_name = d['output'].get('rprs_mrkt_kor_name', '')
            mkt_upper = mkt_name.upper()
            # 영문: KOSPI, KOSPI200 → KOSPI / KSQ150, KOSDAQ → KOSDAQ
            # 한글: 코스피, 코스피200 → KOSPI / 코스닥, KSQ150 → KOSDAQ
            mkt = 'KOSPI' if ('KOSPI' in mkt_upper or '코스피' in mkt_name) else 'KOSDAQ'
            with _stock_market_cache_lock:
                _stock_market_cache[stock_code] = mkt
            return mkt
    except Exception:
        pass
    return 'KOSPI'


def _calc_peak_trough_trend(highs: list, closes: list, lows: list, dates: list) -> dict | None:
    """일봉 고가/저가 리스트(날짜 오름차순) 기준 지그재그 고점/저점으로 현재 추세와 그 시작일 계산.
    simul_server.py stock_trend() / _calc_peak_trough_trend() 동일 로직.
    고점: 전일 대비 상승 + 익일 대비 하락. 저점: 전일 대비 하락 + 익일 대비 상승.
    추세: 마지막 고점 재돌파 → Uptrend, 마지막 저점 재이탈 → Downtrend, 그 외 → Sideways."""
    n = len(closes)
    if n < 3:
        return None

    high_pts = [None] * n
    low_pts  = [None] * n
    for i in range(1, n - 1):
        if highs[i] > highs[i - 1] and highs[i] > highs[i + 1]:
            high_pts[i] = highs[i]
        if lows[i] < lows[i - 1] and lows[i] < lows[i + 1]:
            low_pts[i] = lows[i]

    trends = []
    last_high, last_low = None, None
    for i in range(n):
        if high_pts[i] is not None:
            last_high = high_pts[i]
        if low_pts[i] is not None:
            last_low = low_pts[i]
        if last_high is not None and highs[i] > last_high:
            trends.append('Uptrend')
        elif last_low is not None and lows[i] < last_low:
            trends.append('Downtrend')
        else:
            trends.append('Sideways')

    cur_trend = trends[-1]
    start_idx = n - 1
    while start_idx > 0 and trends[start_idx - 1] == cur_trend:
        start_idx -= 1
    # 추세전환 기준가: Downtrend → 이탈 기준이 된 저점, Uptrend → 돌파 기준이 된 고점
    if cur_trend == 'Downtrend':
        ref_price = last_low
    elif cur_trend == 'Uptrend':
        ref_price = last_high
    else:
        ref_price = None
    return {'trend': cur_trend, 'start_date': dates[start_idx], 'ref_price': ref_price}


def get_stock_trend(stock_code: str, trade_date: str, access_token: str, app_key: str, app_secret: str) -> dict | None:
    """종목의 현재 추세(Uptrend/Downtrend/Sideways) 조회 (지그재그 고점/저점 기준, simul_server.py 동일 로직)."""
    daily = get_kis_daily_chart_full(stock_code, trade_date, access_token, app_key, app_secret)
    if not daily:
        return None
    dates  = [d["date"]        for d in daily]
    highs  = [d["high_price"]  for d in daily]
    lows   = [d["low_price"]   for d in daily]
    closes = [d["close_price"] for d in daily]
    return _calc_peak_trough_trend(highs, closes, lows, dates)


# ─────────────────────────────────────────
# 시뮬레이션 DB 업데이트 함수 (실제 주문 없음)
# ─────────────────────────────────────────
def update_simul_trail(stop_price, target_price, volumn, acct_no, code,
                       trail_day, trail_dtm, trail_tp, proc_min, conn):
    """안전마진 돌파 / 기준봉 갱신 시 추적 상태 업데이트 (trading_trail_simul)."""
    cur = conn.cursor()
    cur.execute(f"""
        UPDATE {SIMUL_TABLE} SET
            stop_price   = %s,
            target_price = %s,
            volumn       = %s,
            trail_tp     = %s,
            proc_min     = %s,
            mod_dt       = %s
        WHERE acct_no = %s AND code = %s
          AND trail_day = %s AND trail_dtm = %s
          AND trail_tp IN ('1','2')
    """, (stop_price, target_price, volumn, trail_tp, proc_min,
          datetime.now(), acct_no, code, trail_day, trail_dtm))
    conn.commit()
    cur.close()


def _do_simul_sell_update(nick, trail_price, trail_qty, trail_amt, trail_rate,
                          trail_plan, basic_qty, basic_amt,
                          acct_no, code, name, trail_day, trail_dtm,
                          new_trail_tp, proc_min, trade_result, conn,
                          where_tp_cond: str):
    """공통 시뮬레이션 매도 DB 업데이트."""
    now = datetime.now()
    trail_price  = int(trail_price)
    trail_qty    = int(trail_qty)
    trail_amt    = int(trail_amt)
    trail_rate   = float(trail_rate)
    basic_qty    = int(basic_qty)
    basic_amt    = int(basic_amt)
    trade_result = (trade_result or '')[:100]
    try:
        cur = conn.cursor()
        cur.execute(f"""
            UPDATE {SIMUL_TABLE} SET
                trail_price  = %s,
                trail_qty    = %s,
                trail_amt    = %s,
                trail_rate   = %s,
                trail_plan   = %s,
                trail_tp     = %s,
                proc_min     = %s,
                basic_qty    = %s,
                basic_amt    = %s,
                trade_result = %s,
                mod_dt       = %s
            WHERE acct_no = %s AND code = %s
              AND trail_day = %s AND trail_dtm = %s
              AND {where_tp_cond}
        """, (
            trail_price, trail_qty, trail_amt, trail_rate,
            trail_plan, new_trail_tp, proc_min,
            basic_qty, basic_amt, trade_result, now,
            acct_no, code, trail_day, trail_dtm,
        ))
        updated = cur.rowcount > 0
        conn.commit()
        cur.close()
        if updated:
            print(f"[시뮬]-{nick}-[{name}({code})] {trade_result}"
                  f" 시뮬매도가:{int(trail_price):,}원 {int(trail_qty):,}주"
                  f" 수익률:{trail_rate}%")
        return updated
    except Exception as e:
        print(f"[시뮬] _do_simul_sell_update 오류 [{code}]: {e}")
        return False


def update_simul_daily_close(nick, trail_price, trail_qty, trail_amt, trail_rate,
                             trail_plan, basic_qty, basic_amt,
                             acct_no, 
                             code, name, trail_day, trail_dtm, trail_tp,
                             proc_min, trade_result, conn):
    """전일저가 이탈 매도 시뮬레이션 (trail_tp='L' 대상)."""
    return _do_simul_sell_update(
        nick, trail_price, trail_qty, trail_amt, trail_rate,
        trail_plan, basic_qty, basic_amt,
        acct_no, code, name, trail_day, trail_dtm,
        trail_tp, proc_min, trade_result, conn,
        where_tp_cond="trail_tp = 'L'"
    )


def update_simul_close(nick, trail_price, trail_qty, trail_amt, trail_rate,
                       trail_plan, basic_qty, basic_amt,
                       acct_no, 
                       code, name, trail_day, trail_dtm, trail_tp,
                       proc_min, trade_result, conn):
    """이탈가/안전마진 매도 시뮬레이션 (trail_tp='1','2' 대상)."""
    return _do_simul_sell_update(
        nick, trail_price, trail_qty, trail_amt, trail_rate,
        trail_plan, basic_qty, basic_amt,
        acct_no, code, name, trail_day, trail_dtm,
        trail_tp, proc_min, trade_result, conn,
        where_tp_cond="trail_tp IN ('1','2')"
    )


# ─────────────────────────────────────────
# 핵심 시뮬레이션 로직 (원본 get_kis_1min_from_datetime 동일 구조)
# ─────────────────────────────────────────
def get_kis_1min_from_datetime_simul(
    nick, stock_code, stock_name,
    start_date, start_time,
    target_price, stop_price,
    basic_price, basic_qty,
    trail_tp, trail_plan, proc_min, volumn, trade_tp, exit_price,
    access_token, app_key, app_secret,
    acct_no, conn,
    bot=None, chat_id=None,
    breakout_type="high",
    breakdown_type="low",
    verbose=True,
):
    start_dt      = datetime.strptime(start_date + start_time, "%Y%m%d%H%M%S")
    loop_start_dt = get_next_completed_10min_dt(start_dt)
    trade_date    = start_dt.strftime("%Y%m%d")
    _alert_keys   = _read_alert_keys_db(conn, acct_no, stock_code, start_date, start_time)
    signals       = []

    if verbose:
        print(f"[시뮬]-{nick}-[{stock_name}-{stock_code}] {trade_date} {datetime.now().strftime('%H%M%S')} 분봉 처리 중")

    prev_day_info = get_prev_day_info(stock_code, trade_date, access_token, app_key, app_secret, conn)
    if prev_day_info is None:
        print(f"[시뮬] [{stock_name}-{stock_code}] 전일 일봉 미존재")
        return signals

    prev_volume = prev_day_info['volume']
    prev_close  = prev_day_info['close_price']
    upper_limit = get_valid_sell_price(int(prev_close * 1.30))
    # 시장 흐름 사전 조회 (_get_dly_mkt_trend → 루프 내 중복 호출 방지)
    _stk_mkt_pre    = _get_stock_market_type(stock_code, access_token, app_key, app_secret)
    _mkt_trend_pre  = _get_dly_mkt_trend(trade_date, conn)
    _short_market_down = False
    if _mkt_trend_pre:
        _short_key         = 'kospi_short' if _stk_mkt_pre == 'KOSPI' else 'kosdak_short'
        _short_val         = _mkt_trend_pre.get(_short_key, '')
        _short_market_down = (_short_val == '02')
        if verbose and _short_market_down:
            print(f"[시뮬] {stock_name}[{stock_code}] {_stk_mkt_pre} 단기하락({_short_val}) → 이탈감지 강화")

    # ── L 유형 (장기보유 이탈가 이탈 감시) ──────────────────────────────
    if trail_tp == 'L':
        _start_time = "163000" if trade_date.endswith("1119") else "153000"
        df = get_kis_1min_full_day(stock_code, trade_date, _start_time,
                                   access_token, app_key, app_secret, verbose=False)
        if df.empty:
            print(f"⚠️ [{stock_name}-{stock_code}] 분봉 없음")
            return signals

        _stock_trend_pre  = get_stock_trend(stock_code, trade_date, access_token, app_key, app_secret)
        _trend_up         = bool(_stock_trend_pre and _stock_trend_pre.get('trend') == 'Uptrend')
        _trend_down       = bool(_stock_trend_pre and _stock_trend_pre.get('trend') == 'Downtrend')
        _trend_ref_price  = (_stock_trend_pre.get('ref_price') or 0) if _stock_trend_pre else 0
        if verbose and _trend_up:
            print(f"{stock_name}[{stock_code}] 현재 상승추세({_stock_trend_pre.get('start_date')}~, 기준가:{_trend_ref_price:,}) → 추세기준 감지")
        elif verbose and _trend_down:
            print(f"[시뮬] {stock_name}[{stock_code}] 현재 하락추세({_stock_trend_pre.get('start_date')}~, 기준가:{_trend_ref_price:,}) → 추세기준 감지")

        _df_tv = df.copy()
        _df_tv["거래량"] = _df_tv["거래량"].astype(int)
        _df_tv["_tk"]   = _df_tv["dt"].apply(get_10min_key)
        tenmin_vol_ser  = (
            _df_tv.groupby("_tk")["거래량"].sum()
            .reset_index()
            .rename(columns={"_tk": "tenmin_key", "거래량": "tenmin_vol"})
            .sort_values("tenmin_key")
            .reset_index(drop=True)
        )

        def _is_tenmin_vol_surge(key, n=20, mult=2.0):
            rows = tenmin_vol_ser[tenmin_vol_ser["tenmin_key"] == key]
            if rows.empty: return False, 0, 0
            idx      = rows.index[0]
            prev_ser = tenmin_vol_ser.iloc[max(0, idx - n):idx]["tenmin_vol"]
            if prev_ser.empty: return False, 0, 0
            cur_vol  = int(rows.iloc[0]["tenmin_vol"])
            avg_vol  = int(prev_ser.mean())
            return cur_vol >= avg_vol * mult, cur_vol, avg_vol

        df = df[df["dt"] >= loop_start_dt]
        start_t = dt_time(10, 0) if trade_date.endswith("0102") else dt_time(9, 0)
        df = df[(df["dt"].dt.time >= start_t) & (df["dt"].dt.time <= dt_time(15, 30))]
        df = df.sort_values("dt").reset_index(drop=True)

        peak_high_tenmin          = basic_price
        tenmin_completed_key_last = None
        order_price               = 0
        has_reached_15pct         = False
        breakdown_wait = {
            "active": False, "tenmin_key": None, "tenmin_low": None,
            "tenmin_vol_ok": None, "tenmin_vol": 0, "tenmin_avg_vol": 0,
            "reason": "", "signal_type": "", "effective_stop": 0, "order_price": 0,
        }
        downtrend_warn_last_key    = _alert_keys.get("downtrend_warn")
        breakdown_notify_last_key  = _alert_keys.get("L")
        _market_sell_checked       = False

        for _, row in df.iterrows():
            if int(proc_min) < int(row['시간'].replace(':', '') + '00'):
                sell_trigger = False; sell_reason = ""; sell_signal_type = ""
                high_price   = int(row["고가"])
                low_price    = int(row["저가"])
                close_price  = int(row["종가"])
                acml_vol     = int(row["누적거래량"])
                breakout_check  = high_price  if breakout_type  == "high"  else close_price
                breakdown_check = low_price   if breakdown_type == "low"   else close_price
                _late_open   = trade_date.endswith("0102") or trade_date.endswith("1119")
                if _late_open and row["dt"].time() < datetime.strptime("10:10", "%H:%M").time():
                    continue
                elif not _late_open and row["dt"].time() < datetime.strptime("09:10", "%H:%M").time():
                    continue
                current_time = row["시간"].replace(":", "")
                vol_ratio    = (acml_vol / prev_volume) * 100 if prev_volume > 0 else 0
                gain_pct     = ((close_price - basic_price) / basic_price) * 100 if basic_price > 0 else 0

                if not has_reached_15pct and prev_close and int(prev_close) > 0:
                    if high_price >= int(prev_close * 1.15):
                        has_reached_15pct = True

                # 상승추세 아닌 추세이탈가 즉시 매도
                if not _trend_up and exit_price and int(exit_price) > 0 and close_price <= int(exit_price):
                    _ep         = int(exit_price)
                    i_trail_plan = trail_plan if trail_plan else "100"
                    trail_qty   = int(basic_qty * int(i_trail_plan) * 0.01)
                    trail_amt   = _ep * trail_qty
                    trail_rate  = round((100 - (_ep / basic_price) * 100) * -1, 2) if basic_price > 0 else 0
                    u_basic_qty = basic_qty - trail_qty
                    u_basic_amt = basic_price * u_basic_qty
                    result = update_simul_daily_close(
                        nick, _ep, trail_qty, trail_amt, trail_rate, i_trail_plan,
                        u_basic_qty, u_basic_amt, acct_no,
                        stock_code, stock_name, start_date, start_time, "4",
                        row['시간'].replace(':', '') + '00',
                        f"추세이탈가({_ep:,})원 이탈 즉시 매도", conn
                    )
                    signals.append({"signal_type": "EXIT_PRICE_IMMEDIATE",
                                    "종목코드": stock_code, "발생일자": row["일자"],
                                    "발생시간": row["시간"], "매도가": _ep, "수익률": gain_pct})
                    return signals

                current_10min_key = get_10min_key(row["dt"])
                if breakdown_wait["active"]:
                    if current_10min_key != breakdown_wait["tenmin_key"]:
                        if breakdown_wait["tenmin_low"] is None:
                            trigger_key  = breakdown_wait["tenmin_key"]
                            trigger_bars = df[df["dt"].apply(get_10min_key) == trigger_key]
                            if not trigger_bars.empty:
                                breakdown_wait["tenmin_low"] = trigger_bars["저가"].astype(int).min()
                                vol_ok, cur_vol, avg_vol = _is_tenmin_vol_surge(trigger_key)
                                breakdown_wait["tenmin_vol_ok"]    = vol_ok
                                breakdown_wait["tenmin_vol"]       = cur_vol
                                breakdown_wait["tenmin_avg_vol"]   = avg_vol
                                if not vol_ok:
                                    breakdown_wait.update({"active": False, "tenmin_key": None,
                                                           "tenmin_low": None, "tenmin_vol_ok": None,
                                                           "tenmin_vol": 0, "tenmin_avg_vol": 0,
                                                           "reason": "", "signal_type": "",
                                                           "effective_stop": 0, "order_price": 0})
                    if breakdown_wait["active"] and breakdown_wait["tenmin_low"] is not None:
                        if not _trend_up and low_price < breakdown_wait["tenmin_low"]:
                            sell_trigger     = True
                            sell_reason      = (breakdown_wait["reason"]
                                                + f" → 10분봉저가({breakdown_wait['tenmin_low']:,}) 이탈 확정")
                            sell_signal_type = breakdown_wait["signal_type"]
                            effective_stop   = breakdown_wait["effective_stop"]
                            # 해당 종목의 시장이 단기 하락인 경우 : 매도주문가 = 현재가
                            if _short_market_down:
                                order_price = close_price
                            else:   # 해당 종목의 시장이 단기 상승인 경우 : 매도주문가 = 현재가가 이탈가 아래면 이탈가 otherwise 현재가
                                order_price = effective_stop if close_price < effective_stop else close_price
                    else:
                        sell_trigger = False

                if not breakdown_wait["active"] and not sell_trigger:
                    if has_reached_15pct:
                        current_10min_key_b = get_10min_key(row["dt"])
                        tenmin_df_b = df[df["dt"].apply(get_10min_key) == current_10min_key_b]
                        is_last_of_tenmin = (not tenmin_df_b.empty
                                             and row["dt"] == tenmin_df_b["dt"].max())
                        if is_last_of_tenmin and current_10min_key_b != tenmin_completed_key_last:
                            tenmin_completed_key_last = current_10min_key_b
                            tenmin_high_b             = int(tenmin_df_b["고가"].astype(int).max())
                            tenmin_close_b            = close_price
                            peak_high_tenmin          = int(max(peak_high_tenmin, tenmin_high_b))
                            safety_margin_L           = int(basic_price * 1.10)
                            peak_to_safety_L          = peak_high_tenmin - safety_margin_L
                            effective_retracement_rate_L = 0.3 if row["dt"].time() >= dt_time(14, 30) else 0.5
                            if not _trend_up and (peak_high_tenmin > safety_margin_L
                                    and peak_to_safety_L >= int(safety_margin_L * 0.05)):
                                peak_sell_threshold_L = peak_high_tenmin - int(peak_to_safety_L * effective_retracement_rate_L)
                                if tenmin_close_b < peak_sell_threshold_L:
                                    sell_trigger     = True
                                    sell_price_b     = get_valid_sell_price(max(tenmin_close_b, peak_sell_threshold_L))
                                    sell_reason      = f"고점({peak_high_tenmin:,})원 되돌림 임계({peak_sell_threshold_L:,})원 종가 이탈 (매도가:{sell_price_b:,})"
                                    sell_signal_type = "PEAK_RETRACEMENT_B"
                                    order_price      = sell_price_b
                        if not sell_trigger:
                            _safety_m = int(basic_price * 1.10)
                            _p2s      = peak_high_tenmin - _safety_m
                            _cond_b_capable = peak_high_tenmin > _safety_m and _p2s >= int(_safety_m * 0.05)
                            if not _trend_up and not _cond_b_capable:
                                fixed_stop = int(stop_price) if stop_price else 0
                                if fixed_stop > 0 and close_price <= fixed_stop and volume_rate_chk(current_time, vol_ratio, trade_date):
                                     # 시장 단기 하락인 경우 매도 진행
                                    if _short_market_down:
                                        breakdown_wait.update({"active": True, "tenmin_key": current_10min_key,
                                                           "tenmin_low": None, "tenmin_vol_ok": None,
                                                           "reason": f"이탈가({fixed_stop:,})원 이탈 [15%달성·수익률:{gain_pct:.1f}%]",
                                                           "signal_type": "FIXED_STOP",
                                                           "effective_stop": fixed_stop, "order_price": close_price})
                                        sell_trigger = True
                                        sell_reason      = f"이탈가({fixed_stop:,})원 이탈 [15%달성·수익률:{gain_pct:.1f}%] (매도가:{close_price:,})"
                                        sell_signal_type = "FIXED_STOP"
                                        order_price      = close_price

                                        if verbose and breakdown_notify_last_key is None:
                                            breakdown_notify_last_key = current_10min_key.strftime("%Y%m%d%H%M")
                                            _write_alert_key_db(conn, acct_no, stock_code, start_date, start_time, "L", breakdown_notify_last_key)
                                            print(f"[시뮬]-{nick}-[{row['일자']}-{row['시간']}]{stock_name}[{stock_code}] 15%달성·수익률:{gain_pct:.1f}%, 이탈가({fixed_stop:,}) 이탈 [시장 약세] 매도 진행")
                                    else:
                                        breakdown_wait.update({"active": True, "tenmin_key": current_10min_key,
                                                           "tenmin_low": None, "tenmin_vol_ok": None,
                                                           "reason": f"이탈가({fixed_stop:,})원 이탈 [15%달성·수익률:{gain_pct:.1f}%]",
                                                           "signal_type": "FIXED_STOP",
                                                           "effective_stop": fixed_stop, "order_price": 0})
                                        if verbose and breakdown_notify_last_key is None:
                                            breakdown_notify_last_key = current_10min_key.strftime("%Y%m%d%H%M")
                                            _write_alert_key_db(conn, acct_no, stock_code, start_date, start_time, "L", breakdown_notify_last_key)
                                            print(f"[시뮬]-{nick}-[{row['일자']}-{row['시간']}]{stock_name}[{stock_code}] 15%달성·수익률:{gain_pct:.1f}%, 이탈가({fixed_stop:,}) 이탈 → 10분봉 저가 대기")
                    else:
                        fixed_stop = int(stop_price) if stop_price else 0
                        if not _trend_up and fixed_stop > 0 and close_price <= fixed_stop and volume_rate_chk(current_time, vol_ratio, trade_date):
                            # 시장 단기 하락인 경우 매도 진행
                            if _short_market_down:
                                breakdown_wait.update({"active": True, "tenmin_key": current_10min_key,
                                                   "tenmin_low": None, "tenmin_vol_ok": None,
                                                   "reason": f"이탈가({fixed_stop:,})원 이탈 [수익률:{gain_pct:.1f}%]",
                                                   "signal_type": "FIXED_STOP",
                                                   "effective_stop": fixed_stop, "order_price": close_price})
                                sell_trigger = True
                                sell_reason      = f"이탈가({fixed_stop:,})원 이탈 [수익률:{gain_pct:.1f}%] (매도가:{close_price:,})"
                                sell_signal_type = "FIXED_STOP"
                                order_price      = close_price
                                if verbose and breakdown_notify_last_key is None:
                                    breakdown_notify_last_key = current_10min_key.strftime("%Y%m%d%H%M")
                                    _write_alert_key_db(conn, acct_no, stock_code, start_date, start_time, "L", breakdown_notify_last_key)
                                    print(f"[시뮬]-{nick}-[{row['일자']}-{row['시간']}]{stock_name}[{stock_code}] 수익률:{gain_pct:.1f}%, 이탈가({fixed_stop:,}) 이탈 [시장 약세] 매도 진행")
                            else:
                                breakdown_wait.update({"active": True, "tenmin_key": current_10min_key,
                                                   "tenmin_low": None, "tenmin_vol_ok": None,
                                                   "reason": f"이탈가({fixed_stop:,})원 이탈 [수익률:{gain_pct:.1f}%]",
                                                   "signal_type": "FIXED_STOP",
                                                   "effective_stop": fixed_stop, "order_price": 0})                                
                                if verbose and breakdown_notify_last_key is None:
                                    breakdown_notify_last_key = current_10min_key.strftime("%Y%m%d%H%M")
                                    _write_alert_key_db(conn, acct_no, stock_code, start_date, start_time, "L", breakdown_notify_last_key)
                                    print(f"[시뮬]-{nick}-[{row['일자']}-{row['시간']}]{stock_name}[{stock_code}] 수익률:{gain_pct:.1f}%, 이탈가({fixed_stop:,}) 이탈 감지 → 10분봉 저가 대기")        

                # 하락추세 이탈 사전 경고
                _downtrend_start   = "101000" if (trade_date.endswith("0102") or trade_date.endswith("1119")) else "091000"
                _downtrend_warn_end = "161000" if trade_date.endswith("1119") else "151000"
                if (current_time >= _downtrend_start and current_time < _downtrend_warn_end and _trend_down and close_price < _trend_ref_price):                        
                    _cur_key_str = current_10min_key.strftime("%Y%m%d%H%M")
                    if downtrend_warn_last_key is None:
                        downtrend_warn_last_key = _cur_key_str
                        _write_alert_key_db(conn, acct_no, stock_code, start_date, start_time, "downtrend_warn", downtrend_warn_last_key)
                        msg_warn = (
                            f"[시뮬]-{nick}-[{row['일자']}-{row['시간']}]{stock_name}[<code>{stock_code}</code>]"
                            f" [사전경고] 종가:{close_price:,}원 하락추세:{_trend_ref_price:,}원 이탈"
                        )
                        print(msg_warn)                            
                        # _w_mkt_data = _mkt_trend_pre
                        # # 해당 종목의 코스피 또는 코스닥의 단기 하락
                        # if _w_mkt_data:
                        #     _w_stk_mkt       = _stk_mkt_pre
                        #     _w_allow_ratio   = _w_mkt_data['market_ratio']
                        #     _w_total_invested = _get_total_invested(trade_date, conn)
                        #     _w_dnca_tot       = _w_mkt_data.get('dnca_tot_amt') or 0
                        #     _w_allowed_invest = int(_w_dnca_tot * _w_allow_ratio / 100)
                        #     _w_excess_invest  = _w_total_invested - _w_allowed_invest
                        #     _w_invest_pct     = round(_w_total_invested / _w_dnca_tot * 100, 1) if _w_dnca_tot > 0 else 0
                        #     _w_mid_key   = 'kospi_mid'  if _w_stk_mkt == 'KOSPI' else 'kosdak_mid'
                        #     _w_long_key  = 'kospi_long' if _w_stk_mkt == 'KOSPI' else 'kosdak_long'
                        #     _w_mid_str   = '상승' if _w_mkt_data.get(_w_mid_key)  == '03' else '하락'
                        #     _w_long_str  = '상승' if _w_mkt_data.get(_w_long_key) == '05' else '하락'

                        #     # 트레이딩 매입금액이 시징비율허용된 금액보다 큰 경우
                        #     if _w_excess_invest > 0 and close_price > 0:
                        #         _w_qty      = min(int(basic_qty), max(1, (_w_excess_invest + close_price - 1) // close_price))
                        #         _w_plan     = round(_w_qty / basic_qty * 100) if basic_qty > 0 else 100
                        #         _w_reason   = (f" [사전경고] 현재가:{close_price:,}원 하락추세:{_trend_ref_price:,}원 이탈"
                        #                        f" 시장흐름[{_w_stk_mkt}] 중기:{_w_mid_str}/장기:{_w_long_str}"
                        #                        f" 시장허용:{_w_allow_ratio}%({_w_allowed_invest:,}원)"
                        #                        f" 현재진행:{_w_invest_pct}%({_w_total_invested:,}원)"
                        #                        f" 초과:{_w_excess_invest:,}원→{_w_plan}% 매도 필요")
                        #         print(f"[시뮬]-{nick}-[{row['일자']}-{row['시간']}]{stock_name}[{stock_code}] {_w_reason}")

                # 15:10 이후 하락 추세 매도
                _downtrend_cutoff = "161000" if trade_date.endswith("1119") else "151000"
                if (not breakdown_wait["active"] and not sell_trigger and current_time >= _downtrend_cutoff and _trend_down and close_price < _trend_ref_price and not _market_sell_checked):
                    _market_sell_checked = True

                    _mkt_reason = f"[장종료전] 종가:{close_price:,}원 하락추세:{_trend_ref_price:,}원 이탈"
                    print(f"[시뮬]-{nick}-[{row['일자']}-{row['시간']}]{stock_name}[{stock_code}] {_mkt_reason}")
                    
                    update_simul_daily_close(
                        nick, close_price, basic_qty, close_price * basic_qty, round((close_price / basic_price - 1) * 100, 2) if basic_price > 0 else 0,
                        "100", basic_qty, close_price * basic_qty, acct_no,
                        stock_code, stock_name, start_date, start_time, "4",
                        row['시간'].replace(':', '') + '00', _mkt_reason, conn
                    )
                    signals.append({
                        "signal_type": "DOWNTREND_SELL",
                        "종목코드": stock_code,
                        "발생일자": row["일자"],
                        "발생시간": row["시간"],
                        "매도수량": basic_qty,
                        "매도가격": close_price,
                    })
                    return signals
                    # _mkt_data = _mkt_trend_pre
                    # # 해당 종목의 코스피 또는 코스닥의 단기 하락
                    # if _mkt_data:
                    #     _stk_mkt        = _stk_mkt_pre
                    #     _allow_ratio    = _mkt_data['market_ratio']
                    #     _total_invested = _get_total_invested(trade_date, conn)
                    #     _dnca_tot       = _mkt_data.get('dnca_tot_amt') or 0
                    #     _allowed_invest = int(_dnca_tot * _allow_ratio / 100)
                    #     _excess_invest  = _total_invested - _allowed_invest
                    #     _invest_pct     = round(_total_invested / _dnca_tot * 100, 1) if _dnca_tot > 0 else 0
                    #     _mid_key        = 'kospi_mid'  if _stk_mkt == 'KOSPI' else 'kosdak_mid'
                    #     _long_key       = 'kospi_long' if _stk_mkt == 'KOSPI' else 'kosdak_long'
                    #     _mid_str        = '상승' if _mkt_data.get(_mid_key)  == '03' else '하락'
                    #     _long_str       = '상승' if _mkt_data.get(_long_key) == '05' else '하락'

                    #     # 트레이딩 매입금액이 시징비율허용된 금액보다 큰 경우
                    #     if _excess_invest > 0 and close_price > 0:
                    #         # 초과금액 해소에 필요한 수량 (올림 나눗셈, 보유수량 상한)
                    #         _mkt_qty    = min(int(basic_qty), max(1, (_excess_invest + close_price - 1) // close_price))
                    #         _trail_plan = round(_mkt_qty / basic_qty * 100) if basic_qty > 0 else 100
                    #         _mkt_amt    = close_price * _mkt_qty
                    #         _mkt_rate   = round((close_price / basic_price - 1) * 100, 2) if basic_price > 0 else 0
                    #         _u_qty      = basic_qty - _mkt_qty
                    #         _u_amt      = basic_price * _u_qty
                    #         _new_tp     = "4" if _mkt_qty >= basic_qty else "L"
                    #         _mkt_reason = (f" [장종료전 매도] 현재가:{close_price:,}원 하락추세:{_trend_ref_price:,}원 이탈"
                    #                        f" 시장흐름[{_stk_mkt}] 중기:{_mid_str}/장기:{_long_str}"
                    #                        f" 시장허용:{_allow_ratio}%({_allowed_invest:,}원)"
                    #                        f" 현재진행:{_invest_pct}%({_total_invested:,}원)"
                    #                        f" 초과:{_excess_invest:,}원→{_trail_plan}% 매도 진행")
                    #         print(f"[시뮬]-{nick}-[{row['일자']}-{row['시간']}]{stock_name}[{stock_code}] {_mkt_reason}")
                    #         print(f" 현재가:{close_price:,}원 | 매도:{_mkt_qty:,}주 | 잔여:{_u_qty:,}주 | 수익률:{_mkt_rate:+.2f}%")
                    #         update_simul_daily_close(
                    #             nick, close_price, _mkt_qty, _mkt_amt, _mkt_rate,
                    #             str(_trail_plan), _u_qty, _u_amt, acct_no,
                    #             stock_code, stock_name, start_date, start_time, _new_tp,
                    #             row['시간'].replace(':', '') + '00', _mkt_reason, conn
                    #         )
                    #         signals.append({
                    #             "signal_type":  "MARKET_TREND_SELL",
                    #             "종목코드":      stock_code, "발생일자": row["일자"],
                    #             "발생시간":      row["시간"], "시장": _stk_mkt,
                    #             "허용비율":      _allow_ratio,
                    #             "현재투자비율":  _invest_pct,
                    #             "매도수량":      int(_mkt_qty), "매도가격": close_price,
                    #         })
                    #         return signals

                # 매도 실행
                if sell_trigger:
                    breakdown_wait["active"] = False
                    i_trail_plan = trail_plan if trail_plan else "100"
                    trail_qty    = int(basic_qty * int(i_trail_plan) * 0.01)
                    trail_amt    = order_price * trail_qty
                    trail_rate   = round((100 - (order_price / basic_price) * 100) * -1, 2) if basic_price > 0 else 0
                    u_basic_qty  = basic_qty - trail_qty
                    u_basic_amt  = basic_price * u_basic_qty
                    update_simul_daily_close(
                        nick, order_price, trail_qty, trail_amt, trail_rate, i_trail_plan,
                        u_basic_qty, u_basic_amt, acct_no, 
                        stock_code, stock_name, start_date, start_time, "4",
                        row['시간'].replace(':', '') + '00', sell_reason, conn
                    )
                    signals.append({"signal_type": sell_signal_type, "종목코드": stock_code,
                                    "발생일자": row["일자"], "발생시간": row["시간"],
                                    "이탈가격": order_price, "수익률": gain_pct})
                    return signals

    # ── trail_tp '1' 또는 '2' (안전마진 추적) ─────────────────────────
    else:
        if trail_tp == '2':
            tenmin_state = {
                "base_low":  int(stop_price)   if stop_price   else 0,
                "base_high": int(target_price) if target_price else 0,
                "base_vol":  int(volumn)       if volumn       else 0,
                "peak_high": int(target_price) if target_price else 0,
                "base_key":  None,
            }
        else:
            tenmin_state = {
                "base_low":  None, "base_high": None, "base_vol":  None,
                "peak_high": 0,    "base_key":  None,
            }

        _start_time = "163000" if trade_date.endswith("1119") else "153000"
        df = get_kis_1min_full_day(stock_code, trade_date, _start_time,
                                   access_token, app_key, app_secret, verbose=False)
        if df.empty:
            print(f"⚠️ [{stock_name}-{stock_code}] 분봉 없음")
            return signals

        df = df[df["dt"] >= loop_start_dt]
        start_t = dt_time(10, 0) if trade_date.endswith("0102") else dt_time(9, 0)
        df = df[(df["dt"].dt.time >= start_t) & (df["dt"].dt.time <= dt_time(15, 30))]
        df = df.sort_values("dt").reset_index(drop=True)

        # trail_tp='1' 이탈 대기 상태
        if trail_tp == '1':
            _df_tv_1 = df.copy()
            _df_tv_1["거래량"] = _df_tv_1["거래량"].astype(int)
            _df_tv_1["_tk"]   = _df_tv_1["dt"].apply(get_10min_key)
            tenmin_vol_ser_1  = (
                _df_tv_1.groupby("_tk")["거래량"].sum()
                .reset_index()
                .rename(columns={"_tk": "tenmin_key", "거래량": "tenmin_vol"})
                .sort_values("tenmin_key")
                .reset_index(drop=True)
            )

            def _is_tenmin_vol_surge_1(key, n=20, mult=2.0):
                rows = tenmin_vol_ser_1[tenmin_vol_ser_1["tenmin_key"] == key]
                if rows.empty: return False, 0, 0
                idx      = rows.index[0]
                prev_ser = tenmin_vol_ser_1.iloc[max(0, idx - n):idx]["tenmin_vol"]
                if prev_ser.empty: return False, 0, 0
                cur_vol  = int(rows.iloc[0]["tenmin_vol"])
                avg_vol  = int(prev_ser.mean())
                return cur_vol >= avg_vol * mult, cur_vol, avg_vol

            _cached_alert_key_1 = _alert_keys.get("1")
            breakdown_wait_1 = {
                "active": False,             # 이탈 감시 활성화 여부
                "tenmin_key": None,          # 이탈 발생 10분봉 키
                "tenmin_low": None,          # 이탈 발생 10분봉 저가 (완성 후 확정)
                "tenmin_vol_ok": None,       # 거래량 조건 충족 여부 (완성 후 확정)
                "tenmin_vol": 0,             # 이탈 발생 10분봉 거래량
                "tenmin_avg_vol": 0,         # 직전 20개 10분봉 평균 거래량
                "sell_label": "",            # 매도 사유 ('손절매도' / '이탈매도')
                "sell_on_candle_close": False,  # True: 10분봉 완성 시점에 즉시 매도 (_short_market_down 케이스)
                "last_alert_tenmin_key": _cached_alert_key_1,  # 스케줄러 재호출 간 유지 (10분 중복 방지)
            }
            _cached_alert_key_2 = _alert_keys.get("2")
            breakdown_wait_2 = {
                "active": False,             # 이탈가 이탈 감지 여부
                "breach_price": 0,           # 이탈된 가격 (stop_price 또는 exit_price)
                "breach_type": "",           # "exit" or "stop"
                "tenmin_key": None,          # 이탈 발생 10분봉 키 (같은 봉 완성 시점에 sell_trigger)
                "last_alert_tenmin_key": _cached_alert_key_2,
            }

        current_10min_key = get_completed_10min_key(datetime.now())

        for _, row in df.iterrows():
            if int(proc_min) < int(row['시간'].replace(':', '') + '00'):
                sell_trigger = False; sell_reason = ""; sell_signal_type = ""
                high_price   = int(row["고가"])
                low_price    = int(row["저가"])
                close_price  = int(row["종가"])
                acml_vol     = int(row["누적거래량"])
                breakout_check  = high_price if breakout_type  == "high"  else close_price
                breakdown_check = low_price  if breakdown_type == "low"   else close_price
                current_time    = row["dt"].time()

                if high_price > low_price:
                    # 상한가 절반 매도
                    if high_price >= upper_limit:
                        i_trail_plan = "50"
                        trail_qty    = max(1, int(basic_qty * 0.5))
                        order_price  = upper_limit
                        trail_rate   = round((100 - (order_price / basic_price) * 100) * -1, 2) if basic_price > 0 else 0
                        trail_amt    = order_price * trail_qty
                        u_basic_qty  = basic_qty - trail_qty
                        u_basic_amt  = basic_price * u_basic_qty
                        update_simul_close(
                            nick, order_price, trail_qty, trail_amt, trail_rate, i_trail_plan,
                            u_basic_qty, u_basic_amt, acct_no, 
                            stock_code, stock_name, start_date, start_time, "3",
                            row['시간'].replace(':', '') + '00', '상한가매도', conn
                        )
                        signals.append({"signal_type": "UPPER_LIMIT", "종목코드": stock_code,
                                        "발생일자": row["일자"], "발생시간": row["시간"],
                                        "상한가": upper_limit})
                        return signals

                    # 기준봉 미생성
                    if tenmin_state["base_low"] is None:
                        chk_vol = volumn if volumn else 0
                        if trail_tp == '1':
                            # breakdown_wait_1 대기 처리
                            if breakdown_wait_1["active"]:
                                current_10min_key_1 = get_10min_key(row["dt"])
                                if current_10min_key_1 != breakdown_wait_1["tenmin_key"]:
                                    # sell_on_candle_close: _short_market_down 이탈가 이탈 → 해당 10분봉 완성 시 즉시 매도
                                    # (tenmin_low 이탈 대기 없이 봉 완성 시점에 현재가로 매도)
                                    if breakdown_wait_1.get("sell_on_candle_close"):
                                        order_price = close_price  # 시장 약세 → 현재가
                                        trail_rate = round((100 - (order_price / basic_price) * 100) * -1, 2) if basic_price > 0 else 0
                                        i_trail_plan = trail_plan if trail_plan else "100"
                                        trail_qty = int(basic_qty * int(i_trail_plan) * 0.01)
                                        trail_amt = order_price * trail_qty
                                        u_basic_qty = basic_qty - trail_qty
                                        u_basic_amt = basic_price * u_basic_qty
                                        sell_label = breakdown_wait_1["sell_label"]
                                        update_simul_close(
                                            nick, order_price, trail_qty, trail_amt, trail_rate, i_trail_plan,
                                            u_basic_qty, u_basic_amt, acct_no, 
                                            stock_code, stock_name, start_date, start_time, "4",
                                            row['시간'].replace(':', '') + '00', sell_label, conn
                                        )
                                        signals.append({
                                            "signal_type": "BREAKDOWN_BEFORE_BREAKOUT",
                                            "종목명": stock_name,
                                            "종목코드": stock_code,
                                            "발생일자": row["일자"],
                                            "발생시간": row["시간"],
                                            "이탈가격": breakdown_check
                                        })
                                        return signals

                                    if breakdown_wait_1["tenmin_low"] is None:
                                        trigger_key_1   = breakdown_wait_1["tenmin_key"]
                                        trigger_bars_1  = df[df["dt"].apply(get_10min_key) == trigger_key_1]
                                        if not trigger_bars_1.empty:
                                            breakdown_wait_1["tenmin_low"]  = trigger_bars_1["저가"].astype(int).min()
                                            vol_ok_1, cur_v1, avg_v1 = _is_tenmin_vol_surge_1(trigger_key_1)
                                            breakdown_wait_1["tenmin_vol_ok"]    = vol_ok_1
                                            breakdown_wait_1["tenmin_vol"]       = cur_v1
                                            breakdown_wait_1["tenmin_avg_vol"]   = avg_v1
                                            if not vol_ok_1:
                                                breakdown_wait_1.update({
                                                    "active": False, "tenmin_key": None,
                                                    "tenmin_low": None, "tenmin_vol_ok": None,
                                                    "tenmin_vol": 0, "tenmin_avg_vol": 0,
                                                    "sell_label": "",
                                                })
                                if breakdown_wait_1["tenmin_low"] is not None and low_price < breakdown_wait_1["tenmin_low"]:
                                    # 해당 종목의 시장이 단기 하락인 경우 : 매도주문가 = 현재가
                                    if _short_market_down:
                                        order_price = close_price
                                    else:   # 해당 종목의 시장이 단기 상승인 경우 : 매도주문가 = 현재가가 이탈가 아래면 이탈가 otherwise 현재가
                                        order_price = int(stop_price) if close_price < int(stop_price) else close_price
                                    trail_rate   = round((100 - (order_price / basic_price) * 100) * -1, 2) if basic_price > 0 else 0
                                    i_trail_plan = trail_plan if trail_plan else "100"
                                    trail_qty    = int(basic_qty * int(i_trail_plan) * 0.01)
                                    trail_amt    = order_price * trail_qty
                                    u_basic_qty  = basic_qty - trail_qty
                                    u_basic_amt  = basic_price * u_basic_qty
                                    sell_label   = breakdown_wait_1["sell_label"]
                                    update_simul_close(
                                        nick, order_price, trail_qty, trail_amt, trail_rate, i_trail_plan,
                                        u_basic_qty, u_basic_amt, acct_no, 
                                        stock_code, stock_name, start_date, start_time, "4",
                                        row['시간'].replace(':', '') + '00', sell_label, conn
                                    )
                                    signals.append({"signal_type": "BREAKDOWN_BEFORE_BREAKOUT",
                                                    "종목코드": stock_code, "발생일자": row["일자"],
                                                    "발생시간": row["시간"], "이탈가격": breakdown_check})
                                    return signals

                            else:
                                if breakdown_check <= exit_price and acml_vol > chk_vol:
                                    current_10min_key_1 = get_10min_key(row["dt"])
                                    breakdown_wait_1.update({"active": True, "tenmin_key": current_10min_key_1,
                                                            "tenmin_low": None, "tenmin_vol_ok": None,
                                                            "sell_label": "최종이탈가 매도"})
                                    
                                    # 시장 단기 하락인 경우 10분봉 완성 후 매도 대기
                                    if _short_market_down:
                                        breakdown_wait_1["sell_on_candle_close"] = True
                                    
                                        _cur_key_str = current_10min_key_1.strftime("%Y%m%d%H%M")
                                        if breakdown_wait_1["last_alert_tenmin_key"] is None:
                                            breakdown_wait_1["last_alert_tenmin_key"] = _cur_key_str
                                            _write_alert_key_db(conn, acct_no, stock_code, start_date, start_time, "1", breakdown_wait_1["last_alert_tenmin_key"])
                                            print(f"[시뮬]-{nick}-[{row['일자']}-{row['시간']}]{stock_name}[{stock_code}] 최종이탈가({exit_price:,})원 이탈 [시장 약세] 10분봉 완성 후 매도 대기")
                                    else:
                                        _cur_key_str = current_10min_key_1.strftime("%Y%m%d%H%M")    
                                        if breakdown_wait_1["last_alert_tenmin_key"] is None:
                                            breakdown_wait_1["last_alert_tenmin_key"] = _cur_key_str
                                            _write_alert_key_db(conn, acct_no, stock_code, start_date, start_time, "1", breakdown_wait_1["last_alert_tenmin_key"])
                                            print(f"[시뮬]-{nick}-[{row['일자']}-{row['시간']}]{stock_name}[{stock_code}] 최종이탈가({exit_price:,})원 이탈 대기")

                                elif breakdown_check <= stop_price and acml_vol > chk_vol:
                                    current_10min_key_1 = get_10min_key(row["dt"])
                                    breakdown_wait_1.update({"active": True, "tenmin_key": current_10min_key_1,
                                                            "tenmin_low": None, "tenmin_vol_ok": None,
                                                            "sell_label": "이탈가 매도"})
                                    
                                    # 시장 단기 하락인 경우 10분봉 완성 후 매도 대기
                                    if _short_market_down:
                                        breakdown_wait_1["sell_on_candle_close"] = True

                                        _cur_key_str = current_10min_key_1.strftime("%Y%m%d%H%M")
                                        if breakdown_wait_1["last_alert_tenmin_key"] is None:
                                            breakdown_wait_1["last_alert_tenmin_key"] = _cur_key_str
                                            _write_alert_key_db(conn, acct_no, stock_code, start_date, start_time, "1", breakdown_wait_1["last_alert_tenmin_key"])
                                            print(f"[시뮬]-{nick}-[{row['일자']}-{row['시간']}]{stock_name}[{stock_code}] 이탈가({stop_price:,})원 이탈 [시장 약세] 10분봉 완성 후 매도 대기")
                                    else:
                                        _cur_key_str = current_10min_key_1.strftime("%Y%m%d%H%M")
                                        if breakdown_wait_1["last_alert_tenmin_key"] is None:
                                            breakdown_wait_1["last_alert_tenmin_key"] = _cur_key_str
                                            _write_alert_key_db(conn, acct_no, stock_code, start_date, start_time, "1", breakdown_wait_1["last_alert_tenmin_key"])
                                            print(f"[시뮬]-{nick}-[{row['일자']}-{row['시간']}]{stock_name}[{stock_code}] 이탈가({stop_price:,})원 이탈 대기")        

                        # 목표가 돌파 → 기준봉 생성
                        if breakout_check >= target_price:
                            base_key    = get_completed_10min_key(row["dt"])
                            base_10min  = df[df["dt"].apply(get_10min_key) == base_key]
                            if base_10min.empty:
                                continue
                            tenmin_state.update({
                                "base_low":  base_10min["저가"].astype(int).min(),
                                "base_high": base_10min["고가"].astype(int).max(),
                                "base_vol":  base_10min["거래량"].astype(int).sum(),
                                "base_key":  base_key,
                            })
                            if verbose:
                                print(f"[시뮬]-{nick}-[{row['일자']}-{row['시간']}]{stock_name}[{stock_code}]"
                                    f" 목표가 {target_price:,}원 돌파 기준봉 설정"
                                    f" 고가:{tenmin_state['base_high']:,} 저가:{tenmin_state['base_low']:,}"
                                    f" 거래량:{tenmin_state['base_vol']:,}")
                            update_simul_trail(
                                int(tenmin_state['base_low']), int(tenmin_state['base_high']),
                                int(tenmin_state['base_vol']), acct_no, stock_code,
                                start_date, start_time, "2",
                                row['시간'].replace(':', '') + '00', conn
                            )
                            signals.append({"signal_type": "BREAKOUT", "종목코드": stock_code,
                                            "발생일자": row["일자"], "발생시간": row["시간"],
                                            "돌파가격": breakout_check})
                            continue

                    # 기준봉 존재 → 10분봉 완성 시 이탈 체크
                    else:
                        completed_key = get_completed_10min_key(row["dt"])
                        tenmin_df     = df[df["dt"].apply(get_completed_10min_key) == completed_key]

                        # ── breakdown_wait_2: 분봉 저가 기준 이탈가 감지 → 해당 10분봉 완성 시 매도 ──
                        # 이탈가 이탈이 발생한 10분봉의 마지막 분봉 처리 시 조건 D에서 sell_trigger 활성화
                        if not breakdown_wait_2["active"]:
                            _bw2_10min_key = get_10min_key(row["dt"])
                            _bw2_is_exit = exit_price and low_price <= int(exit_price)
                            _bw2_is_stop = low_price <= int(stop_price)
                            if _bw2_is_exit or _bw2_is_stop:
                                _bw2_breach_price = int(exit_price) if _bw2_is_exit else int(stop_price)
                                _bw2_breach_type = "exit" if _bw2_is_exit else "stop"
                                _bw2_label = "최종이탈가" if _bw2_is_exit else "이탈가"
                                breakdown_wait_2.update({
                                    "active": True,
                                    "breach_price": _bw2_breach_price,
                                    "breach_type": _bw2_breach_type,
                                    "tenmin_key": _bw2_10min_key,
                                })
                                _bw2_key_str = _bw2_10min_key.strftime("%Y%m%d%H%M")
                                if breakdown_wait_2["last_alert_tenmin_key"] is None or _bw2_key_str > breakdown_wait_2["last_alert_tenmin_key"]:
                                    breakdown_wait_2["last_alert_tenmin_key"] = _bw2_key_str
                                    _write_alert_key_db(conn, acct_no, stock_code, start_date, start_time, "2", _bw2_key_str)
                                    print(f"[시뮬]-{nick}-[{row['일자']}-{row['시간']}]{stock_name}[{stock_code}] {_bw2_label}({_bw2_breach_price:,})원 분봉 저가 이탈 → 10분봉 완성 후 매도 대기")
                        
                        # 10분봉의 마지막 1분봉일 때만 처리 (10분봉 완성 시점)
                        # 돌파 발생 10분봉 자체는 스킵 → 다음 완성 10분봉부터 매도/갱신 체크
                        if not tenmin_df.empty and row["dt"] == tenmin_df["dt"].max():
                            if completed_key == tenmin_state["base_key"]:
                                continue
                            if completed_key >= current_10min_key:
                                continue
                            tenmin_low   = tenmin_df["저가"].astype(int).min()
                            tenmin_high  = tenmin_df["고가"].astype(int).max()
                            tenmin_vol   = tenmin_df["거래량"].astype(int).sum()
                            tenmin_close = close_price
                            sell_price   = close_price

                            sell_trigger    = False
                            sell_reason     = ""
                            safety_margin   = int(basic_price + basic_price * 0.05)
                            PEAK_RETRACEMENT_RATE = 0.5

                            prev_key      = completed_key - timedelta(minutes=10)
                            prev_tenmin_df = df[df["dt"].apply(get_completed_10min_key) == prev_key]
                            prev_close_10  = (int(prev_tenmin_df.loc[prev_tenmin_df["dt"].idxmax(), "종가"])
                                            if not prev_tenmin_df.empty else safety_margin + 1)

                            # 조건 D: breakdown_wait_2 활성 → 분봉 저가 이탈가 이탈 확인된 10분봉 완성 시 매도
                            # 이탈가 이탈이 발생한 10분봉(tenmin_key)과 현재 완성 중인 봉(completed_key)이
                            # 같거나 이후이면 sell_trigger 활성 (이탈가 이탈 분봉이 속한 봉이 완성되는 시점)
                            if not sell_trigger and breakdown_wait_2["active"] and breakdown_wait_2["tenmin_key"] is not None:
                                if completed_key >= breakdown_wait_2["tenmin_key"]:
                                    _bw2_label = "최종이탈가" if breakdown_wait_2["breach_type"] == "exit" else "이탈가"
                                    sell_trigger = True
                                    sell_price = get_valid_sell_price(max(tenmin_close, safety_margin))
                                    sell_reason = f"{_bw2_label}({breakdown_wait_2['breach_price']:,})원 분봉 저가 이탈 10분봉 완성 매도 (매도가:{sell_price:,})"
                            
                            # 조건 A: 기준봉 저가 이탈 + 안전마진 이하
                            if not sell_trigger and tenmin_close < tenmin_state["base_low"] and tenmin_close <= safety_margin:
                                sell_trigger = True
                                gap_rate     = (safety_margin - tenmin_close) / safety_margin * 100
                                prev_below   = prev_close_10 < safety_margin
                                if gap_rate <= 0.5 and not prev_below:
                                    sell_price = get_valid_sell_price(safety_margin)
                                    a_case = "[안전마진가]"
                                elif gap_rate <= 2.0 and not prev_below:
                                    sell_price = get_valid_sell_price(int((safety_margin + tenmin_close) / 2))
                                    a_case = "[절충가]"
                                else:
                                    sell_price = get_valid_sell_price(tenmin_close)
                                    a_case = "[현재가]"
                                sell_reason = f"안전마진({safety_margin:,})원 이하 기준봉 저가({tenmin_state['base_low']:,})원 종가 이탈 {a_case}"

                            # 조건 B: 고점 대비 되돌림
                            peak_to_safety = tenmin_state["peak_high"] - safety_margin
                            effective_retr = 0.3 if current_time >= dt_time(14, 30) else PEAK_RETRACEMENT_RATE
                            if (not sell_trigger and tenmin_state["peak_high"] > safety_margin
                                    and peak_to_safety >= int(safety_margin * 0.05)):
                                peak_sell_threshold = tenmin_state["peak_high"] - int(peak_to_safety * effective_retr)
                                if tenmin_close < peak_sell_threshold:
                                    sell_trigger = True
                                    sell_price   = get_valid_sell_price(max(tenmin_close, peak_sell_threshold))
                                    sell_reason  = f"고점({tenmin_state['peak_high']:,})원 되돌림 임계({peak_sell_threshold:,})원 종가 이탈"

                            # 조건 C-1: 이탈가/최종이탈가 이탈 → 즉시 매도
                            # 거래량·연속이탈 조건 없이 이탈가 기준으로 즉시 매도 처리
                            if not sell_trigger and tenmin_close > safety_margin:
                                is_exit_breach = exit_price and tenmin_close <= int(exit_price)
                                is_stop_breach = tenmin_close <= int(stop_price)
                                if is_exit_breach or is_stop_breach:
                                    sell_trigger = True
                                    sell_price = get_valid_sell_price(max(tenmin_close, safety_margin))
                                    sell_reason = f"이탈가({stop_price:,})원 이탈 (매도가:{sell_price:,})"

                            # 조건 C-2: 기준봉 저가를 종가로 이탈 + 안전마진 이상 → 연속 이탈 판단
                            # 거래량 초과 OR 연속 이탈 시 매도 (저거래량 지속 하락 방어)
                            consecutive_breaks = ((1 if tenmin_close < tenmin_state["base_low"] else 0)
                                                + (1 if prev_close_10 < tenmin_state["base_low"] else 0))
                            late_day_thresh    = 1 if current_time >= dt_time(14, 30) else 2
                            if (not sell_trigger and tenmin_close < tenmin_state["base_low"]
                                    and tenmin_close > safety_margin
                                    and (tenmin_vol > tenmin_state["base_vol"] or consecutive_breaks >= late_day_thresh)):
                                sell_trigger = True
                                sell_price   = get_valid_sell_price(max(tenmin_close, safety_margin))
                                sell_reason  = f"기준봉 저가({tenmin_state['base_low']:,})원 종가 이탈"

                            if sell_trigger:
                                    # 해당 종목의 시장이 단기 하락인 경우 : 매도주문가 = 현재가
                                if _short_market_down:
                                    order_price = sell_price
                                else:   # 해당 종목의 시장이 단기 상승인 경우 : 매도주문가 = 기준봉저가가 매도가 아래면 매도가 otherwise 기준봉저가
                                    order_price = sell_price if tenmin_state['base_low'] < sell_price else tenmin_state['base_low']
                                trail_rate   = round((100 - (order_price / basic_price) * 100) * -1, 2) if basic_price > 0 else 0
                                i_trail_plan = trail_plan if trail_plan else "50"
                                trail_qty    = int(basic_qty * int(i_trail_plan) * 0.01)
                                trail_amt    = order_price * trail_qty
                                u_basic_qty  = basic_qty - trail_qty
                                u_basic_amt  = basic_price * u_basic_qty

                                if basic_qty == trail_qty:
                                    update_simul_close(
                                        nick, order_price, trail_qty, trail_amt, trail_rate, i_trail_plan,
                                        u_basic_qty, u_basic_amt, acct_no,
                                        stock_code, stock_name, start_date, start_time, "4",
                                        row['시간'].replace(':', '') + '00', '수익완료', conn
                                    )
                                else:
                                    update_simul_close(
                                        nick, order_price, trail_qty, trail_amt, trail_rate, i_trail_plan,
                                        u_basic_qty, u_basic_amt, acct_no,
                                        stock_code, stock_name, start_date, start_time, "3",
                                        row['시간'].replace(':', '') + '00', '안전마진', conn
                                    )
                                if verbose:
                                    print(f"[시뮬]-{nick}-[{row['일자']}-{row['시간']}]{stock_name}[{stock_code}]"
                                        f" {sell_reason} (10분봉 종가:{tenmin_close:,}원, 저가:{tenmin_low:,}원)")
                                signals.append({"signal_type": "BASE_10MIN_LOW_BREAK",
                                                "종목코드": stock_code, "발생일자": row["일자"],
                                                "발생시간": row["시간"],
                                                "기준봉저가": tenmin_state["base_low"]})
                                return signals

                            # 기준봉 갱신
                            base_updated = False
                            if tenmin_high > tenmin_low:
                                if tenmin_high > tenmin_state["base_high"] or tenmin_vol > tenmin_state["base_vol"]:
                                    tenmin_state.update({
                                        "base_low":  max(tenmin_low, tenmin_state["base_low"]),
                                        "base_high": tenmin_high,
                                        "base_vol":  tenmin_vol,
                                        "peak_high": max(tenmin_state["peak_high"], tenmin_high),
                                    })
                                    base_updated = True
                                    if verbose:
                                        reason = "고가 돌파" if tenmin_high > tenmin_state["base_high"] else "거래량 돌파"
                                        print(f"[시뮬]-{nick}-[{completed_key.strftime('%Y%m%d %H:%M')}]{stock_name}[{stock_code}]"
                                            f" {reason} 기준봉 갱신 고가:{tenmin_high:,} 저가:{tenmin_low:,} 거래량:{tenmin_vol:,}")
                                    update_simul_trail(
                                        int(tenmin_low), int(tenmin_high), int(tenmin_vol),
                                        acct_no, stock_code, start_date, start_time, "2",
                                        row['시간'].replace(':', '') + '00', conn
                                    )
                            if not base_updated:
                                update_simul_trail(
                                    int(tenmin_state["base_low"]), int(tenmin_state["base_high"]),
                                    int(tenmin_state["base_vol"]), acct_no, stock_code,
                                    start_date, start_time, "2",
                                    row['시간'].replace(':', '') + '00', conn
                                )
    return signals


# ─────────────────────────────────────────
# 일별 잔고 집계 저장
# ─────────────────────────────────────────
def _update_dly_trading_balance_simul(trade_date: str, conn,
                                       access_token: str, app_key: str, app_secret: str):
    """process_account_simul 완료 후 dly_trading_balance_simul 일별 잔고 업서트.
    value_rate·value_amt 는 당일 종가(KIS API) 기준으로 계산.
    """
    try:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM public.dly_trading_balance_simul WHERE balance_day = %s",
            (trade_date,)
        )
        # 종목별 최신 레코드: 보유단가·수량·매도수량만 조회 (value 계산은 종가 기반)
        cur.execute(f"""
            WITH ranked AS (
                SELECT code, name,
                       basic_price,
                       basic_qty,
                       COALESCE(trail_qty, 0) AS sell_qty,
                       ROW_NUMBER() OVER (PARTITION BY code ORDER BY mod_dt DESC) AS rn
                FROM {SIMUL_TABLE}
                WHERE acct_no = 'SIMUL' AND trail_day = %s
            )
            SELECT code, name, basic_price, basic_qty, sell_qty
            FROM ranked
            WHERE rn = 1
        """, (trade_date,))
        rows = cur.fetchall()
        now = datetime.now()
        cnt = 0

        for r in rows:
            code, name, balance_price, balance_qty, sell_qty = r
            balance_price = int(balance_price or 0)
            balance_qty   = int(balance_qty   or 0)
            sell_qty      = int(sell_qty      or 0)
            balance_amt   = balance_price * balance_qty

            # 종가 조회 — 캐시 우선, 미스 시 KIS API 직접 호출
            close_price = 0
            try:
                with _daily_cache_lock:
                    cached = _daily_chart_full_cache.get(code)
                if cached:
                    entry = next((d for d in cached if d['date'] == trade_date), None)
                    if entry:
                        close_price = entry['close_price']
                if close_price == 0:
                    day_info = get_kis_daily_chart(
                        code, trade_date, access_token, app_key, app_secret, verbose=False
                    )
                    if day_info:
                        close_price = day_info['close_price']
                    time.sleep(0.1)
            except Exception as e_p:
                print(f"[SIMUL] {code} 종가 조회 오류: {e_p}")

            # 종가 기준 수익율·수익금액 계산
            if close_price > 0 and balance_price > 0:
                value_rate = round((close_price - balance_price) / balance_price * 100, 2)
                value_amt  = (close_price - balance_price) * balance_qty
            else:
                value_rate = 0.0
                value_amt  = 0

            cur.execute("""
                INSERT INTO public.dly_trading_balance_simul (
                    acct_no, code, name, balance_day,
                    balance_price, balance_qty, balance_amt,
                    value_rate, value_amt,
                    buy_qty, sell_qty, use_yn,
                    crt_dt, mod_dt
                ) VALUES ('74346047', %s, %s, %s, %s, %s, %s, %s, %s, 0, %s, 'Y', %s, %s)
                ON CONFLICT (acct_no, code, balance_day)
                DO UPDATE SET
                    name          = EXCLUDED.name,
                    balance_price = EXCLUDED.balance_price,
                    balance_qty   = EXCLUDED.balance_qty,
                    balance_amt   = EXCLUDED.balance_amt,
                    value_rate    = EXCLUDED.value_rate,
                    value_amt     = EXCLUDED.value_amt,
                    sell_qty      = EXCLUDED.sell_qty,
                    use_yn        = 'Y',
                    mod_dt        = EXCLUDED.mod_dt
            """, (
                code, name, trade_date,
                balance_price,
                balance_qty,
                balance_amt,
                float(value_rate),
                int(value_amt),
                sell_qty,
                now, now,
            ))
            cnt += 1

        conn.commit()
        cur.close()
        print(f"[SIMUL] dly_trading_balance_simul 업데이트 완료: {cnt}건")
    except Exception as e:
        print(f"[SIMUL] dly_trading_balance_simul 업데이트 오류: {e}")


# ─────────────────────────────────────────
# 시장흐름 동기화
# ─────────────────────────────────────────
def _sync_market_trend_to_simul(trade_date: str, conn, prev_tot_evlu_amt=None):
    """dly_acct_balance(trade_date) 시장흐름 값을
    dly_acct_balance_simul 의 다음 영업일(dt) 에 upsert.
    prev_tot_evlu_amt: trade_date +1일 dly_acct_balance_simul.tot_evlu_amt (dnca_tot_amt 설정)
    """
    try:
        cur = conn.cursor()

        # trade_date 다음 영업일 계산
        d_fmt = (datetime.strptime(trade_date, "%Y%m%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        cur.execute("SELECT post_business_day_char(%s::date)", (d_fmt,))
        next_biz = cur.fetchone()[0]

        # trade_date 다음 영업일기준 시장흐름 조회
        cur.execute("""
            SELECT market_ratio, kospi_short, kosdak_short,
                   kospi_mid,   kosdak_mid,
                   kospi_long,  kosdak_long
            FROM public.dly_acct_balance
            WHERE acct = '74346047' AND dt = %s
        """, (next_biz,))
        row = cur.fetchone()
        if not row:
            print(f"[SIMUL] dly_acct_balance({next_biz}) 시장흐름 없음 → 스킵")
            cur.close()
            return
        market_ratio, kospi_short, kosdak_short, kospi_mid, kosdak_mid, kospi_long, kosdak_long = row
        now = datetime.now()
        dnca_tot = int(prev_tot_evlu_amt) if prev_tot_evlu_amt is not None else 0

        cur.execute("""
            INSERT INTO dly_acct_balance_simul (
                acct, dt,
                dnca_tot_amt, prvs_excc_amt,
                td_buy_amt, td_sell_amt, td_tex_amt,
                user_evlu_amt, tot_evlu_amt, nass_amt,
                pchs_amt, evlu_amt, evlu_pfls_amt,
                ytdt_tot_evlu_amt, asst_icdc_amt,
                total_profit_loss_amt, buy_psbl_amt,
                market_ratio, kospi_short, kosdak_short,
                kospi_mid, kosdak_mid, kospi_long, kosdak_long,
                last_chg_date
            ) VALUES (
                '74346047', %s,
                %s, 0,
                0, 0, 0,
                0, 0, 0,
                0, 0, 0,
                0, 0,
                0, 0,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s
            )
            ON CONFLICT (acct, dt) DO UPDATE SET
                dnca_tot_amt  = EXCLUDED.dnca_tot_amt,
                market_ratio  = EXCLUDED.market_ratio,
                kospi_short   = EXCLUDED.kospi_short,
                kosdak_short  = EXCLUDED.kosdak_short,
                kospi_mid     = EXCLUDED.kospi_mid,
                kosdak_mid    = EXCLUDED.kosdak_mid,
                kospi_long    = EXCLUDED.kospi_long,
                kosdak_long   = EXCLUDED.kosdak_long,
                last_chg_date = EXCLUDED.last_chg_date
        """, (
            next_biz,
            dnca_tot,
            market_ratio, kospi_short, kosdak_short,
            kospi_mid, kosdak_mid, kospi_long, kosdak_long,
            now,
        ))
        conn.commit()
        cur.close()
        print(f"[SIMUL] dly_acct_balance_simul 시장흐름 반영"
              f" ({trade_date} → 다음영업일: {next_biz})"
              f" kospi={kospi_short}/{kospi_mid}/{kospi_long}"
              f" kosdak={kosdak_short}/{kosdak_mid}/{kosdak_long}")
    except Exception as e:
        print(f"[SIMUL] 시장흐름 동기화 오류: {e}")


# ─────────────────────────────────────────
# 시장비율 기반 리밸런싱 시뮬레이션
# (kis_market_ratio_rebalance.py simulate_rebalance 및 의존 로직 복제 —
#  process_account_simul 과 같은 프로세스에서 동일 수행일자(today)로 동시 처리하기 위함.
#  kis_holding_item_total.py 를 import 하지 않는 kis_market_ratio_rebalance.py 와
#  동일한 이유로, 필요한 로직을 여기에도 복제한다)
# ─────────────────────────────────────────
def total_excess(cash, total_eval, market_ratio):
    """base = 현금 + 전체 평가금액, target = base*market_ratio/100, excess = 평가금액-target(>0 이면 매도).
    KOSPI/KOSDAQ 구분 없이 트레이딩풀 전체를 대상으로 한 단일 초과분(원)."""
    if market_ratio is None or total_eval <= 0:
        return 0
    base = total_eval + cash
    return int(max(0, total_eval - base * market_ratio / 100))


def sell_priority(strength):
    """수급 또는 차트 strength 약할수록(낮을수록) 우선순위 높음.
    invest_point(quality, 성장/가치 점수)는 매도 우선순위 산정에서 제외(참고용 표시만 유지)."""
    return 100 - strength


def allocate(bucket, excess, cur_price_key="current_price", avail_key="avail_qty"):
    """bucket: sell_priority 계산된 holding dict 리스트. excess 만큼 순위 충전식 배분.
    각 holding dict 는 'sell_priority','eval_sum',cur_price_key,avail_key 를 가진다.
    반환: [(holding, qty), ...]"""
    ranked = sorted(bucket, key=lambda h: -h["sell_priority"])
    orders, filled = [], 0
    for h in ranked:
        if filled >= excess:
            break
        cur = h[cur_price_key]
        avail = int(h.get(avail_key, 0) or 0)
        if cur <= 0 or avail <= 0:
            continue
        # 매도 할당 금액 : 수급 또는 차트 strength > 70 이면 전체 물량, 아니면 절반 물량
        cap_amt = h["eval_sum"] if h["sell_priority"] >= TOP_CUT else h["eval_sum"] * PER_NAME_CAP
        # 시장비율 초과하여 감축할 금액(cap_amt 와 초과한 물량에서 차감한 물량 중 최소 금액)
        amt = min(cap_amt, excess - filled)
        qty = int(amt // cur)
        qty = min(qty, avail)
        if qty > 0:
            orders.append((h, qty))
            filled += qty * cur
    return orders


def build_rebalance_orders(holdings, cash, market_ratio, strength_fn, quality_fn):
    """코어 엔진. holdings 각 dict: code,name,eval_sum,current_price,avail_qty,purchase_price.
    strength_fn(code)->0~100, quality_fn(code)->0~100 주입.
    quality(invest_point)는 참고용으로 h['quality']에 기록만 하고 sell_priority 산정에는 쓰지 않음.
    KOSPI/KOSDAQ 시장 구분 없이 보유종목 전체를 sell_priority 단일 순위로 배분.
    반환: ([(holding, qty), ...], excess)"""
    total_eval = sum(h["eval_sum"] for h in holdings)
    excess = total_excess(cash, total_eval, market_ratio)
    if excess <= 0:
        return [], excess

    for h in holdings:
        st = strength_fn(h["code"])
        ql = quality_fn(h["code"])
        h["strength"], h["quality"] = st, ql
        h["sell_priority"] = sell_priority(st)      # 수급점수, 차트점수 기반 매도 우선 순위
    return allocate(holdings, excess), excess


def quality_score_from_history(conn, code, as_of=None):
    """analysis_history 정량필드 → 0~100 성장/가치 점수 (기준③).
    as_of(YYYYMMDD) 지정 시 point-in-time(그 이하 최신)로 조회."""
    cur = conn.cursor()
    if as_of:
        cur.execute("""
            SELECT growth_trend, op_yoy_forward, value_signal, band_position, target_upside_pct
            FROM analysis_history
            WHERE stock_code = %s AND run_at <= %s::date + interval '1 day'
            ORDER BY id DESC LIMIT 1
        """, (code, as_of))
    else:
        cur.execute("""
            SELECT growth_trend, op_yoy_forward, value_signal, band_position, target_upside_pct
            FROM analysis_history
            WHERE stock_code = %s ORDER BY id DESC LIMIT 1
        """, (code,))
    r = cur.fetchone()
    cur.close()
    if not r:
        return 45.0   # 자료 없음 → 중립
    trend, fwd, vsig, band, upside = r
    # 성장트렌드
    base = {"실적 턴어라운드(적자→흑자 전환 예상)": 90, "성장 가속": 85, "성장 지속": 70, "실적 개선(저점 통과 추정)": 60, "역성장/둔화": 25}.get(trend, 45)
    # 영업이익증감률 : ±10
    fwd_adj = max(-20, min(20, float(fwd or 0))) * 0.5
    # 예상실적 상승 대비 밴드하단 주가 위치 : 저평가+실적↑
    val_adj = 15 if vsig else 0
    # 목표주가 대비 상승여력 : 0~10
    up_adj = max(0, min(40, float(upside or 0))) * 0.25
    return max(0.0, min(100.0, base + fwd_adj + val_adj + up_adj))

# ────────────────────────────────────────────────────────────────────────────
# 점수 계산용 데이터 fetch (simul_server 복제)
# ────────────────────────────────────────────────────────────────────────────
def _fetch_daily_ohlcv(at, ak, asec, code):
    try:
        headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {at}",
               "appkey": ak, "appsecret": asec,
               "tr_id": "FHKST01010400", "custtype": "P"}
        
        r = requests.get(f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-price",
                         headers=headers,
                         params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code,
                                 "FID_PERIOD_DIV_CODE": "D", "FID_ORG_ADJ_PRC": "1"},
                         verify=False, timeout=10)
        rows = r.json().get("output") or []
        return rows if isinstance(rows, list) else []
    except Exception:
        return []

def _make_strength_fn(ac, cache):
    at, ak, asec = ac["access_token"], ac["app_key"], ac["app_secret"]

    def fn(code):
        if code in cache:
            return cache[code]
        time.sleep(0.25)
        ohlcv = _fetch_daily_ohlcv(at, ak, asec, code)
        chart  = _calc_chart_score(ohlcv)
        chart  = 50 if chart  is None else chart
        
        st = W_CHART * chart
        cache[code] = st
        return st
    return fn

# ────────────────────────────────────────────────────────────────────────────
# 수급/차트 점수 (simul_server._calc_*_score 복제, 각 0~100)
# ────────────────────────────────────────────────────────────────────────────
def _adx(highs, lows, closes, period=14):
    n = len(closes)
    if n < period * 2:
        return None, None, None
    trs, plus_dm, minus_dm = [], [], []
    for i in range(1, n):
        up, dn = highs[i] - highs[i-1], lows[i-1] - lows[i]
        plus_dm.append(up if (up > dn and up > 0) else 0.0)
        minus_dm.append(dn if (dn > up and dn > 0) else 0.0)
        trs.append(max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])))

    def _smooth(x):
        s = [sum(x[:period])]
        for v in x[period:]:
            s.append(s[-1] - s[-1]/period + v)
        return s
    tr_s, pdm_s, mdm_s = _smooth(trs), _smooth(plus_dm), _smooth(minus_dm)
    dxs = []
    for tr, pdm, mdm in zip(tr_s, pdm_s, mdm_s):
        if tr == 0:
            continue
        pdi, mdi = 100*pdm/tr, 100*mdm/tr
        if pdi + mdi == 0:
            continue
        dxs.append(100*abs(pdi-mdi)/(pdi+mdi))
    if len(dxs) < period:
        return None, None, None
    adx = sum(dxs[-period:]) / period
    pdi = 100*pdm_s[-1]/tr_s[-1] if tr_s[-1] else 0.0
    mdi = 100*mdm_s[-1]/tr_s[-1] if tr_s[-1] else 0.0
    return adx, pdi, mdi

def _calc_chart_score(rows):
    if not rows or len(rows) < 25:
        return None
    def _f(v):
        try: return float(v)
        except: return 0.0
    closes  = [_f(r.get("stck_clpr", 0)) for r in rows]
    highs   = [_f(r.get("stck_hgpr", 0)) for r in rows]
    lows    = [_f(r.get("stck_lwpr", 0)) for r in rows]
    volumes = [_f(r.get("acml_vol",  0)) for r in rows]
    cur = closes[0]
    ma5  = sum(closes[:5]) / 5
    ma20 = sum(closes[:20]) / 20
    ma60 = sum(closes[:60]) / 60 if len(closes) >= 60 else None

    if ma60:
        if   ma5 > ma20 > ma60:               trend_sc = 30
        elif ma5 > ma20 and ma20 < ma60:      trend_sc = 22
        elif ma5 > ma60 and ma5 <= ma20:      trend_sc = 16
        elif abs(ma5-ma20)/ma20 < 0.01:       trend_sc = 10
        elif ma5 < ma20 and ma20 > ma60:      trend_sc = 5
        else:                                 trend_sc = 0
    else:
        if   ma5 > ma20*1.02: trend_sc = 22
        elif ma5 > ma20:      trend_sc = 16
        elif ma5 > ma20*0.99: trend_sc = 10
        else:                 trend_sc = 0

    adx, pdi, mdi = _adx(list(reversed(highs)), list(reversed(lows)), list(reversed(closes)))
    if adx is None:                            adx_sc = 8
    elif adx >= 40 and pdi > mdi:              adx_sc = 25
    elif adx >= 25 and pdi > mdi:              adx_sc = 20
    elif adx >= 25 and pdi <= mdi:             adx_sc = 5
    elif adx >= 20:                            adx_sc = 12
    else:                                      adx_sc = 8

    d = (cur - ma20)/ma20*100 if ma20 else 0.0
    if   -3 <= d <= 5:    dev_sc = 20
    elif  5 <  d <= 10:   dev_sc = 15
    elif -8 <= d < -3:    dev_sc = 15
    elif -15 <= d < -8:   dev_sc = 12
    elif 10 <  d <= 15:   dev_sc = 10
    elif d < -15:         dev_sc = 8
    else:                 dev_sc = 5

    va5, va20 = sum(volumes[:5])/5, sum(volumes[:20])/20
    v = va5/va20*100 if va20 else 100
    vol_sc = 15 if v > 150 else 12 if v > 120 else 9 if v > 90 else 6 if v > 70 else 3

    vod = volumes[0]/volumes[1]*100 if len(volumes) >= 2 and volumes[1] > 0 else 100
    vod_sc = 10 if vod > 150 else 8 if vod > 120 else 6 if vod > 80 else 4 if vod > 50 else 2

    return trend_sc + adx_sc + dev_sc + vol_sc + vod_sc

def simulate_rebalance(phiils2_account, sim_date, horizon="D", strength_mode="neutral",
                       dly_acct=SIMUL_DLY_ACCT):
    """
    sim_date(YYYYMMDD) 종가 기준 리밸런싱을 시뮬레이션.
      - 보유:   dly_trading_balance_simul (balance_price=매입가, balance_qty, balance_amt=보유금액, value_amt=수익금액)
      - 현금:   dly_acct_balance_simul.prvs_excc_amt
      - 시장비율: dly_acct_balance.market_ratio (실제 그 날의 값)
      - 기준①:  strength_mode='neutral'(50 고정) | 'pit'(로컬 일봉 이력 필요)
      - 참고:    invest_point(quality)는 analysis_history point-in-time (run_at <= sim_date) 값으로 표시만
    매도분은 trading_trail_simul(trail_tp='4')에 기록하고 dly_trading_balance_simul 잔량을 차감.
    dly_acct_balance_simul 재집계는 process_account_simul 이 이미 담당한 뒤 마지막 단계로 호출된다."""
    conn = db.connect(conn_string)
    try:
        cur = conn.cursor()
        # 1) 보유 포지션
        cur.execute("""
            SELECT A.code, A.name, A.balance_price, A.balance_qty, A.balance_amt, A.value_amt
            FROM public.dly_trading_balance_simul A, 
            (
                SELECT code, name
                FROM  public.trading_trail_simul
                WHERE acct_no = %s
                AND trading_plan = 'h'
                AND COALESCE(basic_qty, 0) > 0
                AND trail_day = %s
            ) B 
            WHERE A.code = B.code
            AND A.acct_no = %s AND A.balance_day = %s AND A.use_yn = 'Y' AND A.balance_qty > 0
        """, (SIMUL_ACCT, sim_date, dly_acct, sim_date))
        rows = cur.fetchall()
        if not rows:
            print(f"[SIMUL {sim_date}] 리밸런싱 대상 보유 없음 → 스킵")
            return {"date": sim_date, "orders": []}

        holdings = []
        for code, name, bprice, bqty, bamt, vamt in rows:
            prev_day_info = get_prev_day_info(
                code,
                sim_date,
                phiils2_account["access_token"],
                phiils2_account["app_key"], 
                phiils2_account["app_secret"],
                conn
            )

            if prev_day_info is None:
                print(f"[{name}-{code}] 전일 일봉 데이터 미존재")
                continue

            prev_low = prev_day_info['low_price']

            bqty = int(bqty or 0)
            eval_sum = int(bamt or 0) + int(vamt or 0)              # 평가금액 = 보유금액 + 수익금액
            cur_price = int(round(eval_sum / bqty)) if bqty else 0  # 종가 근사

            # 전일 저가 현재가 이탈시
            if prev_low > int(cur_price or 0):
                holdings.append({"code": code, "name": name or code,
                                "current_price": cur_price, "eval_sum": eval_sum,
                                "purchase_price": int(float(bprice or 0)),
                                "avail_qty": bqty})

        if len(holdings) > 0:
            # 2) 현금 (sim_date 시점 예수금)
            prev_date = get_previous_business_day(
                (datetime.strptime(sim_date, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d"), conn
            )
            
            cur.execute("""
                SELECT 
                    prvs_excc_amt, 
                    pchs_amt, 
                    (
                        SELECT SUM(balance_amt)
                        FROM public.dly_trading_balance_simul
                        WHERE acct_no = %s AND balance_day = %s AND use_yn = 'Y'
                    ),
                    (
                        SELECT COALESCE(SUM((trail_price - basic_price)*trail_qty), 0)
                        FROM trading_trail_simul
                        WHERE acct_no = 'SIMUL' AND trail_day = %s
                        AND trail_tp IN ('3','4')
                        AND trail_price > 0 AND trail_qty > 0
                    )
                FROM dly_acct_balance_simul
                WHERE acct = %s AND dt < %s ORDER BY dt DESC LIMIT 1
            """, (dly_acct, sim_date, sim_date, dly_acct, prev_date))
            pt_row                  = cur.fetchone()
            prev_excc               = int(pt_row[0]) if pt_row else None
            prev_pchs               = int(pt_row[1]) if pt_row else 0
            pchs_amt                = int(pt_row[2]) if pt_row else 0
            total_profit_loss_amt   = int(pt_row[3]) if pt_row else 0

            # 자본기준: dly_acct_balance_simul 의 prev_excc(전일 예수금) 사용 (없으면 TOTAL_INVEST_BASE 폴백)
            base_capital  = prev_excc if prev_excc is not None else TOTAL_INVEST_BASE
            # 예수금 = 전일 예수금 - 매수총액 + 전일 매수총액 + 실현손익
            cash = base_capital - pchs_amt + prev_pchs + total_profit_loss_amt

            # 3) 그 날의 시장비율 (실제 dly_acct_balance)
            cur.execute("""
                SELECT market_ratio
                FROM dly_acct_balance WHERE dt = %s
                ORDER BY last_chg_date DESC NULLS LAST LIMIT 1
            """, (sim_date,))
            sr = cur.fetchone()
            if not sr:
                print(f"[SIMUL {sim_date}] dly_acct_balance 신호 없음 → 리밸런싱 스킵")
                return {"date": sim_date, "orders": []}
            mr = sr[0]
            cur.close()

            # 4) 점수 함수 (point-in-time)
            def quality_fn(code):
                return quality_score_from_history(conn, code, as_of=sim_date)

            cache = {}
            strength_fn = _make_strength_fn(phiils2_account, cache)

            # 5) 주문 산출
            orders, excess = build_rebalance_orders(holdings, cash, mr, strength_fn, quality_fn)

            print(f"[SIMUL {sim_date}] 리밸런싱 cash={cash:,} mr={mr} "
                f"초과={excess:,} 매도={len(orders)}건")

            # 6) 체결 반영 (종가 매도) → 기존 trading_trail_simul 활성 행 UPDATE
            applied = []
            for h, qty in orders:
                sell_price = h["current_price"]
                cur2 = conn.cursor()
                cur2.execute("""
                    SELECT basic_price, basic_qty, trail_dtm, trail_tp
                    FROM trading_trail_simul
                    WHERE acct_no = %s AND trail_day = %s AND code = %s
                    AND trail_tp IN ('1','2','L','P')
                    ORDER BY trail_dtm DESC LIMIT 1
                """, (SIMUL_ACCT, sim_date, h["code"]))
                row = cur2.fetchone()
                if not row:
                    print(f"  ⚠ {h['name']}[{h['code']}] 활성 trading_trail_simul 행 없음 → 리밸런싱 매도 스킵")
                    cur2.close()
                    continue
                basic_price, basic_qty, trail_dtm, cur_trail_tp = int(row[0]), int(row[1]), row[2], row[3]
                remaining_qty = basic_qty - qty
                new_trail_tp  = '4' if remaining_qty <= 0 else '3'
                new_basic_amt = basic_price * remaining_qty
                trail_rate    = round((sell_price - basic_price) / basic_price * 100, 2) if basic_price else 0.0
                pnl = int((sell_price - basic_price) * qty)

                cur2.execute("""
                    UPDATE trading_trail_simul SET
                        trail_price  = %s,
                        trail_qty    = %s,
                        trail_amt    = %s,
                        trail_rate   = %s,
                        trail_tp     = %s,
                        proc_min     = %s,
                        basic_qty    = %s,
                        basic_amt    = %s,
                        trade_result = %s,
                        mod_dt       = %s
                    WHERE acct_no = %s AND code = %s
                    AND trail_day = %s AND trail_dtm = %s
                    AND trail_tp IN ('1','2','L','P')
                """, (
                    sell_price, qty, sell_price * qty, trail_rate,
                    new_trail_tp, "152900",
                    remaining_qty, new_basic_amt,
                    f"시장비율 리밸런싱({horizon})", datetime.now(),
                    SIMUL_ACCT, h["code"], sim_date, trail_dtm,
                ))
                cur2.execute("""
                    UPDATE dly_trading_balance_simul
                    SET balance_qty = balance_qty - %s,
                        balance_amt = GREATEST(balance_amt - %s, 0),
                        value_amt = %s,
                        sell_qty = %s,
                        mod_dt = %s
                    WHERE acct_no = %s AND balance_day = %s AND code = %s
                """, (qty, basic_price * qty, pnl, qty, datetime.now(), dly_acct, sim_date, h["code"]))
                conn.commit()
                cur2.close()
                applied.append({"code": h["code"], "qty": qty, "sell_price": sell_price, "pnl": pnl})
                print(f"  · 리밸런싱 매도 {h['name']}[{h['code']}] {qty}주 @ {sell_price:,} 손익 {pnl:,} "
                    f"(quality={h.get('quality',0):.0f}, trail_tp {cur_trail_tp}→{new_trail_tp})")
                
            return {"date": sim_date, "market_ratio": mr,
                            "excess": excess,
                            "orders": applied,
                            "realized_pnl": sum(a["pnl"] for a in applied)}    

        else:
            return {"date": sim_date, "market_ratio": "",
                            "excess": "",
                            "orders": "",
                            "realized_pnl": ""}
        
    finally:
        conn.close()


# ─────────────────────────────────────────
# proc_min 이후 1분봉 기준 trail_price 미존재 대상 종가 갱신
# ─────────────────────────────────────────
def update_trail_price_at_close(conn, ac):
    """trail_tp='3','4' 대상: proc_min 이후 1분봉 기준 trail_price 미존재 시 15:20 현재가로 trail_price 갱신."""
    now = datetime.now()
    cur = conn.cursor()
    cur.execute(f"""
        SELECT code, name, trail_price, proc_min, trail_day, trail_dtm, basic_price, trail_qty
        FROM {SIMUL_TABLE}
        WHERE acct_no = %s AND trail_day = %s AND trail_tp IN ('3', '4') AND proc_min < '152000'
        ORDER BY code
    """, (SIMUL_ACCT, today))
    rows = cur.fetchall()
    cur.close()

    if not rows:
        print("[시뮬] 15:20 이전 trail_price 갱신 대상 없음")
        return

    target_dt = datetime.strptime(today + "152000", "%Y%m%d%H%M%S")

    for code, name, trail_price, proc_min, trail_day, trail_dtm, basic_price, trail_qty in rows:
        trail_price = int(trail_price or 0)
        if trail_price <= 0:
            continue
        proc_min_str = str(int(proc_min or 0)).zfill(6)
        try:
            proc_dt = datetime.strptime(trail_day + proc_min_str, "%Y%m%d%H%M%S")
        except Exception:
            continue

        df = get_kis_1min_full_day(code, trail_day, "152000",
                                    ac['access_token'], ac['app_key'], ac['app_secret'])
        if df.empty:
            print(f"[시뮬] [{name}({code})] 15:20 분봉 없음 → 스킵")
            continue

        df_range = df[(df["dt"] > proc_dt) & (df["dt"] <= target_dt)].copy()
        if df_range.empty:
            print(f"[시뮬] [{name}({code})] proc_min({proc_min_str}) 이후 분봉 없음 → 스킵")
            continue

        _low  = df_range["저가"].astype(int)
        _high = df_range["고가"].astype(int)
        if ((_low <= trail_price) & (_high >= trail_price)).any():
            print(f"[시뮬] [{name}({code})] trail_price({trail_price:,}) 봉 범위 내 존재 → 미갱신")
            continue

        current_price = int(df_range.iloc[-1]["종가"])
        _trail_rate  = round((current_price / basic_price - 1) * 100, 2) if basic_price > 0 else 0
        _trail_amt   = current_price * trail_qty
        upd_cur = conn.cursor()
        upd_cur.execute(f"""
            UPDATE {SIMUL_TABLE}
            SET trail_price   = %s
                , trail_rate  = %s
                , trail_amt   = %s
                , proc_min    = %s
                , mod_dt      = %s
            WHERE acct_no = %s AND code = %s
              AND trail_day = %s AND trail_dtm = %s
              AND trail_tp IN ('3', '4')
        """, (current_price, _trail_rate, _trail_amt, '152000', now, SIMUL_ACCT, code, trail_day, trail_dtm))
        conn.commit()
        upd_cur.close()
        print(f"[시뮬] [{name}({code})] trail_price 갱신: {trail_price:,}→{current_price:,}원 (15:20 현재가)")


# ─────────────────────────────────────────
# 종목별 처리 (스레드)
# ─────────────────────────────────────────
def process_stock_simul(stock_info, ac, conn_stock):
    try:
        signal = get_kis_1min_from_datetime_simul(
            nick        = "SIMUL",
            stock_code  = stock_info[0],
            stock_name  = stock_info[1],
            start_date  = stock_info[2],
            start_time  = stock_info[3],
            target_price= int(stock_info[4]),
            stop_price  = int(stock_info[5]),
            basic_price = int(stock_info[6]),
            basic_qty   = int(stock_info[7]),
            trail_tp    = stock_info[8],
            trail_plan  = stock_info[9],
            proc_min    = stock_info[10],
            volumn      = stock_info[11],
            trade_tp    = stock_info[12],
            exit_price  = int(stock_info[13]),
            access_token= ac['access_token'],
            app_key     = ac['app_key'],
            app_secret  = ac['app_secret'],
            acct_no     = SIMUL_ACCT,
            conn        = conn_stock,
            bot         = None,
            chat_id     = None,
            breakout_type="high",
            verbose     = True,
        )
        if signal:
            print(f"\n📌 [시뮬] 신호 발생: {signal}")
        else:
            print(f"\n📌 [시뮬] 신호 없음 [{stock_info[1]}-{stock_info[0]}]")
    except Exception as e:
        print(f"\n⚠️ [시뮬] [{stock_info[1]}-{stock_info[0]}] 처리 오류: {e}")


# ─────────────────────────────────────────
# 메인 처리
# ─────────────────────────────────────────
def process_account_simul():
    """trading_trail_simul 레코드를 기준으로 시뮬레이션 실행."""
    conn_acct = db.connect(conn_string)
    try:
        # API 조회용 계좌 정보
        ac = account("phills2", conn_acct)

        # trading_trail_simul 조회 (acct_no='SIMUL', trail_dtm+5분 이내 조건)
        cur = conn_acct.cursor()
        cur.execute(f"""
            SELECT code, name, trail_day, trail_dtm,
                   target_price, stop_price, basic_price, COALESCE(basic_qty, 0),
                   CASE WHEN trail_tp = 'L' THEN 'L' ELSE trail_tp END,
                   trail_plan, proc_min, volumn, trade_tp, exit_price
            FROM {SIMUL_TABLE}
            WHERE acct_no = %s
              AND trail_tp IN ('1','2','L')
              AND trail_day = %s
            ORDER BY code, proc_min, mod_dt
        """, (SIMUL_ACCT, today))
        result = cur.fetchall()
        cur.close()

        if not result:
            print(f"[SIMUL] {today} 처리 대상 없음")
            return

        # 일봉 사전 캐싱
        unique_codes = list(set(r[0] for r in result))
        for code in unique_codes:
            get_prev_day_info(code, today, ac['access_token'], ac['app_key'], ac['app_secret'], conn_acct)
            time.sleep(0.1)
            get_kis_daily_chart_full(code, today, ac['access_token'], ac['app_key'], ac['app_secret'])
            time.sleep(0.1)

        # 종목별 병렬 처리
        max_workers = min(len(result), 6)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for stock_info in result:
                conn_stock = db.connect(conn_string)
                f = executor.submit(process_stock_simul, stock_info, ac, conn_stock)
                futures[f] = (stock_info, conn_stock)
            for future in as_completed(futures):
                stock_info, conn_stock = futures[future]
                try:
                    future.result()
                except Exception as e:
                    print(f"⚠️ [시뮬] [{stock_info[1]}-{stock_info[0]}] 오류: {e}")
                finally:
                    conn_stock.close()

        update_trail_price_at_close(conn_acct, ac)

        # 전체 종목 처리 완료 후 일별 잔고 집계 저장 (종가 기준 수익율 계산)
        _update_dly_trading_balance_simul(
            today, conn_acct,
            ac['access_token'], ac['app_key'], ac['app_secret']
        )

        # dly_acct_balance_simul tot_evlu_amt 조회
        _cur_tot = conn_acct.cursor()
        _cur_tot.execute("""
            SELECT tot_evlu_amt FROM dly_acct_balance_simul
            WHERE acct = '74346047' AND dt = prev_business_day_char(%s::date)
        """, (today,))
        _tot_row = _cur_tot.fetchone()
        _cur_tot.close()
        prev_tot_evlu_amt = int(_tot_row[0]) if _tot_row else TOTAL_INVEST_BASE

        # dly_acct_balance_simul 미존재시 생성
        _cur_chk = conn_acct.cursor()
        _cur_chk.execute("SELECT prev_business_day_char(%s)", (today,))
        _prev_dt = _cur_chk.fetchone()[0]
        _today_exists = _cur_chk.fetchone()
        if not _today_exists:
            _cur_chk.execute("""
                SELECT market_ratio, kospi_short, kosdak_short,
                       kospi_mid, kosdak_mid, kospi_long, kosdak_long
                FROM public.dly_acct_balance
                WHERE acct = '74346047' AND dt = prev_business_day_char(%s::date)
            """, (today,))
            _mkt = _cur_chk.fetchone()
            if _mkt:
                _mkt_ratio, _ks, _kds, _km, _kdm, _kl, _kdl = _mkt
            else:
                _mkt_ratio = _ks = _kds = _km = _kdm = _kl = _kdl = 0
            _now = datetime.now()
            _cur_chk.execute("""
                INSERT INTO dly_acct_balance_simul (
                    acct, dt,
                    dnca_tot_amt, prvs_excc_amt,
                    td_buy_amt, td_sell_amt, td_tex_amt,
                    user_evlu_amt, tot_evlu_amt, nass_amt,
                    pchs_amt, evlu_amt, evlu_pfls_amt,
                    ytdt_tot_evlu_amt, asst_icdc_amt,
                    total_profit_loss_amt, buy_psbl_amt,
                    market_ratio, kospi_short, kosdak_short,
                    kospi_mid, kosdak_mid, kospi_long, kosdak_long,
                    last_chg_date
                ) VALUES (
                    '74346047', %s,
                    %s, %s, 0, 0, 0, 0, %s, %s, 0, 0, 0, 0, 0, 0, 0,
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (acct, dt) DO NOTHING
            """, (
                _prev_dt,
                TOTAL_INVEST_BASE, TOTAL_INVEST_BASE, TOTAL_INVEST_BASE, TOTAL_INVEST_BASE,
                _mkt_ratio, _ks, _kds, _km, _kdm, _kl, _kdl,
                _now,
            ))
            conn_acct.commit()
            print(f"[SIMUL] dly_acct_balance_simul {today} 신규 생성 (dnca_tot_amt={prev_tot_evlu_amt})")
        _cur_chk.close()

        # dly_acct_balance 시장흐름 값을 dly_acct_balance_simul 에 반영
        _sync_market_trend_to_simul(today, conn_acct, prev_tot_evlu_amt)

        # 파동기준(trading_plan = 'h') 금일종가 전일 저가 이탈시 매도 - 시장비율 기반 리밸런싱 시뮬레이션 (동일 수행일자 today 를 sim_date 로 동시 처리)
        try:
            simulate_rebalance(ac, today)
        except Exception as e:
            print(f"[SIMUL] 리밸런싱 시뮬레이션 오류: {e}")

    except Exception as e:
        print(f"[SIMUL] 계좌 처리 오류: {e}")
    finally:
        conn_acct.close()


if __name__ == "__main__":
    _conn_check = db.connect(conn_string)
    try:
        _is_business = is_business_day(today, _conn_check)
    finally:
        _conn_check.close()

    if _is_business:
        process_account_simul()
    else:
        print(f"[SIMUL] {today} 비영업일 - 처리 생략")
