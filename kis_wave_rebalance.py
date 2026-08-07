"""
kis_market_ratio_rebalance.py
================================================================================
장마감 동시호가(15:20~15:30) 시장비율 기반 비중 리밸런싱.

개요
----
투자종목(trading_plan='i')을 제외한 운용 base(현금 + 트레이딩 평가금액)에서
트레이딩 종목의 비율이 시장비율(market_ratio)을 초과하면, 초과분만큼
동시호가에서 시장가로 매도한다.

  base            = 현금(prvs_rcdl_excc_amt) + Σ eval_sum (trading_plan NOT IN ('i'))
  target_stock    = base * market_ratio/100
  excess          = 현재 트레이딩 평가금액 - target_stock   (>0 이면 매도)

매도 대상 선정·수량 배분 기준
  수급점수 + 차트점수(simul_server 점수식 재사용, 각 0~100)를 합산한 종합점수(strength)가
  낮은 종목부터 우선 매도. KOSPI/KOSDAQ 시장 구분 없이 보유종목 전체를 단일 우선순위로 배분.
  invest_point 성장/가치 점수(quality)는 참고용으로 계산·표시만 하고 매도 우선순위 산정에서는 제외.

정책 결정 (합의)
  - 우선 '일 단위(horizon='D')'만 처리.
  - 'h'(보류/헤지) 종목도 매도 대상에 포함 → base/트레이딩풀 모두 NOT IN ('i') 기준.
    (기존 kis_holding_item_total 의 현금확보 로직은 NOT IN ('i','h') base 를 쓰므로
     두 메커니즘의 base 정의가 다름에 유의 — 의도된 차이)

주의
  - kis_holding_item_total.py 는 import 시 배치가 자동 실행되므로 import 하지 않고
    필요한 KIS 함수를 이 파일에 복제한다.
  - stockBalance_stock_balance / stockFundMng_stock_fund_mng 는 선행 배치가 최신화한
    값을 읽는다(이 모듈은 잔고 API 재조회를 하지 않는다). 15:18 실행 전 잔고 동기화가
    돌아있어야 한다.
  - 수급/차트 점수는 simul_server._calc_*_score 로직을 그대로 복제(동기화 유지 필요).
================================================================================
"""

import sys
import time
import json
import math
import argparse
from datetime import datetime, timedelta, time as dtime, date
import threading
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import psycopg2 as db
import kis_api_resp as resp   # Batch 공용 응답 파서

# ────────────────────────────────────────────────────────────────────────────
# 설정
# ────────────────────────────────────────────────────────────────────────────
URL_BASE    = "https://openapi.koreainvestment.com:9443"
conn_string = "dbname='fund_risk_mng' host='192.168.50.81' port='5432' user='postgres' password='asdf1234'"
today = datetime.now().strftime("%Y%m%d")

# 리밸런싱 가중치 (튜닝 포인트)
W_SUPPLY      = 0.5    # strength = W_SUPPLY*수급
W_CHART       = 0.5    # strength = W_CHART*차트 
TOP_CUT       = 70     # sell_priority(100 - strength) 이 값 이상이면 종목당 전량 매도 허용, 아니면 일 최대 70%
PER_NAME_CAP  = 0.5    # 종목당 1일 최대 매도 비중 (평가금액 대비)

requests.packages.urllib3.disable_warnings()     # verify=False 경고 억제
_daily_cache_lock = threading.Lock()
_prev_day_info_cache = {} 

# ────────────────────────────────────────────────────────────────────────────
# KIS 함수 (kis_holding_item_total.py 에서 복제 — import 시 부작용 방지)
# ────────────────────────────────────────────────────────────────────────────
def auth(app_key, app_secret):
    headers = {"content-type": "application/json"}
    body = {"grant_type": "client_credentials", "appkey": app_key, "appsecret": app_secret}
    res = requests.post(f"{URL_BASE}/oauth2/tokenP", headers=headers,
                        data=json.dumps(body), verify=False, timeout=10)
    return res.json()["access_token"]


def account(nickname, conn):
    """토큰 유효성 확인 후 갱신, 계좌 인증정보 반환."""
    today = datetime.now().strftime("%Y%m%d")
    cur = conn.cursor()
    cur.execute("""
        SELECT acct_no, access_token, app_key, app_secret, token_publ_date,
               substr(token_publ_date, 0, 9) AS token_day, bot_token2, chat_id
        FROM "stockAccount_stock_account" WHERE nick_name = %s
    """, (nickname,))
    row = cur.fetchone()
    cur.close()
    if not row:
        raise RuntimeError(f"계좌 없음: {nickname}")
    acct_no, access_token, app_key, app_secret, token_publ_date, token_day, bot_token2, chat_id = row
    valid = datetime.strptime(token_publ_date, "%Y%m%d%H%M%S")
    if (datetime.now() - valid).days >= 1 or token_day != today:
        access_token = auth(app_key, app_secret)
        token_publ_date = datetime.now().strftime("%Y%m%d%H%M%S")
        cur2 = conn.cursor()
        cur2.execute("""UPDATE "stockAccount_stock_account"
                        SET access_token=%s, token_publ_date=%s, last_chg_date=%s
                        WHERE acct_no=%s""",
                     (access_token, token_publ_date, datetime.now(), acct_no))
        conn.commit()
        cur2.close()
    return {"acct_no": acct_no, "access_token": access_token,
            "app_key": app_key, "app_secret": app_secret,
            "bot_token2": bot_token2, "chat_id": chat_id}


def _headers(access_token, app_key, app_secret, tr_id):
    return {"Content-Type": "application/json",
            "authorization": f"Bearer {access_token}",
            "appKey": app_key, "appSecret": app_secret,
            "tr_id": tr_id, "custtype": "P"}


def inquire_price(access_token, app_key, app_secret, code):
    """FHKST01010100 현재가 시세. 동시호가(15:18~15:30)에는 J(KRX) 기준."""
    t = datetime.now().strftime("%H%M")
    params = {"FID_COND_MRKT_DIV_CODE": "J" if "0900" <= t < "1530" else "NX",
              "FID_INPUT_ISCD": code}
    res = requests.get(f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-price",
                       headers=_headers(access_token, app_key, app_secret, "FHKST01010100"),
                       params=params, verify=False, timeout=10)
    return resp.APIResp(res).getBody().output


def order_cash(buy_flag, access_token, app_key, app_secret, acct_no, stock_code, ord_dvsn, order_qty, order_price, excg_id="KRX"):
    """현금 주문. 매도=False(TTTC0011U). 시장가=ord_dvsn '01', order_price '0'."""
    tr_id = "TTTC0012U" if buy_flag else "TTTC0011U"
    params = {"CANO": acct_no, "ACNT_PRDT_CD": "01", "PDNO": stock_code,
              "ORD_DVSN": ord_dvsn, "ORD_QTY": str(order_qty), "ORD_UNPR": str(order_price),
              "EXCG_ID_DVSN_CD": excg_id}
    res = requests.post(f"{URL_BASE}/uapi/domestic-stock/v1/trading/order-cash",
                        headers=_headers(access_token, app_key, app_secret, tr_id),
                        data=json.dumps(params), verify=False, timeout=10)
    ar = resp.APIResp(res)
    return ar  # 호출부에서 isOK()/output 판단

def get_previous_business_day(day, conn):
    cur100 = conn.cursor()
    cur100.execute("select prev_business_day_char(%s)", (day,))
    result_one00 = cur100.fetchall()
    cur100.close()

    return result_one00[0][0]

def get_prev_day_info(stock_code, trade_date, access_token, app_key, app_secret, conn):
    cache_key = (stock_code, trade_date)
    with _daily_cache_lock:
        if cache_key in _prev_day_info_cache:
            return _prev_day_info_cache[cache_key]
        
    prev_date = get_previous_business_day((datetime.strptime(trade_date, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d"), conn)

    result = get_kis_daily_chart(
        stock_code=stock_code,
        trade_date=prev_date,
        access_token=access_token,
        app_key=app_key,
        app_secret=app_secret
    )

    if result is not None:
        with _daily_cache_lock:
            _prev_day_info_cache[cache_key] = result

    return result  

def get_kis_daily_chart(
        stock_code: str,
        trade_date: str,
        access_token: str,
        app_key: str,
        app_secret: str,
        market_code: str = "J",           # J:KRX, NX:NXT, UN:통합
        period: str = "D",                # D:최근30거래일, W:최근30주, M:최근30개월
        adjust_price: str = "1",          # 0:수정주가미반영, 1:수정주가반영
        verbose: bool = True              # 출력 제어 옵션
    ):
    url = f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-daily-price"

    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {access_token}",
        "appkey": app_key,
        "appsecret": app_secret,
        "tr_id": "FHKST01010400",
        "custtype": "P"
    }

    params = {
        "FID_COND_MRKT_DIV_CODE": market_code,
        "FID_INPUT_ISCD": stock_code,
        "FID_PERIOD_DIV_CODE": period,
        "FID_ORG_ADJ_PRC": adjust_price,
    }

    for attempt in range(3):
        try:
            res = requests.get(url, headers=headers, params=params, timeout=10)
            data = res.json()
            if data.get('rt_cd') == '1' and attempt < 2:
                time.sleep(1)
                continue
            break
        except Exception as e:
            if attempt < 2:
                time.sleep(0.5 * (attempt + 1))
            else:    
                if verbose:
                    print(f"\n⚠️ 일봉 조회 실패 ({stock_code}): {e}")
                return None

    if "output" not in data or not data["output"]:
        if verbose:
            print(f"⛔ 일봉 데이터 없음 ({stock_code}) rt_cd={data.get('rt_cd')}, msg={data.get('msg1', '')}")
        return None

    df = pd.DataFrame(data["output"])
    if df.empty:
        return None

    # 날짜 필터 (YYYYMMDD)
    day_df = df[df["stck_bsop_date"] == trade_date]

    if day_df.empty:
        if verbose:
            print(f"⛔ {trade_date} 일봉 없음")
        return None

    result = {
        "low_price": int(day_df.iloc[0]["stck_lwpr"]),
        "high_price": int(day_df.iloc[0]["stck_hgpr"]),
        "close_price": int(day_df.iloc[0]["stck_clpr"]),
        "volume": int(day_df.iloc[0]["acml_vol"]),
    }

    return result


# ────────────────────────────────────────────────────────────────────────────
# 점수 계산용 데이터 fetch (simul_server 복제)
# ────────────────────────────────────────────────────────────────────────────
def _fetch_daily_ohlcv(at, ak, asec, code):
    try:
        r = requests.get(f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-daily-price",
                         headers=_headers(at, ak, asec, "FHKST01010400"),
                         params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code,
                                 "FID_PERIOD_DIV_CODE": "D", "FID_ORG_ADJ_PRC": "1"},
                         verify=False, timeout=10)
        rows = r.json().get("output") or []
        return rows if isinstance(rows, list) else []
    except Exception:
        return []


def _fetch_short_selling(at, ak, asec, code):
    today  = datetime.now().strftime("%Y%m%d")
    d60ago = (datetime.now() - timedelta(days=60)).strftime("%Y%m%d")
    try:
        r = requests.get(f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-daily-short-over",
                         headers=_headers(at, ak, asec, "FHPST04830000"),
                         params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code,
                                 "FID_INPUT_DATE_1": d60ago, "FID_INPUT_DATE_2": today},
                         verify=False, timeout=10)
        d = r.json()
        return (d.get("output2") or []) if d.get("rt_cd") == "0" else []
    except Exception:
        return []


def _fetch_investor(at, ak, asec, code):
    try:
        r = requests.get(f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-investor",
                         headers=_headers(at, ak, asec, "FHKST01010900"),
                         params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code},
                         verify=False, timeout=10)
        d = r.json()
        return d["output"] if d.get("rt_cd") == "0" and isinstance(d.get("output"), list) else []
    except Exception:
        return []


def _fetch_cur_price_out(at, ak, asec, code):
    try:
        r = requests.get(f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-price",
                         headers=_headers(at, ak, asec, "FHKST01010100"),
                         params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code},
                         verify=False, timeout=10)
        d = r.json()
        return d["output"] if d.get("rt_cd") == "0" and d.get("output") else None
    except Exception:
        return None


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


def _obv_trend(closes, volumes, n=5):
    if len(closes) < n + 1:
        return 0.0
    obv = [0.0]
    for i in range(1, len(closes)):
        if   closes[i] > closes[i-1]: obv.append(obv[-1] + volumes[i])
        elif closes[i] < closes[i-1]: obv.append(obv[-1] - volumes[i])
        else:                         obv.append(obv[-1])
    recent, past = obv[0], obv[min(n, len(obv)-1)]
    denom = abs(past) if past else 1.0
    return (recent - past) / denom * 100


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


def _calc_supply_score(ohlcv, inv, price, ssts=None):
    if not inv:
        return None
    def _si(v):
        try: return int(v)
        except: return 0
    def _sf(v):
        try: return float(v)
        except: return 0.0
    n5 = min(5, len(inv))
    frgn = sum(_si(r.get("frgn_ntby_tr_pbmn", 0)) for r in inv[:n5]) / 100
    orgn = sum(_si(r.get("orgn_ntby_tr_pbmn", 0)) for r in inv[:n5]) / 100
    if ssts:
        nd5 = min(5, len(ssts))
        ssts_avg = sum(_sf(r.get("ssts_vol_rlim", 0)) for r in ssts[:nd5]) / nd5
    else:
        ssts_avg = 0.0
    loan = _sf((price or {}).get("whol_loan_rmnd_rate", 0))
    obv = 0.0
    if ohlcv and len(ohlcv) >= 6:
        closes  = [_sf(r.get("stck_clpr", 0)) for r in ohlcv]
        volumes = [_sf(r.get("acml_vol",  0)) for r in ohlcv]
        obv = _obv_trend(closes, volumes, n=5)

    frgn_sc = 30 if frgn > 200 else 24 if frgn > 50 else 18 if frgn > 10 else 14 if frgn > 0 else 8 if frgn > -10 else 3 if frgn > -50 else 0
    orgn_sc = 25 if orgn > 200 else 20 if orgn > 50 else 15 if orgn > 10 else 11 if orgn > 0 else 6 if orgn > -10 else 2 if orgn > -50 else 0
    ssts_sc = 20 if ssts_avg < 1 else 16 if ssts_avg < 2 else 12 if ssts_avg < 3 else 8 if ssts_avg < 5 else 4 if ssts_avg < 10 else 0
    loan_sc = 15 if loan < 0.5 else 12 if loan < 1 else 9 if loan < 2 else 5 if loan < 5 else 2 if loan < 10 else 0
    obv_sc  = 10 if obv > 3 else 7 if obv > 0 else 5 if obv > -3 else 2
    return frgn_sc + orgn_sc + ssts_sc + loan_sc + obv_sc


# ────────────────────────────────────────────────────────────────────────────
# 코어 로직 (순수 함수 — 라이브/백테스트 공용, 주입 가능)
# ────────────────────────────────────────────────────────────────────────────
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
        # h["sell_priority"] = sell_priority(ql)      # 성장트렌드, 영업이익증감률, 예상실적 상승 대비 밴드하단 위치여부, 목표주가 대비 상승여력 기반 매도 우선 순위
    return allocate(holdings, excess), excess


# ────────────────────────────────────────────────────────────────────────────
# 데이터 접근 (라이브)
# ────────────────────────────────────────────────────────────────────────────
def load_fund_signals(conn, acct_no):
    """stockFundMng_stock_fund_mng → (cash, sig dict, market_ratio)."""
    cur = conn.cursor()
    cur.execute("""
        SELECT prvs_rcdl_excc_amt, market_ratio,
               kospi_short, kospi_mid, kospi_long,
               kosdak_short, kosdak_mid, kosdak_long,
               (SELECT SUM(eval_sum) 
                FROM "stockBalance_stock_balance"
	            WHERE acct_no = %s
	            AND proc_yn = 'Y'
	            AND (trading_plan = 'h' OR trading_plan IS NULL)
	            AND COALESCE(eval_sum, 0) > 0) AS total_eval
        FROM "stockFundMng_stock_fund_mng" WHERE acct_no = %s
    """, (str(acct_no),str(acct_no),))
    r = cur.fetchone()
    cur.close()
    if not r:
        return 0, {}, None
    cash = int(r[0]) if r[0] is not None else 0
    mr = float(r[1]) if r[1] is not None else None
    sig = {"kospi_short": r[2], "kospi_mid": r[3], "kospi_long": r[4],
           "kosdak_short": r[5], "kosdak_mid": r[6], "kosdak_long": r[7]}
    total_eval = int(r[8]) if r[8] is not None else 0
    return cash, sig, mr, total_eval


def load_holdings(conn, acct_no, access_token, app_key, app_secret):
    """trading_plan == 'h', trail_tp = 'L' 대상"""
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            A.code, 
            A.name, 
            A.current_price, 
            A.eval_sum, 
            A.purchase_price,
            COALESCE(A.avail_amount, A.purchase_amount, 0) AS avail_qty
        FROM "stockBalance_stock_balance" A 
        WHERE acct_no = %s 
        AND proc_yn = 'Y'
        AND trading_plan = 'h'
        AND COALESCE(eval_sum, 0) > 0
    """, (str(acct_no),))
    rows = cur.fetchall()
    cur.close()
    out = []
    for code, name, cur_p, eval_sum, pur_p, avail in rows:

        prev_day_info = get_prev_day_info(
            code,
            datetime.now().strftime("%Y%m%d"),
            access_token,
            app_key,
            app_secret,
            conn
        )

        if prev_day_info is None:
            print(f"[{name}-{code}] 전일 일봉 데이터 미존재")
            continue

        prev_low = prev_day_info['low_price']

        # 전일 저가 현재가 이탈시
        if prev_low > int(cur_p or 0):
            out.append({"code": code, "name": name, "prev_low_price" : prev_low,
                        "current_price": int(cur_p or 0), "eval_sum": int(eval_sum or 0),
                        "purchase_price": int(float(pur_p or 0)),
                        "avail_qty": int(avail or 0)})
    return out


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


def record_sell(conn, acct_no, h, qty, est_price, order_no, sell_amt):
    """매도 접수 후 trading_trail 에 완료-매도(trail_tp='4') 레코드 INSERT.
    est_price 는 동시호가 예상체결가(현재가). 정확한 체결가는 주문체결 배치가 보정."""
    now = datetime.now()
    yd, hms = now.strftime("%Y%m%d"), now.strftime("%H%M%S")
    basic_price = h["purchase_price"]
    trail_amt = est_price * qty
    trail_rate = round((est_price - basic_price) / basic_price * 100, 2) if basic_price else 0
    loss_amt = int((basic_price - est_price) * qty)
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO trading_trail (
                acct_no,
                code, 
                name, 
                trail_day, 
                trail_dtm, 
                trail_tp,
                stop_price, 
                target_price, 
                trail_plan,
                basic_price, 
                basic_qty, 
                basic_amt,
                proc_min, 
                trade_tp, 
                exit_price, 
                loss_amt,
                trail_price, 
                trail_qty, 
                trail_amt, 
                trail_rate, 
                trade_result,
                order_no, 
                crt_dt, 
                mod_dt
            ) VALUES (%s,%s,%s,%s,%s,'4',0,0,%s,%s,%s,%s,%s,'M',0,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (acct_no, 
              h["code"], 
              h["name"], 
              yd, 
              hms,
              str(int(qty)), 
              basic_price, 
              qty, 
              basic_price * qty,
              hms, 
              loss_amt,
              est_price, 
              qty, 
              trail_amt, 
              trail_rate, 
              f"시장비율 리밸런싱 매도({sell_amt:,}원)",
              str(order_no or ""), 
              now, 
              now))
        conn.commit()
        cur.close()
    except Exception as e:
        conn.rollback()
        print(f"[record_sell 오류] {h['name']}[{h['code']}]: {e}")


# ────────────────────────────────────────────────────────────────────────────
# 라이브 실행
# ────────────────────────────────────────────────────────────────────────────
def _make_strength_fn(ac, cache):
    at, ak, asec = ac["access_token"], ac["app_key"], ac["app_secret"]

    def fn(code):
        if code in cache:
            return cache[code]
        time.sleep(0.25)
        ohlcv = _fetch_daily_ohlcv(at, ak, asec, code)
        ssts  = _fetch_short_selling(at, ak, asec, code)
        inv   = _fetch_investor(at, ak, asec, code)
        price = _fetch_cur_price_out(at, ak, asec, code)
        chart  = _calc_chart_score(ohlcv)
        supply = _calc_supply_score(ohlcv, inv, price, ssts=ssts)
        chart  = 50 if chart  is None else chart
        supply = 50 if supply is None else supply
        # 합산(차트+수급) strength = W_SUPPLY * supply + W_CHART * chart
        # st = W_SUPPLY * supply + W_CHART * chart
        # 차트 strength = W_CHART * chart
        st = W_CHART * chart
        # 수급 strength = W_SUPPLY * supply
        # st = W_SUPPLY * supply
        cache[code] = st
        return st
    return fn

def send_telegram(token, chat_id, text, parse_mode='HTML'):
    """Bot API 직접 호출로 텔레그램 메시지 발송.
    python-telegram-bot(v20+)이 비동기 전용 API 라 이 배치의 동기 흐름과 맞지 않아 requests 로 우회."""
    if not token or not chat_id:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={"chat_id": chat_id, "text": text, "parse_mode": parse_mode},
            timeout=10,
        )
    except Exception as te:
        print(f"텔레그램 발송 실패: {te}")


def is_business_day(check_date: datetime, conn) -> bool:
    """
    DB 기준 영업일 여부 확인
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT is_business_day(%s)",
        (check_date,)
    )
    result = cur.fetchone()
    cur.close()

    return bool(result[0])

# 일별주문체결조회
def get_my_complete(access_token, app_key, app_secret, acct_no, code, order_no):

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "TTTC0081R",                            # (3개월이내) TTTC0081R, (3개월이전) CTSC9215R
               "custtype": "P"}
    params = {
            'CANO': acct_no,                                    # 종합계좌번호 계좌번호 체계(8-2)의 앞 8자리
            'ACNT_PRDT_CD':"01",                                # 계좌상품코드 계좌번호 체계(8-2)의 뒤 2자리
            'SORT_DVSN': "01",                                  # 00: 최근 순, 01: 과거 순, 02: 최근 순
            'INQR_STRT_DT': datetime.now().strftime('%Y%m%d'),  # 조회시작일(8자리)
            'INQR_END_DT': datetime.now().strftime('%Y%m%d'),   # 조회종료일(8자리)
            # 'INQR_STRT_DT': "20250522",  # 조회시작일(8자리)
            # 'INQR_END_DT': "20250522",   # 조회종료일(8자리)
            'SLL_BUY_DVSN_CD': "00",                            # 매도매수구분코드 00 : 전체 / 01 : 매도 / 02 : 매수
            'PDNO': code,                                       # 종목번호(6자리) ""공란입력 시, 전체
            'ORD_GNO_BRNO': "",                                 # 주문채번지점번호 ""공란입력 시, 전체
            'ODNO': order_no,                                   # 주문번호 ""공란입력 시, 전체
            'CCLD_DVSN': "00",                                  # 체결구분 00 전체, 01 체결, 02 미체결
            'INQR_DVSN': "01",                                  # 조회구분 00 역순, 01 정순
            'INQR_DVSN_1': "",                                  # 조회구분1 없음: 전체, 1: ELW, 2: 프리보드
            'INQR_DVSN_3': "00",                                # 조회구분3 00 전체, 01 현금, 02 신용, 03 담보, 04 대주, 05 대여, 06 자기융자신규/상환, 07 유통융자신규/상환
            'EXCG_ID_DVSN_CD': "ALL",                           # 거래소ID구분코드 KRX : KRX, NXT : NXT, SOR (Smart Order Routing) : SOR, ALL : 전체
            'CTX_AREA_NK100': "",
            'CTX_AREA_FK100': ""
    }
    PATH = "uapi/domestic-stock/v1/trading/inquire-daily-ccld"
    URL = f"{URL_BASE}/{PATH}"

    try:
        res = requests.get(URL, headers=headers, params=params, verify=False, timeout=10)
        ar = resp.APIResp(res)

        # 응답에 output1이 있는지 확인
        body = ar.getBody()
        return body.output1 if hasattr(body, 'output1') else []

    except Exception as e:
        print("일별주문체결조회 중 오류 발생:", e)
        return []

# 주식주문(정정취소)
def order_cancel_revice(access_token, app_key, app_secret, acct_no, cncl_dv, order_no, order_qty, order_price, excg_id):

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "TTTC0013U",            # TTTC0013U[실전투자], VTTC0013U[모의투자]
               "custtype": "P"
    }
    params = {
               "CANO": acct_no,
               "ACNT_PRDT_CD": "01",
               "KRX_FWDG_ORD_ORGNO": "06010",
               "ORGN_ODNO": order_no,
               "ORD_DVSN": "00" if int(order_price) > 0 else "01",  # 지정가 : 00, 시장가 : 01
               "RVSE_CNCL_DVSN_CD": cncl_dv,    # 정정 : 01, 취소 : 02
               "ORD_QTY": str(order_qty),
               "ORD_UNPR": str(order_price),
               "QTY_ALL_ORD_YN": "Y",           # 전량 : Y, 일부 : N
               "EXCG_ID_DVSN_CD": excg_id       # 한국거래소 : KRX, 대체거래소 (넥스트레이드) : NXT, SOR (Smart Order Routing) : SOR
    }
    PATH = "uapi/domestic-stock/v1/trading/order-rvsecncl"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.post(URL, data=json.dumps(params), headers=headers, verify=False, timeout=10)
    ar = resp.APIResp(res)
    if ar.isOK():
        return ar.getBody().output
    else:
        ar.printError()
        return None

# 매도 주문정보 존재시 취소 처리
def sell_order_cancel_proc(access_token, app_key, app_secret, acct_no, code):

    result_msgs = []

    try:
        # 일별주문체결 조회
        output1 = get_my_complete(access_token, app_key, app_secret, acct_no, code, '')

        if len(output1) > 0:

            tdf = pd.DataFrame(output1)
            d = tdf[['odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty', 'excg_id_dvsn_cd']]
            order_no = 0

            for i, name in enumerate(d.index):

                # 매도주문 잔여수량 존재시
                if d['sll_buy_dvsn_cd'][i] == "01":

                    # 잔량 존재 AND cncl_yn != 'Y' (Y=취소, 그 외 공백/N 모두 취소 아님으로 처리)
                    if int(d['rmn_qty'][i]) > 0 and d['cncl_yn'][i] != 'Y':
                        order_no = int(d['odno'][i])
                        ord_excg_id = d['excg_id_dvsn_cd'][i] if 'excg_id_dvsn_cd' in d.columns else None

                        # 주문취소
                        c = order_cancel_revice(access_token, app_key, app_secret, acct_no, "02", str(order_no), "0", "0", "KRX")
                        if c is not None and c['ODNO'] != "":
                            print("매도주문취소 완료")

                        else:
                            print("매도주문취소 실패")
                            msg = f"[{d['prdt_name'][i]}] 매도주문취소 실패"
                            result_msgs.append(msg)

    except Exception as e:
        print('매도주문취소 오류.', e)
        msg = f"[{code}] 매도주문취소 오류 - {str(e)}"
        result_msgs.append(msg)

    final_message = result_msgs if result_msgs else "success"

    return final_message

def process_account(nick):
    """계좌별 독립 DB 연결로 병렬 처리"""
    conn = db.connect(conn_string)
    token = chat_id = None   # account() 실패 시에도 except 블록에서 안전하게 참조되도록 선-초기화
    try:
        ac = account(nick, conn)
        acct_no      = ac['acct_no']
        access_token = ac['access_token']
        app_key      = ac['app_key']
        app_secret   = ac['app_secret']
        token        = ac['bot_token2']
        chat_id      = ac['chat_id']

        cash, _sig, mr, total_eval = load_fund_signals(conn, acct_no)
        holdings = load_holdings(conn, acct_no, access_token, app_key, app_secret)
        transfer_cash_need = total_excess(cash, total_eval, mr)
        if not holdings:
            print(f"[{nick}] 시장비율({int(mr):,}%) 현금: {cash:,}원, 현금전환필요: {transfer_cash_need:,}원 홀딩 대상 없음 → 스킵")
            telegram_text = (f"✅ [{nick}] 시장비율({int(mr):,}%) 현금: {cash:,}원, 현금전환필요: {transfer_cash_need:,}원 홀딩 대상 없음")
            send_telegram(token, chat_id, telegram_text)
            return

        cache = {}
        strength_fn = _make_strength_fn(ac, cache)

        def quality_fn(code):
            return quality_score_from_history(conn, code)

        orders, excess = build_rebalance_orders(holdings, cash, mr, strength_fn, quality_fn)
        print(f"[{nick}] 시장비율({int(mr):,}%) 리밸런싱 평가총액: {total_eval:,}원, 현금: {cash:,}원, 현금전환필요: {transfer_cash_need:,}원, 현금전환: {excess:,}원 매도대상: {len(orders)}건")

        if len(orders) > 1:
            sold_cnt, fail_cnt, sold_amt = 0, 0, 0
            for h, qty in orders:

                tag = (f"{h['name']}[{h['code']}] {qty}주 "
                    f"(strength={h.get('strength',0):.0f} quality={h.get('quality',0):.0f} "
                    f"priority={h.get('sell_priority',0):.0f}) 예상 {h['current_price']*qty:,}원")

                # 매도 주문정보 존재시 취소 처리
                if sell_order_cancel_proc(ac["access_token"], ac["app_key"], ac["app_secret"], str(acct_no), h["code"],) == 'success':
                    # 매도 : 시장가 주문
                    ar = order_cash(False, ac["access_token"], ac["app_key"], ac["app_secret"], str(acct_no), h["code"], "01", qty, 0, excg_id="KRX")
                    if ar.isOK():
                        out = ar.getBody().output
                        order_no = (out or {}).get("ODNO", "")
                        print(f"  ✅ 매도접수 {tag} 주문번호={str(int(order_no))}")
                        record_sell(conn, acct_no, h, qty, h["current_price"], str(int(order_no)), h['current_price']*qty)
                        sold_cnt += 1
                        sold_amt += h['current_price'] * qty

                        telegram_text = (
                            f"✅ [{nick}] 시장비율({int(mr):,}%) 리밸런싱 매도\n"
                            f"{h['name']}[<code>{h['code']}</code>] {qty:,}주*{h['current_price']:,}원={h['current_price']*qty:,}원\n"
                            f"매도산정기준: {h.get('sell_priority',0):.0f} 주문번호: {str(int(order_no))}"
                        )
                        send_telegram(token, chat_id, telegram_text)
                    else:
                        print(f"  ❌ 매도실패 {tag}: {ar.getErrorCode()} {ar.getErrorMessage()}")
                        fail_cnt += 1

                        telegram_text = (
                            f"❌ [{nick}] 매도실패\n"
                            f"{h['name']}[<code>{h['code']}</code>] {qty:,}주\n"
                            f"{ar.getErrorCode()} {ar.getErrorMessage()}"
                        )
                        send_telegram(token, chat_id, telegram_text)
                time.sleep(0.3)

            if orders:
                summary_text = (f"📊 [{nick}] 성공 {sold_cnt}건 / 실패 {fail_cnt}건, 현금전환필요: {transfer_cash_need:,}원, 총 매도금액: {sold_amt:,}원")
                send_telegram(token, chat_id, summary_text)
        else:
            summary_text = (f"📊 [{nick}] 시장비율({int(mr):,}%) 평가총액: {total_eval:,}원, 현금: {cash:,}원, 현금전환필요: {transfer_cash_need:,}원")
            send_telegram(token, chat_id, summary_text)        
    except Exception as e:
        print(f"[{nick}] 계좌 처리 오류: {e}")
        send_telegram(token, chat_id, f"⚠️ [{nick}] 계좌 처리 오류\n{e}")

    finally:
        conn.close()

if __name__ == "__main__":

    # 영업일 확인용 임시 연결 (스레드 진입 전 단일 사용)
    _conn_check = db.connect(conn_string)
    try:
        _is_business = is_business_day(today, _conn_check)
    finally:
        _conn_check.close()

    if _is_business:

        nickname_list = ['phills2', 'phills75', 'yh480825', 'mamalong', 'phills13', 'phills15', 'worry106']

        # 7개 계좌 병렬 처리
        with ThreadPoolExecutor(max_workers=len(nickname_list)) as account_executor:
            account_futures = {
                account_executor.submit(process_account, nick): nick
                for nick in nickname_list
            }
            for future in as_completed(account_futures):
                nick = account_futures[future]
                try:
                    future.result()
                except Exception as e:
                    print(f"[{nick}] 계좌 최종 오류: {e}")


