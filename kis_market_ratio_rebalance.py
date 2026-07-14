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
  ① 수급점수 + 차트점수        (simul_server 점수식 재사용, 각 0~100) → 종합점수 낮은 종목 우선 매도
  ② KOSPI/KOSDAQ 시장 흐름 추종 (시장별 목표비율로 버킷 초과분 분리 → 약한 시장을 더 매도)
  invest_point 성장/가치 점수(quality)는 참고용으로 계산·표시만 하고 매도 우선순위 산정에서는 제외.

정책 결정 (합의)
  - 우선 '일 단위(horizon='D')'만 처리. 주/월은 horizon_market_ratio에 구조만 남겨둠.
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

import requests
import psycopg2 as db
import kis_api_resp as resp   # Batch 공용 응답 파서

# ────────────────────────────────────────────────────────────────────────────
# 설정
# ────────────────────────────────────────────────────────────────────────────
URL_BASE    = "https://openapi.koreainvestment.com:9443"
conn_string = "dbname='fund_risk_mng' host='192.168.50.81' port='5432' user='postgres' password='asdf1234'"

# 리밸런싱 가중치 (튜닝 포인트)
W_SUPPLY      = 0.5    # strength = W_SUPPLY*수급 + W_CHART*차트
W_CHART       = 0.5
# sell_priority = 100 - strength (invest_point/quality는 매도 우선순위에서 제외, 참고용 표시만 유지)
TOP_CUT       = 70     # sell_priority 이 값 이상이면 종목당 전량 매도 허용, 아니면 일 최대 50%
PER_NAME_CAP  = 0.5    # 종목당 1일 최대 매도 비중 (평가금액 대비)

REBAL_WINDOW  = (dtime(15, 18), dtime(15, 29))   # 동시호가 접수 창

requests.packages.urllib3.disable_warnings()     # verify=False 경고 억제


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


def order_cash(buy_flag, access_token, app_key, app_secret, acct_no, stock_code,
               ord_dvsn, order_qty, order_price, excg_id="KRX"):
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
_SIGN = {"01": +1, "02": -1, "03": +1, "04": -1, "05": +1, "06": -1}


def horizon_market_ratio(sig, market, horizon="D"):
    """market: 'kospi'|'kosdak', horizon: 'D'|'W'|'M' → 0~100 시장별 목표 주식비율.
    단일 market_ratio 만 저장되므로 6개 timeframe 신호에서 horizon별로 재산출."""
    s = _SIGN.get(sig.get(f"{market}_short"), 0)
    m = _SIGN.get(sig.get(f"{market}_mid"),   0)
    l = _SIGN.get(sig.get(f"{market}_long"),  0)
    if   horizon == "D": raw, span = s,             1     # 일: 단기
    elif horizon == "W": raw, span = s + 2*m,       3     # 주: 단기+중기
    else:                raw, span = s + 2*m + 3*l, 6     # 월: 단기+중기+장기(장기 가중)
    return int(50 + raw / span * 50)


def market_excess(cash, eval_kospi, eval_kosdaq, sig, horizon="D"):
    """공유 현금을 시장별 평가금액 비중으로 배분 → 시장별 독립 초과분(원) 산출.
    반환: (kospi_excess, kosdaq_excess)"""
    tot = eval_kospi + eval_kosdaq
    if tot <= 0:
        return 0, 0
    w_kp, w_kd = eval_kospi / tot, eval_kosdaq / tot
    base_kp = eval_kospi  + cash * w_kp
    base_kd = eval_kosdaq + cash * w_kd
    ex_kp = max(0, eval_kospi  - base_kp * horizon_market_ratio(sig, "kospi",  horizon) / 100)
    ex_kd = max(0, eval_kosdaq - base_kd * horizon_market_ratio(sig, "kosdak", horizon) / 100)
    return int(ex_kp), int(ex_kd)


def classify_market(price_out):
    """inquire_price output 의 rprs_mrkt_kor_name 으로 시장 분류.
    ETF/ETN 은 추종지수(코스닥150 등)와 무관하게 실제로는 KOSPI 시장에 상장되므로 최우선 kospi 처리.
    'KSQ150'(코스닥150 소속 표기) 등 코스닥 계열 표기는 kosdak, 그 외 기본값은 kospi."""
    name = (price_out or {}).get("rprs_mrkt_kor_name", "") or ""
    upper = name.upper()
    if "ETF" in upper or "ETN" in upper:
        return "kospi"
    return "kosdak" if ("코스닥" in name or "KOSDAQ" in upper or "KSQ" in upper) else "kospi"


def sell_priority(strength):
    """수급+차트 종합점수(strength) 약할수록(낮을수록) 우선순위 높음.
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
        cap_amt = h["eval_sum"] if h["sell_priority"] >= TOP_CUT else h["eval_sum"] * PER_NAME_CAP
        amt = min(cap_amt, excess - filled)
        qty = int(amt // cur)
        qty = min(qty, avail)
        if qty > 0:
            orders.append((h, qty))
            filled += qty * cur
    return orders


def build_rebalance_orders(holdings, cash, sig, strength_fn, quality_fn, horizon="D"):
    """코어 엔진. holdings 각 dict: code,name,market,eval_sum,current_price,avail_qty,purchase_price.
    strength_fn(code)->0~100, quality_fn(code)->0~100 주입.
    quality(invest_point)는 참고용으로 h['quality']에 기록만 하고 sell_priority 산정에는 쓰지 않음.
    반환: [(holding, qty), ...]"""
    eval_kp = sum(h["eval_sum"] for h in holdings if h["market"] == "kospi")
    eval_kd = sum(h["eval_sum"] for h in holdings if h["market"] == "kosdak")
    ex_kp, ex_kd = market_excess(cash, eval_kp, eval_kd, sig, horizon)

    orders = []
    for mk, excess in (("kospi", ex_kp), ("kosdak", ex_kd)):
        if excess <= 0:
            continue
        bucket = [h for h in holdings if h["market"] == mk]
        for h in bucket:
            st = strength_fn(h["code"])
            ql = quality_fn(h["code"])
            h["strength"], h["quality"] = st, ql
            h["sell_priority"] = sell_priority(st)
        orders += allocate(bucket, excess)
    return orders, (ex_kp, ex_kd)


# ────────────────────────────────────────────────────────────────────────────
# 데이터 접근 (라이브)
# ────────────────────────────────────────────────────────────────────────────
def load_fund_signals(conn, acct_no):
    """stockFundMng_stock_fund_mng → (cash, sig dict, market_ratio)."""
    cur = conn.cursor()
    cur.execute("""
        SELECT prvs_rcdl_excc_amt, market_ratio,
               kospi_short, kospi_mid, kospi_long,
               kosdak_short, kosdak_mid, kosdak_long
        FROM "stockFundMng_stock_fund_mng" WHERE acct_no = %s
    """, (str(acct_no),))
    r = cur.fetchone()
    cur.close()
    if not r:
        return 0, {}, None
    cash = int(r[0]) if r[0] is not None else 0
    mr = float(r[1]) if r[1] is not None else None
    sig = {"kospi_short": r[2], "kospi_mid": r[3], "kospi_long": r[4],
           "kosdak_short": r[5], "kosdak_mid": r[6], "kosdak_long": r[7]}
    return cash, sig, mr


def load_holdings(conn, acct_no):
    """트레이딩 종목(trading_plan NOT IN ('i')) 조회. 'h' 포함(정책 결정)."""
    cur = conn.cursor()
    cur.execute("""
        SELECT code, name, current_price, eval_sum, purchase_price,
               COALESCE(avail_amount, purchase_amount, 0) AS avail_qty,
               COALESCE(NULLIF(trading_plan,''), 'as') AS trading_plan
        FROM "stockBalance_stock_balance"
        WHERE acct_no = %s AND proc_yn = 'Y'
          AND (trading_plan IS NULL OR trading_plan NOT IN ('i'))
          AND COALESCE(eval_sum, 0) > 0
    """, (str(acct_no),))
    rows = cur.fetchall()
    cur.close()
    out = []
    for code, name, cur_p, eval_sum, pur_p, avail, plan in rows:
        out.append({"code": code, "name": name,
                    "current_price": int(cur_p or 0), "eval_sum": int(eval_sum or 0),
                    "purchase_price": int(float(pur_p or 0)),
                    "avail_qty": int(avail or 0), "trading_plan": plan})
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
    base = {"실적 턴어라운드(적자→흑자 전환 예상)": 90, "성장 가속": 85, "성장 지속": 70,
            "실적 개선(저점 통과 추정)": 60, "역성장/둔화": 25}.get(trend, 45)
    fwd_adj = max(-20, min(20, float(fwd or 0))) * 0.5        # ±10
    val_adj = 15 if vsig else 0                                # 저평가+실적↑
    up_adj  = max(0, min(40, float(upside or 0))) * 0.25       # 0~10
    return max(0.0, min(100.0, base + fwd_adj + val_adj + up_adj))


def has_pending_sell(conn, acct_no, code):
    """당일 미체결 매도 방지: trading_trail 활성 추적('1','2','L') 여부."""
    cur = conn.cursor()
    cur.execute("""
        SELECT 1 FROM trading_trail
        WHERE acct_no = %s AND code = %s
          AND trail_day = TO_CHAR(now(), 'YYYYMMDD')
          AND trail_tp IN ('1','2','L') LIMIT 1
    """, (acct_no, code))
    hit = cur.fetchone() is not None
    cur.close()
    return hit


def record_sell(conn, acct_no, h, qty, est_price, order_no, horizon):
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
                acct_no, code, name, trail_day, trail_dtm, trail_tp,
                stop_price, target_price, trail_plan,
                basic_price, basic_qty, basic_amt,
                proc_min, trade_tp, exit_price, loss_amt,
                trail_price, trail_qty, trail_amt, trail_rate, trade_result,
                order_no, crt_dt, mod_dt
            ) VALUES (%s,%s,%s,%s,%s,'4', 0,0,%s, %s,%s,%s, %s,'M', 0,%s,
                      %s,%s,%s,%s,%s, %s,%s,%s)
        """, (acct_no, h["code"], h["name"], yd, hms,
              str(int(qty)), basic_price, qty, basic_price * qty,
              hms, loss_amt,
              est_price, qty, trail_amt, trail_rate, f"시장비율 리밸런싱 매도({horizon})",
              str(order_no or ""), now, now))
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
            return cache[code]["strength"]
        time.sleep(0.25)
        ohlcv = _fetch_daily_ohlcv(at, ak, asec, code)
        ssts  = _fetch_short_selling(at, ak, asec, code)
        inv   = _fetch_investor(at, ak, asec, code)
        price = _fetch_cur_price_out(at, ak, asec, code)
        chart  = _calc_chart_score(ohlcv)
        supply = _calc_supply_score(ohlcv, inv, price, ssts=ssts)
        chart  = 50 if chart  is None else chart
        supply = 50 if supply is None else supply
        st = W_SUPPLY * supply + W_CHART * chart
        cache[code] = {"strength": st, "market": classify_market(price)}
        return st
    return fn


def run(nick, horizon="D", dry_run=False, force=False):
    """장마감 동시호가 시장비율 리밸런싱 실행."""
    conn = db.connect(conn_string)
    try:
        now_t = datetime.now().time()
        if not force and not (REBAL_WINDOW[0] <= now_t <= REBAL_WINDOW[1]):
            print(f"[{nick}] 동시호가 창(15:18~15:29) 아님 → 스킵 (now={now_t})")
            return

        ac = account(nick, conn)
        acct_no = ac["acct_no"]

        cash, sig, mr = load_fund_signals(conn, acct_no)
        holdings = load_holdings(conn, acct_no)
        if not holdings:
            print(f"[{nick}] 트레이딩 보유 없음 → 스킵")
            return

        # 시장 분류 (price fetch 캐시와 공유)
        cache = {}
        strength_fn = _make_strength_fn(ac, cache)
        for h in holdings:
            po = _fetch_cur_price_out(ac["access_token"], ac["app_key"], ac["app_secret"], h["code"])
            h["market"] = classify_market(po)
            time.sleep(0.1)

        def quality_fn(code):
            return quality_score_from_history(conn, code)

        orders, (ex_kp, ex_kd) = build_rebalance_orders(
            holdings, cash, sig, strength_fn, quality_fn, horizon)

        print(f"[{nick}] cash={cash:,} market_ratio={mr} "
              f"KOSPI초과={ex_kp:,} KOSDAQ초과={ex_kd:,} 매도후보={len(orders)}건")

        for h, qty in orders:
            if has_pending_sell(conn, acct_no, h["code"]):
                print(f"  · {h['name']}[{h['code']}] 활성 추적 존재 → 매도 스킵")
                continue
            tag = (f"{h['name']}[{h['code']}] {qty}주 "
                   f"(strength={h.get('strength',0):.0f} quality={h.get('quality',0):.0f} "
                   f"priority={h.get('sell_priority',0):.0f}) 예상 {h['current_price']*qty:,}원")
            if dry_run:
                print(f"  [DRY] 매도 {tag}")
                continue
            ar = order_cash(False, ac["access_token"], ac["app_key"], ac["app_secret"],
                            str(acct_no), h["code"], "01", qty, 0, excg_id="KRX")
            if ar.isOK():
                out = ar.getBody().output
                order_no = (out or {}).get("ODNO", "")
                print(f"  ✅ 매도접수 {tag} ODNO={order_no}")
                record_sell(conn, acct_no, h, qty, h["current_price"], order_no, horizon)
            else:
                print(f"  ❌ 매도실패 {tag}: {ar.getErrorCode()} {ar.getErrorMessage()}")
            time.sleep(0.3)
    finally:
        conn.close()


# ────────────────────────────────────────────────────────────────────────────
# 백테스트 훅 (kis_trading_set_simul 계열 테이블 기반)
# ────────────────────────────────────────────────────────────────────────────
SIMUL_ACCT       = "SIMUL"          # trading_trail_simul.acct_no
SIMUL_DLY_ACCT   = "74346047"     # dly_*_simul 의 acct (실제 값에 맞게 조정)
API_NICK         = "phills2"   

def _sim_market_of(conn, code, ac=None):
    """백테스트 시장 분류. 마스터 테이블 우선, 없으면 라이브 API 폴백(근사)."""
    # 선택: 종목 마스터가 있으면 사용 (예: stock_item.market). 없으면 API.
    if ac:
        po = _fetch_cur_price_out(ac["access_token"], ac["app_key"], ac["app_secret"], code)
        return classify_market(po)
    return "kospi"


def simulate_rebalance(sim_date, horizon="D", strength_mode="neutral", ac=None,
                       dly_acct=SIMUL_DLY_ACCT):
    """
    sim_date(YYYYMMDD) 종가 기준 리밸런싱을 시뮬레이션.
      - 보유:   dly_trading_balance_simul (balance_price=매입가, balance_qty, balance_amt=보유금액, value_amt=수익금액)
      - 현금:   dly_acct_balance_simul.prvs_excc_amt
      - 신호:   dly_acct_balance (실제 그 날의 kospi_*/kosdak_* / market_ratio)
      - 기준①:  strength_mode='neutral'(50 고정) | 'pit'(로컬 일봉 이력 필요)
      - 기준③:  analysis_history point-in-time (run_at <= sim_date)
    매도분은 trading_trail_simul(trail_tp='4')에 기록하고 dly_trading_balance_simul 잔량을 차감.
    dly_acct_balance_simul 재집계는 기존 simul 스크립트가 담당.
    """
    conn = db.connect(conn_string)
    try:
        
        # API 조회용 계좌 정보
        ac = None
        if API_NICK:
            try:
                ac = account(API_NICK, conn)
            except Exception as e_ac:
                print(f"[SIMUL] API 계좌({API_NICK}) 조회 실패 : {e_ac}")
        
        cur = conn.cursor()
        # 1) 보유 포지션
        cur.execute("""
            SELECT code, name, balance_price, balance_qty, balance_amt, value_amt
            FROM public.dly_trading_balance_simul
            WHERE acct_no = %s AND balance_day = %s AND use_yn = 'Y' AND balance_qty > 0
        """, (dly_acct, sim_date))
        rows = cur.fetchall()
        if not rows:
            print(f"[SIMUL {sim_date}] 보유 없음 → 스킵")
            return {"date": sim_date, "orders": []}

        holdings = []
        for code, name, bprice, bqty, bamt, vamt in rows:
            bqty = int(bqty or 0)
            eval_sum = int(bamt or 0) + int(vamt or 0)              # 평가금액 = 보유금액 + 수익금액
            cur_price = int(round(eval_sum / bqty)) if bqty else 0  # 종가 근사
            holdings.append({"code": code, "name": name or code,
                             "current_price": cur_price, "eval_sum": eval_sum,
                             "purchase_price": int(float(bprice or 0)),
                             "avail_qty": bqty,
                             "market": _sim_market_of(conn, code, ac)})

        # 2) 현금 (sim_date 시점 예수금)
        cur.execute("""
            SELECT prvs_excc_amt FROM dly_acct_balance_simul
            WHERE acct = %s AND dt = %s ORDER BY last_chg_date DESC LIMIT 1
        """, (dly_acct, sim_date))
        cr = cur.fetchone()
        cash = int(cr[0]) if cr and cr[0] is not None else 0

        # 3) 그 날의 시장 신호 (실제 dly_acct_balance)
        cur.execute("""
            SELECT kospi_short, kospi_mid, kospi_long,
                   kosdak_short, kosdak_mid, kosdak_long, market_ratio
            FROM dly_acct_balance WHERE dt = %s
            ORDER BY last_chg_date DESC NULLS LAST LIMIT 1
        """, (sim_date,))
        sr = cur.fetchone()
        if not sr:
            print(f"[SIMUL {sim_date}] dly_acct_balance 신호 없음 → 스킵")
            return {"date": sim_date, "orders": []}
        sig = {"kospi_short": sr[0], "kospi_mid": sr[1], "kospi_long": sr[2],
               "kosdak_short": sr[3], "kosdak_mid": sr[4], "kosdak_long": sr[5]}
        mr = sr[6]
        cur.close()

        # 4) 점수 함수 (point-in-time)
        def quality_fn(code):
            return quality_score_from_history(conn, code, as_of=sim_date)

        def strength_fn(code):
            if strength_mode == "pit":
                # TODO: 로컬 일봉 이력 테이블에서 sim_date 이하로 잘라 _calc_*_score 계산.
                #       현재 라이브는 KIS 현재가 API(당일)라 PIT 백테스트 불가 → 이력 테이블 필요.
                return 50.0
            return 50.0  # neutral: 기준①을 백테스트에서 중립 처리(순위는 기준③이 좌우)

        # 5) 주문 산출
        orders, (ex_kp, ex_kd) = build_rebalance_orders(
            holdings, cash, sig, strength_fn, quality_fn, horizon)

        print(f"[SIMUL {sim_date}] cash={cash:,} mr={mr} "
              f"KOSPI초과={ex_kp:,} KOSDAQ초과={ex_kd:,} 매도={len(orders)}건")

        # 6) 체결 반영 (종가 매도) → 기존 trading_trail_simul 활성 행 UPDATE
        #    (simul_server.py /api/sell · _do_simul_sell_update 와 동일 패턴: INSERT 아님)
        applied = []
        for h, qty in orders:
            sell_price = h["current_price"]
            cur2 = conn.cursor()
            cur2.execute("""
                SELECT basic_price, basic_qty, trail_dtm, trail_tp
                FROM trading_trail_simul
                WHERE acct_no = %s AND trail_day = %s AND code = %s
                  AND trail_tp IN ('1','2','3','L','P')
                ORDER BY trail_dtm DESC LIMIT 1
            """, (SIMUL_ACCT, sim_date, h["code"]))
            row = cur2.fetchone()
            if not row:
                print(f"  ⚠ {h['name']}[{h['code']}] 활성 trading_trail_simul 행 없음 → 매도 스킵")
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
                  AND trail_tp IN ('1','2','3','L','P')
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
                    sell_qty = %s     
                WHERE acct_no = %s AND balance_day = %s AND code = %s
            """, (qty, basic_price * qty, pnl, qty, dly_acct, sim_date, h["code"]))
            conn.commit()
            cur2.close()
            applied.append({"code": h["code"], "qty": qty, "sell_price": sell_price, "pnl": pnl})
            print(f"  · 매도 {h['name']}[{h['code']}] {qty}주 @ {sell_price:,} 손익 {pnl:,} "
                  f"(quality={h.get('quality',0):.0f}, trail_tp {cur_trail_tp}→{new_trail_tp})")

        return {"date": sim_date, "market_ratio": mr,
                "excess": {"kospi": ex_kp, "kosdaq": ex_kd},
                "orders": applied,
                "realized_pnl": sum(a["pnl"] for a in applied)}
    finally:
        conn.close()


def backtest(start_date, end_date, horizon="D", strength_mode="neutral"):
    """기간 백테스트: 영업일마다 simulate_rebalance 반복. (prev_business_day_char 활용)"""
    conn = db.connect(conn_string)
    days = []
    try:
        cur = conn.cursor()
        # dly_acct_balance 에 존재하는 영업일만 순회
        cur.execute("""
            SELECT DISTINCT dt FROM dly_acct_balance
            WHERE dt BETWEEN %s AND %s ORDER BY dt
        """, (start_date, end_date))
        days = [r[0] for r in cur.fetchall()]
        cur.close()
    finally:
        conn.close()

    total_pnl, results = 0, []
    for d in days:
        r = simulate_rebalance(d, horizon=horizon, strength_mode=strength_mode)
        results.append(r)
        total_pnl += r.get("realized_pnl", 0)
    print(f"\n[백테스트 {start_date}~{end_date}] 영업일 {len(days)}일 "
          f"누적 리밸런싱 실현손익 {total_pnl:,}원")
    return {"days": len(days), "total_realized_pnl": total_pnl, "results": results}


# ────────────────────────────────────────────────────────────────────────────
# 엔트리포인트
# ────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    p = argparse.ArgumentParser(description="시장비율 동시호가 리밸런싱")
    sub = p.add_subparsers(dest="cmd")

    pr = sub.add_parser("run", help="라이브 실행")
    pr.add_argument("--nick", required=True)
    pr.add_argument("--horizon", default="D", choices=["D", "W", "M"])
    pr.add_argument("--dry-run", action="store_true")
    pr.add_argument("--force", action="store_true", help="시간창 무시(테스트)")

    ps = sub.add_parser("sim", help="단일일 백테스트")
    ps.add_argument("--date", required=True, help="YYYYMMDD")
    ps.add_argument("--horizon", default="D")
    ps.add_argument("--strength", default="neutral", choices=["neutral", "pit"])

    pb = sub.add_parser("backtest", help="기간 백테스트")
    pb.add_argument("--start", required=True, help="YYYYMMDD")
    pb.add_argument("--end", required=True, help="YYYYMMDD")
    pb.add_argument("--horizon", default="D")
    pb.add_argument("--strength", default="neutral", choices=["neutral", "pit"])

    args = p.parse_args()
    if args.cmd == "run":
        run(args.nick, horizon=args.horizon, dry_run=args.dry_run, force=args.force)
    elif args.cmd == "sim":
        print(json.dumps(simulate_rebalance(args.date, horizon=args.horizon,
                                            strength_mode=args.strength),
                         ensure_ascii=False, indent=2, default=str))
    elif args.cmd == "backtest":
        backtest(args.start, args.end, horizon=args.horizon, strength_mode=args.strength)
    else:
        p.print_help()
