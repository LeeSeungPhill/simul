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


def _calc_peak_trough_trend(highs: list, closes: list, lows: list, dates: list) -> dict | None:
    """일봉 고가/저가 리스트(날짜 오름차순) 기준 지그재그 고점/저점으로 현재 추세와 그 시작일 계산.
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

def _kis_headers(access_token, app_key, app_secret, tr_id):
    return {
        "Content-Type": "application/json",
        "authorization": f"Bearer {access_token}",
        "appkey":    app_key,
        "appsecret": app_secret,
        "tr_id":     tr_id,
        "custtype":  "P",
    }

def _fetch_daily_ohlcv(access_token, app_key, app_secret, code):
    """FHKST01010400: 일봉 OHLCV 최근 100거래일 (output 리스트, 최신→과거 내림차순).
    필드: stck_bsop_date, stck_clpr, stck_oprc, stck_hgpr, stck_lwpr, acml_vol"""
    try:
        r = requests.get(
            f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-daily-price",
            headers=_kis_headers(access_token, app_key, app_secret, "FHKST01010400"),
            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code,
                    "FID_PERIOD_DIV_CODE": "D", "FID_ORG_ADJ_PRC": "1"},
            verify=False, timeout=10
        )
        d = r.json()
        rows = d.get('output') or []
        return rows if isinstance(rows, list) else []
    except Exception:
        return []
    
def _format_date(yyyymmdd) -> str:
    """'YYYYMMDD' → 'YYYY/MM/DD'. 파싱 실패 시 원본 그대로 반환."""
    try:
        return datetime.strptime(str(yyyymmdd), "%Y%m%d").strftime("%Y/%m/%d")
    except (ValueError, TypeError):
        return yyyymmdd


def get_stock_trend(stock_code: str, access_token: str, app_key: str, app_secret: str) -> dict | None:
    """종목의 현재 추세(Uptrend/Downtrend/Sideways) 조회"""
    rows = _fetch_daily_ohlcv(access_token, app_key, app_secret, stock_code)
    if not rows:
        return None
    rows = list(reversed(rows))  # KIS 응답(최신→과거) → 날짜 오름차순
    dates  = [r.get('stck_bsop_date') for r in rows]
    highs = [int(r.get('stck_hgpr') or 0) for r in rows]
    closes = [int(r.get('stck_clpr') or 0) for r in rows]
    lows = [int(r.get('stck_lwpr') or 0) for r in rows]
    return _calc_peak_trough_trend(highs, closes, lows, dates) 

def load_holdings(conn, acct_no, access_token, app_key, app_secret):
    cur = conn.cursor()
    cur.execute("""
        SELECT code, name FROM "stockBalance_stock_balance" WHERE acct_no = %s AND proc_yn = 'Y' AND COALESCE(eval_sum, 0) > 0
        UNION
        SELECT code, name FROM public."interestItem_interest_item" WHERE acct_no = %s AND code NOT IN ('0001', '1001')
    """, (str(acct_no),str(acct_no),))
    rows = cur.fetchall()
    cur.close()
    out = []
    for code, name in rows:

        stock_trend_info = get_stock_trend(
            code,
            access_token,
            app_key,
            app_secret
        )

        if stock_trend_info is None:
            print(f"[{name}-{code}] 추세 데이터 미존재")
            continue

        _trend_up = bool(stock_trend_info and stock_trend_info.get('trend') == 'Uptrend')
        _trend_down = bool(stock_trend_info and stock_trend_info.get('trend') == 'Downtrend')
        _trend_ref_price = (stock_trend_info.get('ref_price') or 0) if stock_trend_info else 0
        _start_date = _format_date(stock_trend_info.get('start_date'))
        if _trend_up:
            print(f"{name}[{code}] 현재 상승추세({_start_date}~, 기준가:{_trend_ref_price:,}) → 추세기준 감지")
            out.append({"code": code, "name": name, "trend" : "상승", "start_date" : _start_date, "trend_ref_price": int(stock_trend_info.get('ref_price') or 0)})
        elif _trend_down:
            print(f"{name}[{code}] 현재 하락추세({_start_date}~, 기준가:{_trend_ref_price:,}) → 추세기준 감지")
            out.append({"code": code, "name": name, "trend" : "하락", "start_date" : _start_date, "trend_ref_price": int(stock_trend_info.get('ref_price') or 0)})
        
    return out

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

        holdings = load_holdings(conn, acct_no, access_token, app_key, app_secret)
        if not holdings:
            print(f"[{nick}] 추세 체크 대상 없음 → 스킵")
            return

        up_cnt = 0
        down_cnt = 0
        for h in holdings:

            telegram_text = (
                f"✅ [{nick}] {h['name']}[<code>{h['code']}</code>]-{h['trend']}추세 기준가:{h['trend_ref_price']:,}원 추세시작일:{h['start_date']}~"
            )
            send_telegram(token, chat_id, telegram_text)
            if h['trend'] == "상승":
                up_cnt += 1
            if h['trend'] == "하락":
                down_cnt += 1    
            time.sleep(0.3)

        if holdings:
            _trend_parts = []
            if up_cnt >= 1:
                _trend_parts.append(f"상승추세 {up_cnt}건")
            if down_cnt >= 1:
                _trend_parts.append(f"하락추세 {down_cnt}건")
            summary_text = f"📊 [{nick}] 추세동향 {', '.join(_trend_parts)}\n"
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

        # nickname_list = ['phills2', 'phills75', 'yh480825', 'mamalong', 'phills13', 'phills15', 'worry106']
        nickname_list = ['phills13']

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


