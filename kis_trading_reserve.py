"""
kis_trading_reserve.py
================================================================================
계좌별 주식예약주문(예약매도) 정합성 배치 — 1일 2회 실행.

  07:20 실행 (mode="0720")
    계좌의 미취소 예약주문을 전량 조회해, 지정가 예약주문 중 주문가격이 당일
    상한가~하한가 범위(하한가 이상 ~ 상한가 이하)를 벗어난 건을 취소한다.
    시장가 예약(주문가 0)은 가격범위 체크 대상이 아니므로 스킵.

  15:50 실행 (mode="1550")
    stockBalance_stock_balance 에 reserve_price/reserve_qty/reserve_date 가
    설정된 종목 중, 실제 계좌에 매도 예약주문(미취소)이 존재하지 않는 종목에
    대해서만 해당 값 그대로 예약매도 주문을 신규 등록한다(이미 있으면 중복 등록 안 함).

실행
  python kis_trading_reserve.py
  동작 모드는 인자가 아닌 실행 시각(HHMM) 기준으로 자동 결정된다: 06~10시대→0720(가격이탈 취소),
  그 외 시각→1550(누락 등록). Task Scheduler 에 07:20, 15:50 두 트리거로 등록해 실행한다.

주의
  - reservebot.py 의 order_reserve()/order_reserve_cancel_revice()/order_reserve_complete()
    (tr_id: CTSC0008U 등록 / CTSC0013U 정정·CTSC0009U 취소 / CTSC0004R 조회) 를 복제해 사용한다.
    import 시 배치가 자동 실행되는 파일들(reservebot.py, kis_holding_item_total.py)은
    직접 import 하지 않고 필요한 KIS 함수만 이 파일에 복제한다.
  - 예약주문은 15:40 ~ 다음 영업일 07:30 에만 유효(KIS 서버 제약, 23:40~00:10 제외).
  - python-telegram-bot(v20+)이 비동기 전용 API 라 동기 배치 흐름과 맞지 않아
    텔레그램은 requests 로 Bot API 를 직접 호출한다(kis_wave_rebalance.py 와 동일 방식).
================================================================================
"""

import time
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
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
# KIS 함수 (reservebot.py 에서 복제 — import 시 부작용 방지)
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
    """FHKST01010100 현재가 시세. 당일 상한가(stck_mxpr)/하한가(stck_llam) 포함.
    동시호가/장운영시간(09:00~15:30)에는 J(KRX), 그 외에는 NX 기준."""
    t = datetime.now().strftime("%H%M")
    params = {"FID_COND_MRKT_DIV_CODE": "J" if "0900" <= t < "1530" else "NX",
              "FID_INPUT_ISCD": code}
    res = requests.get(f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-price",
                       headers=_headers(access_token, app_key, app_secret, "FHKST01010100"),
                       params=params, verify=False, timeout=10)
    return resp.APIResp(res).getBody().output


# 주식예약주문 : 15시 40분 ~ 다음 영업일 07시 30분까지 가능(23시 40분 ~ 0시 10분 서버초기화 제외)
def order_reserve(access_token, app_key, app_secret, acct_no, code, ord_qty, ord_price,
                   trade_cd, ord_dvsn_cd, reserve_end_dt):
    """주식예약주문 등록. tr_id CTSC0008U(국내예약매수입력/주문예약매도입력)."""
    params = {
        "CANO": acct_no,
        "ACNT_PRDT_CD": "01",
        "PDNO": code,
        "ORD_QTY": ord_qty,                  # 주문주식수
        "ORD_UNPR": ord_price,               # 주문단가 : 시장가인 경우 0
        "SLL_BUY_DVSN_CD": trade_cd,         # 매도매수구분코드 : 01 매도, 02 매수
        "ORD_DVSN_CD": ord_dvsn_cd,          # 주문구분코드 : 00 지정가, 01 시장가
        "ORD_OBJT_CBLC_DVSN_CD": "10",       # 주문대상잔고구분코드 : 10 현금
        "RSVN_ORD_END_DT": reserve_end_dt,   # 예약주문종료일자(YYYYMMDD)
    }
    res = requests.post(f"{URL_BASE}/uapi/domestic-stock/v1/trading/order-resv",
                        headers=_headers(access_token, app_key, app_secret, "CTSC0008U"),
                        data=json.dumps(params), verify=False, timeout=10)
    ar = resp.APIResp(res)
    if not ar.isOK():
        raise RuntimeError(f"{ar.getErrorCode()} {ar.getErrorMessage()}")
    return ar.getBody().output


# 주식예약주문정정취소 : 15시 40분 ~ 다음 영업일 07시 30분까지 가능
def order_reserve_cancel_revice(access_token, app_key, app_secret, acct_no, reserve_cd, code,
                                 ord_qty, ord_price, trade_cd, ord_dvsn_cd, reserve_end_dt, rsvn_ord_seq):
    """주식예약주문정정취소. reserve_cd '01'=정정(CTSC0013U), 그 외=취소(CTSC0009U)."""
    tr_id = "CTSC0013U" if reserve_cd == "01" else "CTSC0009U"
    params = {
        "CANO": acct_no,
        "ACNT_PRDT_CD": "01",
        "PDNO": code,
        "ORD_QTY": ord_qty,
        "ORD_UNPR": ord_price,
        "SLL_BUY_DVSN_CD": trade_cd,
        "ORD_DVSN_CD": ord_dvsn_cd,
        "ORD_OBJT_CBLC_DVSN_CD": "10",
        "RSVN_ORD_END_DT": reserve_end_dt,
        "RSVN_ORD_SEQ": rsvn_ord_seq,         # 예약주문순번
    }
    res = requests.post(f"{URL_BASE}/uapi/domestic-stock/v1/trading/order-resv-rvsecncl",
                        headers=_headers(access_token, app_key, app_secret, tr_id),
                        data=json.dumps(params), verify=False, timeout=10)
    ar = resp.APIResp(res)
    if not ar.isOK():
        raise RuntimeError(f"{ar.getErrorCode()} {ar.getErrorMessage()}")
    return ar.getBody().output


# 주식예약주문조회 : 15시 40분 ~ 다음 영업일 07시 30분까지 가능
def order_reserve_complete(access_token, app_key, app_secret, reserve_strt_dt, reserve_end_dt, acct_no, code):
    """계좌 예약주문 목록 조회. tr_id CTSC0004R."""
    params = {
        "RSVN_ORD_ORD_DT": reserve_strt_dt,   # 예약주문시작일자
        "RSVN_ORD_END_DT": reserve_end_dt,    # 예약주문종료일자
        "RSVN_ORD_SEQ": "",
        "TMNL_MDIA_KIND_CD": "00",
        "CANO": acct_no,
        "ACNT_PRDT_CD": "01",
        "PRCS_DVSN_CD": "0",                  # 처리구분코드 : 전체 0
        "CNCL_YN": "Y",
        "PDNO": code if code != "" else "",
        "SLL_BUY_DVSN_CD": "",
        "CTX_AREA_FK200": "",
        "CTX_AREA_NK200": "",
    }
    res = requests.get(f"{URL_BASE}/uapi/domestic-stock/v1/trading/order-resv-ccnl",
                       headers=_headers(access_token, app_key, app_secret, "CTSC0004R"),
                       params=params, verify=False, timeout=10)
    ar = resp.APIResp(res)
    if not ar.isOK():
        raise RuntimeError(f"{ar.getErrorCode()} {ar.getErrorMessage()}")
    return ar.getBody().output


def get_previous_business_day(day, conn):
    cur = conn.cursor()
    cur.execute("select prev_business_day_char(%s)", (day,))
    result = cur.fetchall()
    cur.close()
    return result[0][0]


def is_business_day(check_date: datetime, conn) -> bool:
    """DB 기준 영업일 여부 확인."""
    cur = conn.cursor()
    cur.execute("SELECT is_business_day(%s)", (check_date,))
    result = cur.fetchone()
    cur.close()
    return bool(result[0])


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


# ────────────────────────────────────────────────────────────────────────────
# 코어 로직
# ────────────────────────────────────────────────────────────────────────────
def _reserve_scan_range(conn):
    """예약주문 조회 시작/종료일 — 오늘 ~ 1개월 후(직전 영업일로 보정)."""
    reserve_strt_dt = datetime.now().strftime("%Y%m%d")
    reserve_end_dt = (datetime.now() + relativedelta(months=1)).strftime("%Y%m%d")
    reserve_end_dt = get_previous_business_day(reserve_end_dt, conn)
    return reserve_strt_dt, reserve_end_dt


def _to_yyyymmdd(v):
    """DB reserve_date 값(date 타입/문자열 모두 대응) → 'YYYYMMDD' 문자열."""
    return v.strftime("%Y%m%d") if hasattr(v, "strftime") else str(v)


def cancel_out_of_range_reserves(nick, ac, conn):
    """07:20 — 당일 상한가~하한가 범위를 벗어난 지정가 예약주문 취소."""
    acct_no      = ac['acct_no']
    access_token = ac['access_token']
    app_key      = ac['app_key']
    app_secret   = ac['app_secret']
    token, chat_id = ac['bot_token2'], ac['chat_id']

    reserve_strt_dt, reserve_end_dt = _reserve_scan_range(conn)
    output = order_reserve_complete(access_token, app_key, app_secret,
                                     reserve_strt_dt, reserve_end_dt, str(acct_no), "")
    if not output:
        print(f"[{nick}] 예약주문 없음")
        return

    df = pd.DataFrame(output)
    active = df[df['cncl_ord_dt'] == ""]   # 미취소 예약만 대상
    if active.empty:
        print(f"[{nick}] 취소 대상(미취소) 예약주문 없음")
        return

    price_cache = {}
    cancel_cnt = 0
    for _, row in active.iterrows():
        code      = row['pdno']
        name      = row.get('kor_item_shtn_name', code)
        ord_price = int(row['ord_rsvn_unpr'])
        if ord_price <= 0:
            continue   # 시장가 예약은 가격범위 체크 대상 아님

        ord_qty     = int(row['ord_rsvn_qty'])
        trade_cd    = row['sll_buy_dvsn_cd']       # 01 매도, 02 매수
        rsvn_seq    = str(int(row['rsvn_ord_seq']))
        rsvn_end_dt = row['rsvn_end_dt']

        if code not in price_cache:
            try:
                time.sleep(0.25)
                price_cache[code] = inquire_price(access_token, app_key, app_secret, code)
            except Exception as e:
                print(f"[{nick}] {name}[{code}] 현재가 조회 오류: {e}")
                price_cache[code] = None
        price_out = price_cache[code]
        if not price_out:
            continue
        upper = int(price_out.get('stck_mxpr') or 0)
        lower = int(price_out.get('stck_llam') or 0)
        if upper <= 0 or lower <= 0:
            continue
        if lower <= ord_price <= upper:
            continue   # 당일 가격범위 이내 → 취소 불필요

        try:
            cncl_result = order_reserve_cancel_revice(
                access_token, app_key, app_secret, str(acct_no), "02",
                code, str(ord_qty), str(ord_price), trade_cd,
                "01" if ord_price == 0 else "00", rsvn_end_dt, rsvn_seq
            )
        except Exception as e:
            print(f"  ❌ [{nick}] {name}[{code}] 예약취소 오류: {e}")
            send_telegram(token, chat_id, f"❌ [{nick}] {name}[<code>{code}</code>] 예약취소 오류\n{e}")
            continue

        if cncl_result and cncl_result.get('NRML_PRCS_YN', '') == 'Y':
            cancel_cnt += 1
            dvsn_label = "매도" if trade_cd == '01' else "매수"
            print(f"  ✅ [{nick}] {name}[{code}] {dvsn_label}예약 가격이탈 취소 "
                  f"(예약가:{ord_price:,} 범위:{lower:,}~{upper:,}) 예약번호:{rsvn_seq}")
            telegram_text = (
                f"⚠️ [{nick}] {name}[<code>{code}</code>] {dvsn_label}예약 가격이탈 취소\n"
                f"예약가:{ord_price:,}원 (당일 범위 {lower:,}~{upper:,}원) 수량:{ord_qty:,}주\n"
                f"예약번호:{rsvn_seq}"
            )
            send_telegram(token, chat_id, telegram_text)
        else:
            print(f"  ❌ [{nick}] {name}[{code}] 예약취소 실패: {cncl_result}")
        time.sleep(0.3)

    print(f"[{nick}] 가격이탈 예약취소 완료: {cancel_cnt}건")
    if cancel_cnt:
        send_telegram(token, chat_id, f"📊 [{nick}] 가격이탈 예약취소 {cancel_cnt}건 처리")


def register_missing_reserves(nick, ac, conn):
    """15:50 — reserve_price/qty/date 는 있는데 실제 매도 예약주문이 없는 종목 신규 등록."""
    acct_no      = ac['acct_no']
    access_token = ac['access_token']
    app_key      = ac['app_key']
    app_secret   = ac['app_secret']
    token, chat_id = ac['bot_token2'], ac['chat_id']

    cur = conn.cursor()
    cur.execute("""
        SELECT code, name, reserve_price, reserve_qty, reserve_date
        FROM "stockBalance_stock_balance"
        WHERE acct_no = %s AND proc_yn = 'Y'
          AND reserve_price IS NOT NULL
          AND reserve_qty IS NOT NULL
          AND reserve_date IS NOT NULL
    """, (str(acct_no),))
    rows = cur.fetchall()
    cur.close()
    if not rows:
        print(f"[{nick}] 예약등록 대상(reserve_price/qty/date) 없음")
        return

    reserve_strt_dt, reserve_end_dt = _reserve_scan_range(conn)
    try:
        output = order_reserve_complete(access_token, app_key, app_secret,
                                         reserve_strt_dt, reserve_end_dt, str(acct_no), "")
    except Exception as e:
        print(f"[{nick}] 예약주문 조회 오류: {e}")
        send_telegram(token, chat_id, f"⚠️ [{nick}] 예약주문 조회 오류\n{e}")
        return

    df = pd.DataFrame(output) if output else pd.DataFrame(columns=['pdno', 'sll_buy_dvsn_cd', 'cncl_ord_dt'])

    reg_cnt, fail_cnt = 0, 0
    for code, name, reserve_price, reserve_qty, reserve_date in rows:
        matched = df[(df['pdno'] == code) & (df['sll_buy_dvsn_cd'] == '01') & (df['cncl_ord_dt'] == "")]
        if not matched.empty:
            continue   # 이미 등록된(미취소) 매도 예약주문 존재 → 중복 등록 방지

        ord_price  = int(reserve_price)
        ord_qty    = int(reserve_qty)
        ord_end_dt = _to_yyyymmdd(reserve_date)
        if ord_qty <= 0:
            continue
        ord_dvsn_cd = "01" if ord_price == 0 else "00"

        try:
            rsv_result = order_reserve(
                access_token, app_key, app_secret, str(acct_no), code,
                str(ord_qty), str(ord_price), "01", ord_dvsn_cd, ord_end_dt
            )
        except Exception as e:
            fail_cnt += 1
            print(f"  ❌ [{nick}] {name}[{code}] 예약매도등록 오류: {e}")
            send_telegram(token, chat_id, f"❌ [{nick}] {name}[<code>{code}</code>] 예약매도등록 오류\n{e}")
            time.sleep(0.3)
            continue

        if rsv_result and rsv_result.get('RSVN_ORD_SEQ', ''):
            reg_cnt += 1
            print(f"  ✅ [{nick}] {name}[{code}] 예약매도등록 {ord_qty:,}주 @ "
                  f"{ord_price:,}원 종료일:{ord_end_dt} 예약번호:{rsv_result['RSVN_ORD_SEQ']}")
            telegram_text = (
                f"✅ [{nick}] {name}[<code>{code}</code>] 예약매도등록\n"
                f"{ord_qty:,}주 * {ord_price:,}원, 종료일:{ord_end_dt}\n"
                f"예약번호:{rsv_result['RSVN_ORD_SEQ']}"
            )
            send_telegram(token, chat_id, telegram_text)
        else:
            fail_cnt += 1
            print(f"  ❌ [{nick}] {name}[{code}] 예약매도등록 실패: {rsv_result}")
            send_telegram(token, chat_id, f"❌ [{nick}] {name}[<code>{code}</code>] 예약매도등록 실패")
        time.sleep(0.3)

    print(f"[{nick}] 예약매도 신규등록: 성공 {reg_cnt}건 / 실패 {fail_cnt}건")
    if reg_cnt or fail_cnt:
        send_telegram(token, chat_id, f"📊 [{nick}] 예약매도 신규등록 성공 {reg_cnt}건 / 실패 {fail_cnt}건")


def process_account(nick, mode):
    """계좌별 독립 DB 연결로 병렬 처리."""
    conn = db.connect(conn_string)
    token = chat_id = None   # account() 실패 시에도 except 블록에서 안전하게 참조되도록 선-초기화
    try:
        ac = account(nick, conn)
        token, chat_id = ac['bot_token2'], ac['chat_id']

        if mode == "0720":
            cancel_out_of_range_reserves(nick, ac, conn)
        else:
            register_missing_reserves(nick, ac, conn)
    except Exception as e:
        print(f"[{nick}] 계좌 처리 오류: {e}")
        send_telegram(token, chat_id, f"⚠️ [{nick}] 계좌 처리 오류\n{e}")
    finally:
        conn.close()


if __name__ == "__main__":
    # 실행 시각(HHMM) 기준으로 동작 모드 결정 — 06~10시대→0720(가격이탈 취소), 그 외→1550(누락 등록)
    _now_hhmm = datetime.now().strftime("%H%M")
    mode = "0720" if "0600" <= _now_hhmm < "1100" else "1550"

    # 영업일 확인용 임시 연결 (스레드 진입 전 단일 사용)
    _conn_check = db.connect(conn_string)
    try:
        _is_business = is_business_day(today, _conn_check)
    finally:
        _conn_check.close()

    if _is_business:

        nickname_list = ['phills2', 'phills75', 'yh480825', 'mamalong', 'phills13', 'phills15', 'chichipa', 'honeylong', 'worry106']

        # 7개 계좌 병렬 처리
        with ThreadPoolExecutor(max_workers=len(nickname_list)) as account_executor:
            account_futures = {
                account_executor.submit(process_account, nick, mode): nick
                for nick in nickname_list
            }
            for future in as_completed(account_futures):
                nick = account_futures[future]
                try:
                    future.result()
                except Exception as e:
                    print(f"[{nick}] 계좌 최종 오류: {e}")
