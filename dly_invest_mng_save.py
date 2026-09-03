"""
dly_invest_mng_save.py

invest_mng(proc_yn='Y') 대상 종목을 당일(8자리) 스냅샷으로 dly_invest_mng 에 생성한다.

  - dt          : 실행일자(YYYYMMDD, 인자 없으면 오늘)
  - price       : KIS API 실시간 현재가 (조회 실패 시 invest_mng.price 로 폴백)
  - remain_rate : 상승잔존율 = round((high_price - price) / price * 100, 1)
                  현재 시점 현재가 기준으로 재계산 (simul_server 투자관리 현황 목록과 동일 식)

그 외 컬럼(name, market, 재무지표, 체크값 등)은 invest_mng 값을 그대로 복사한다.
(dt, code) 기준 upsert 이므로 같은 날 재실행하면 최신 현재가로 갱신된다.

dly_invest_mng 생성 후, 현재가를 조회한 종목은 invest_mng 의 price / remain_rate 도
동일한 값으로 갱신한다(현재가 조회 실패로 폴백한 종목은 기존 값 유지).

실행:  python dly_invest_mng_save.py [YYYYMMDD]
"""
import sys
import json
import time
import traceback
from datetime import datetime

import psycopg2 as db
import requests

# ─────────────────────────────────────────
# 설정
# ─────────────────────────────────────────
API_NICK  = "phills2"                                   # 현재가 조회용 KIS 계좌 닉네임
URL_BASE  = "https://openapi.koreainvestment.com:9443"
SLEEP_SEC = 0.2                                         # KIS API 호출 간 간격

conn_string = "dbname='fund_risk_mng' host='192.168.50.81' port='5432' user='postgres' password='asdf1234'"
conn = db.connect(conn_string)

requests.packages.urllib3.disable_warnings()

today = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y%m%d")
if len(today) != 8 or not today.isdigit():
    print(f"[dly_invest_mng] 날짜 인자 형식 오류(YYYYMMDD): {today}")
    sys.exit(1)


# ─────────────────────────────────────────
# 인증
# ─────────────────────────────────────────
def auth(app_key, app_secret):
    res = requests.post(
        f"{URL_BASE}/oauth2/tokenP",
        headers={"content-type": "application/json"},
        data=json.dumps({"grant_type": "client_credentials",
                         "appkey": app_key, "appsecret": app_secret}),
        verify=False, timeout=10,
    )
    data = res.json()
    if "access_token" not in data:
        raise ValueError(f"KIS 인증 실패: {data.get('msg1', data)}")
    return data["access_token"]


def account(nickname):
    cur = conn.cursor()
    cur.execute("""
        SELECT acct_no, access_token, app_key, app_secret, token_publ_date,
               substr(token_publ_date, 0, 9) AS token_day
        FROM "stockAccount_stock_account"
        WHERE nick_name = %s
    """, (nickname,))
    row = cur.fetchone()
    cur.close()
    if not row:
        raise ValueError(f"KIS 계좌 정보 없음: {nickname}")
    acct_no, access_token, app_key, app_secret, token_publ_date, token_day = row
    real_today = datetime.now().strftime("%Y%m%d")
    if (datetime.now() - datetime.strptime(token_publ_date, '%Y%m%d%H%M%S')).days >= 1 or token_day != real_today:
        access_token    = auth(app_key, app_secret)
        token_publ_date = datetime.now().strftime('%Y%m%d%H%M%S')
        cur2 = conn.cursor()
        cur2.execute("""
            UPDATE "stockAccount_stock_account"
            SET access_token = %s, token_publ_date = %s, last_chg_date = %s
            WHERE acct_no = %s
        """, (access_token, token_publ_date, datetime.now(), acct_no))
        conn.commit()
        cur2.close()
    return {'acct_no': acct_no, 'access_token': access_token,
            'app_key': app_key, 'app_secret': app_secret}


# ─────────────────────────────────────────
# 현재가 조회
# ─────────────────────────────────────────
def fetch_cur_price(ac, code):
    """KIS inquire-price(FHKST01010100) 로 현재가 조회. 실패 시 None."""
    try:
        r = requests.get(
            f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-price",
            headers={
                "Content-Type": "application/json",
                "authorization": f"Bearer {ac['access_token']}",
                "appkey":    ac['app_key'],
                "appsecret": ac['app_secret'],
                "tr_id":     "FHKST01010100",
                "custtype":  "P",
            },
            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code},
            verify=False, timeout=10,
        )
        d = r.json()
        if d.get('rt_cd') == '0' and d.get('output'):
            return int(str(d['output'].get('stck_prpr', '')).replace(',', ''))
    except Exception as e:
        print(f"  [WARN] {code} 현재가 조회 실패: {e}")
    return None


def calc_remain_rate(high_price, price):
    """상승잔존율(%) = (상단가 - 현재가) / 현재가 * 100, 소수 1자리."""
    if high_price is None or not price or price <= 0:
        return None
    return round((float(high_price) - price) / price * 100, 1)


# ─────────────────────────────────────────
# 메인
# ─────────────────────────────────────────
UPSERT_SQL = """
    INSERT INTO public.dly_invest_mng
        (dt, code, name, market, size, industry, mktcap, main_business,
         invest_issue, invest_point, invest_risk, report_dt, check_dt,
         price, high_price, sales_amt, ep_sales_amt,
         remain_rate, dividend_rate, sales_rate,
         value_check, dividend_check, growth_check, proc_yn, crt_dt, mod_dt)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (dt, code) DO UPDATE SET
        name          = EXCLUDED.name,
        market        = EXCLUDED.market,
        size          = EXCLUDED.size,
        industry      = EXCLUDED.industry,
        mktcap        = EXCLUDED.mktcap,
        main_business = EXCLUDED.main_business,
        invest_issue  = EXCLUDED.invest_issue,
        invest_point  = EXCLUDED.invest_point,
        invest_risk   = EXCLUDED.invest_risk,
        report_dt     = EXCLUDED.report_dt,
        check_dt      = EXCLUDED.check_dt,
        price         = EXCLUDED.price,
        high_price    = EXCLUDED.high_price,
        sales_amt     = EXCLUDED.sales_amt,
        ep_sales_amt  = EXCLUDED.ep_sales_amt,
        remain_rate   = EXCLUDED.remain_rate,
        dividend_rate = EXCLUDED.dividend_rate,
        sales_rate    = EXCLUDED.sales_rate,
        value_check   = EXCLUDED.value_check,
        dividend_check = EXCLUDED.dividend_check,
        growth_check  = EXCLUDED.growth_check,
        proc_yn       = EXCLUDED.proc_yn,
        mod_dt        = EXCLUDED.mod_dt
"""

# dly_invest_mng 생성 후 원본 invest_mng 도 동일 현재가/상승잔존율로 갱신
INVEST_MNG_UPDATE_SQL = """
    UPDATE public.invest_mng
    SET price = %s, remain_rate = %s, mod_dt = %s
    WHERE code = %s AND proc_yn = 'Y'
"""

try:
    ac = account(API_NICK)

    cur = conn.cursor()
    cur.execute("""
        SELECT code, name, market, size, industry, mktcap, main_business,
               invest_issue, invest_point, invest_risk, report_dt, check_dt,
               price, high_price, sales_amt, ep_sales_amt,
               remain_rate, dividend_rate, sales_rate,
               value_check, dividend_check, growth_check, proc_yn
        FROM public.invest_mng
        WHERE proc_yn = 'Y'
        ORDER BY code
    """)
    rows = cur.fetchall()
    cur.close()

    print(f"[dly_invest_mng] {today} 스냅샷 생성 시작 - 대상 {len(rows)}건")

    now = datetime.now()
    ok, fallback, failed, synced = 0, 0, 0, 0
    wcur = conn.cursor()

    for (code, name, market, size, industry, mktcap, main_business,
         invest_issue, invest_point, invest_risk, report_dt, check_dt,
         prev_price, high_price, sales_amt, ep_sales_amt,
         prev_remain_rate, dividend_rate, sales_rate,
         value_check, dividend_check, growth_check, proc_yn) in rows:

        cur_price = fetch_cur_price(ac, code)
        time.sleep(SLEEP_SEC)

        if cur_price and cur_price > 0:
            price = cur_price
            remain_rate = calc_remain_rate(high_price, price)
            if remain_rate is None:
                remain_rate = prev_remain_rate
            ok += 1
            tag = ""
        else:
            # 현재가 조회 실패 → invest_mng 저장값으로 폴백
            price = prev_price
            remain_rate = prev_remain_rate
            fallback += 1
            tag = " (폴백)"

        if price is None:
            print(f"  [SKIP] {code} {name} - 현재가/저장가 모두 없음")
            failed += 1
            continue

        wcur.execute(UPSERT_SQL, (
            today, code, name, market, size, industry, mktcap, main_business,
            invest_issue, invest_point, invest_risk, report_dt, check_dt,
            price, high_price, sales_amt, ep_sales_amt,
            remain_rate, dividend_rate, sales_rate,
            value_check, dividend_check, growth_check, proc_yn, now, now,
        ))

        # 현재가를 조회한 종목은 invest_mng 원본도 동일 값으로 갱신
        if not tag:
            wcur.execute(INVEST_MNG_UPDATE_SQL, (price, remain_rate, now, code))
            synced += 1

        print(f"  {code} {name} price={price:,} high={high_price} "
              f"remain_rate={remain_rate}%{tag}")

    conn.commit()
    wcur.close()

    print(f"[dly_invest_mng] 완료 - 현재가 {ok}건 / 폴백 {fallback}건 / 스킵 {failed}건 "
          f"/ invest_mng 갱신 {synced}건")

except Exception as e:
    conn.rollback()
    print(f"[dly_invest_mng] 오류: {e}")
    traceback.print_exc()
    sys.exit(1)
finally:
    conn.close()
