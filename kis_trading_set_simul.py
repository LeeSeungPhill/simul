"""
kis_trading_set_simul.py
kis_trading_set.py 와 동일한 구조.
다음 영업일 기준으로 전일 trading_trail_simul 기준 레코드를 당일 trading_trail_simul 로 생성.
실제 KIS 계좌잔고 조회 없이 시뮬레이션 데이터만 사용.
"""
import psycopg2 as db
import requests
import json
from datetime import datetime, timedelta
import time
import traceback
import sys

# ─────────────────────────────────────────
# 설정
# ─────────────────────────────────────────
SIMUL_ACCT     = "SIMUL"
SIMUL_TABLE    = "trading_trail_simul"
SIMUL_DLY_ACCT   = "74346047"       # dly_acct_balance_simul acct 키
SIMUL_ACTIVE_TPS = ['1', '2', '3', 'L', 'P']
INITIAL_CAPITAL  = 20_000_000
API_NICK    = "phills2"           # 가격 조회용 KIS API 계좌 닉네임 (없으면 None)
URL_BASE    = "https://openapi.koreainvestment.com:9443"

conn_string = "dbname='fund_risk_mng' host='192.168.50.81' port='5432' user='postgres' password='asdf1234'"
conn        = db.connect(conn_string)

today = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y%m%d")

# ─────────────────────────────────────────
# 인증
# ─────────────────────────────────────────
def auth(APP_KEY, APP_SECRET):
    headers = {"content-type": "application/json"}
    body    = {"grant_type": "client_credentials", "appkey": APP_KEY, "appsecret": APP_SECRET}
    URL     = f"{URL_BASE}/oauth2/tokenP"
    res     = requests.post(URL, headers=headers, data=json.dumps(body), verify=False, timeout=10)
    data    = res.json()
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
    acct_no, access_token, app_key, app_secret, token_publ_date, token_day = row
    _real_today = datetime.now().strftime("%Y%m%d")
    if (datetime.now() - datetime.strptime(token_publ_date, '%Y%m%d%H%M%S')).days >= 1 or token_day != _real_today:
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
    return {'acct_no': acct_no, 'access_token': access_token, 'app_key': app_key, 'app_secret': app_secret}


# ─────────────────────────────────────────
# 영업일 유틸
# ─────────────────────────────────────────
def post_business_day_char(business_day: str) -> str:
    cur = conn.cursor()
    cur.execute("SELECT post_business_day_char(%s::date)", (business_day,))
    res = cur.fetchone()[0]
    cur.close()
    return res


def get_previous_business_day(day: str) -> str:
    cur = conn.cursor()
    cur.execute("SELECT prev_business_day_char(%s)", (day,))
    res = cur.fetchone()[0]
    cur.close()
    return res


# ─────────────────────────────────────────
# 전일 종가·저가 조회 (가격 유효성 검증용)
# ─────────────────────────────────────────
def get_prev_day_price_info(access_token, app_key, app_secret, code, prev_date):
    d_from = (datetime.strptime(prev_date, "%Y%m%d") - timedelta(days=10)).strftime("%Y%m%d")
    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {access_token}",
        "appKey": app_key,
        "appSecret": app_secret,
        "tr_id": "FHKST03010100"
    }
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD":         code,
        "FID_INPUT_DATE_1":       d_from,
        "FID_INPUT_DATE_2":       prev_date,
        "FID_PERIOD_DIV_CODE":    "D",
        "FID_ORG_ADJ_PRC":        "0"
    }
    try:
        res = requests.get(
            f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
            headers=headers, params=params, verify=False, timeout=10
        )
        data = res.json()
        if data.get('rt_cd') == '0' and data.get('output2'):
            for row in data['output2']:
                if row.get('stck_bsop_date', '') == prev_date:
                    return (int(row.get('stck_clpr') or 0),
                            int(row.get('stck_lwpr') or 0),
                            int(row.get('stck_hgpr') or 0))
            first = data['output2'][0]
            return (int(first.get('stck_clpr') or 0),
                    int(first.get('stck_lwpr') or 0),
                    int(first.get('stck_hgpr') or 0))
    except Exception as e:
        print(f"[get_prev_day_price_info] {code} 오류: {e}")
    return 0, 0, 0


# ─────────────────────────────────────────
# 메인 처리
# ─────────────────────────────────────────
try:
    business_day = f"{today[:4]}-{today[4:6]}-{today[6:]}"
    
    trail_day  = post_business_day_char(business_day)
    prev_date  = get_previous_business_day(
        (datetime.strptime(business_day, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    )

    # API 조회용 계좌 정보 (선택 - 가격 검증에만 사용)
    ac = None
    if API_NICK:
        try:
            ac = account(API_NICK)
        except Exception as e_ac:
            print(f"[SIMUL] API 계좌({API_NICK}) 조회 실패 (가격 검증 생략): {e_ac}")

    print(f"[SIMUL] {today} 추적준비 등록 시작 (전일: {prev_date} → 당일: {trail_day})")

    # 전일 trading_trail_simul 조회
    cur_prev = conn.cursor()
    cur_prev.execute(f"""
        SELECT acct_no, name, code, trail_day, trail_dtm, trail_tp,
               basic_price, basic_qty, basic_amt,
               COALESCE(volumn, 0) AS volumn,
               COALESCE(stop_price, 0) AS stop_price,
               COALESCE(target_price, 0) AS target_price,
               COALESCE(trade_tp, 'M') AS trade_tp,
               COALESCE(exit_price, 0) AS exit_price
        FROM {SIMUL_TABLE}
        WHERE acct_no = %s
          AND trail_day = %s
          AND trail_tp IN ('1','2','3','L','P')
    """, (SIMUL_ACCT, prev_date))
    prev_rows = cur_prev.fetchall()
    cur_prev.close()

    if not prev_rows:
        print(f"[SIMUL] 전일({prev_date}) trading_trail_simul 데이터 없음")
    else:
        insert_q = f"""
            INSERT INTO {SIMUL_TABLE} (
                acct_no, name, code, trail_day, trail_dtm, trail_tp,
                basic_price, basic_qty, basic_amt,
                volumn, stop_price, target_price,
                proc_min, trade_tp, exit_price, loss_amt, crt_dt, mod_dt
            ) VALUES (%s,%s,%s,%s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s,%s,%s,%s)
            ON CONFLICT (acct_no, code, trail_day, trail_dtm, trail_tp) DO NOTHING
        """
        cur_ins = conn.cursor()
        inserted_count = 0
        inserted_info  = []

        # 코드별 그룹화 (동일 종목 복수 row 병합)
        code_groups = {}
        for row in prev_rows:
            code_groups.setdefault(row[2], []).append(row)

        for p_code, rows in code_groups.items():
            if len(rows) > 1:
                # 복수 row: 집계하여 단일 row 생성
                trail_tps = [r[5] for r in rows]
                if any(tp in ('1', '2') for tp in trail_tps):
                    next_trail_tp = '1'
                elif any(tp in ('3', 'L') for tp in trail_tps):
                    next_trail_tp = 'L'
                else:
                    next_trail_tp = 'P'

                total_qty      = sum(int(r[7] or 0) for r in rows)
                total_amt      = sum(int(r[8] or 0) for r in rows)
                p_basic_price  = round(total_amt / total_qty) if total_qty > 0 else int(rows[0][6] or 0)
                p_basic_qty    = total_qty
                valid_stops    = [int(r[10]) for r in rows if int(r[10] or 0) > 0]
                valid_exits    = [int(r[13]) for r in rows if int(r[13] or 0) > 0]
                p_stop_price   = min(valid_stops)               if valid_stops  else 0
                p_target_price = max(int(r[11] or 0) for r in rows)
                p_exit_price   = min(valid_exits)               if valid_exits  else 0
                p_name         = rows[0][1]
                p_volumn       = 0
                p_trade_tp     = next((r[12] for r in rows if r[12]), 'M')
                print(f"[SIMUL] {p_code}({p_name}) 동일종목 {len(rows)}건 병합 → trail_tp={next_trail_tp}"
                      f" 매수단가:{p_basic_price:,} 수량:{p_basic_qty:,}")
            else:
                row = rows[0]
                (_, p_name, _, p_trail_day, p_trail_dtm, p_trail_tp,
                 p_basic_price, p_basic_qty, p_basic_amt,
                 p_volumn, p_stop_price, p_target_price,
                 p_trade_tp, p_exit_price) = row

                if p_trail_tp in ('3', 'L'):
                    next_trail_tp = 'L'
                elif p_trail_tp in ('P', 'C', 'U'):
                    next_trail_tp = 'P'
                else:
                    next_trail_tp = '1'

            # 가격 유효성 검증:
            #   stop_price/exit_price > 전일저가 → 전일저가로 조정
            #   전일고가 > target_price → 전일고가로 조정
            stop_price_adj   = int(p_stop_price   or 0)
            exit_price_adj   = int(p_exit_price   or 0)
            target_price_adj = int(p_target_price or 0)
            if ac:
                try:
                    _, prev_low, prev_high = get_prev_day_price_info(
                        ac['access_token'], ac['app_key'], ac['app_secret'], p_code, prev_date
                    )
                    time.sleep(0.2)
                    if prev_low > 0:
                        if stop_price_adj > 0 and stop_price_adj > prev_low:
                            print(f"[SIMUL] {p_code} stop_price({stop_price_adj:,}) > 전일저가({prev_low:,}) → 전일저가로 조정")
                            stop_price_adj = prev_low
                        if exit_price_adj > 0 and exit_price_adj > prev_low:
                            print(f"[SIMUL] {p_code} exit_price({exit_price_adj:,}) > 전일저가({prev_low:,}) → 전일저가로 조정")
                            exit_price_adj = prev_low
                    if prev_high > 0 and target_price_adj > 0 and prev_high > target_price_adj:
                        print(f"[SIMUL] {p_code} 전일고가({prev_high:,}) > target_price({target_price_adj:,}) → 전일고가로 조정")
                        target_price_adj = prev_high
                except Exception as e_p:
                    print(f"[SIMUL] {p_code} 가격 조회 오류 (원본 가격 유지): {e_p}")

            p_basic_qty   = int(p_basic_qty or 0)
            p_basic_price = float(p_basic_price or 0)
            loss_amt      = (p_basic_price - exit_price_adj) * p_basic_qty if p_basic_qty > 0 else 0
            now           = datetime.now()

            try:
                cur_ins.execute(insert_q, (
                    SIMUL_ACCT, p_name, p_code, trail_day, '090000', next_trail_tp,
                    p_basic_price, p_basic_qty, p_basic_price * p_basic_qty,
                    p_volumn, stop_price_adj, target_price_adj,
                    '090000', p_trade_tp, exit_price_adj, loss_amt, now, now
                ))
                if cur_ins.rowcount > 0:
                    inserted_count += 1
                    inserted_info.append({
                        'code': p_code, 'name': p_name, 'trail_tp': next_trail_tp,
                        'basic_price': p_basic_price, 'stop_price': stop_price_adj,
                        'exit_price': exit_price_adj, 'target_price': target_price_adj,
                    })
            except Exception as e_ins:
                print(f"[SIMUL] {p_code} 삽입 오류: {e_ins}")

        conn.commit()
        cur_ins.close()

        skipped = len(code_groups) - inserted_count
        print(
            f"[SIMUL] {today} 추적준비 등록 완료\n"
            f"  전체: {len(prev_rows)}건(전일레코드) / {len(code_groups)}종목 | 생성: {inserted_count}건 | 미생성: {skipped}건"
        )
        for info in inserted_info:
            print(
                f"  └ {info['name']}({info['code']}) trail_tp={info['trail_tp']}"
                f" 매수가:{int(info['basic_price']):,}"
                f" 이탈가:{int(info['stop_price']):,}"
                f" 추세이탈가:{int(info['exit_price']):,}"
                f" 목표가:{int(info['target_price']):,}"
            )

    # ── dly_acct_balance_simul 전일 집계 업데이트 (dly_trading_balance_simul 기반) ──
    try:
        cur_dly = conn.cursor()

        # public.dly_trading_balance_simul 에서 전일 잔고 집계 조회
        cur_dly.execute("""
            SELECT code, balance_price, balance_qty, balance_amt, value_amt
            FROM public.dly_trading_balance_simul
            WHERE acct_no = %s AND balance_day = %s AND use_yn = 'Y'
        """, (SIMUL_DLY_ACCT, prev_date,))
        balance_rows = cur_dly.fetchall()

        if not balance_rows:
            print(f"[SIMUL] dly_trading_balance_simul({prev_date}) 데이터 없음 → dly_acct_balance_simul 스킵")
        else:
            # 보유 포지션(잔여수량 > 0) 집계
            active_rows   = [r for r in balance_rows if int(r[2] or 0) > 0]
            pchs_amt      = sum(int(r[3] or 0) for r in active_rows)                # 보유금액 합계
            evlu_pfls_amt = sum(int(r[4] or 0) for r in active_rows)                # 수익금액 합계
            user_evlu_amt = pchs_amt + evlu_pfls_amt                                # 평가금액 합계 = 보유금액 합계 + 수익금액 합계

            # 전일 실현손익 합계
            cur_dly.execute("""
                SELECT COALESCE(SUM((trail_price - basic_price)*trail_qty), 0)
                FROM trading_trail_simul
                WHERE acct_no = 'SIMUL' AND trail_day = %s
                AND trail_tp IN ('3','4')
                AND trail_price > 0 AND trail_qty > 0
            """, (prev_date,))
            total_profit_loss_amt = int(cur_dly.fetchone()[0])

            # 전전일까지 누적 실현손익 + 전일 합산
            cur_dly.execute("""
                SELECT COALESCE(SUM(total_profit_loss_amt), 0)
                FROM dly_acct_balance_simul
                WHERE acct = %s AND dt < %s
            """, (SIMUL_DLY_ACCT, prev_date))
            cum_profit = int(cur_dly.fetchone()[0]) + total_profit_loss_amt

            # 전전일 예수금, 매수총액
            cur_dly.execute("""
                SELECT prvs_excc_amt, pchs_amt, tot_evlu_amt FROM dly_acct_balance_simul
                WHERE acct = %s AND dt < %s ORDER BY dt DESC LIMIT 1
            """, (SIMUL_DLY_ACCT, prev_date))
            pt_row        = cur_dly.fetchone()
            prev_excc     = int(pt_row[0]) if pt_row else None
            prev_pchs     = int(pt_row[1]) if pt_row else None
            prev_tot      = int(pt_row[2]) if pt_row else None

            # 자본기준: dly_acct_balance_simul 의 prev_excc(전전일 예수금) 사용 (없으면 INITIAL_CAPITAL 폴백)
            base_capital  = prev_excc if prev_excc is not None else INITIAL_CAPITAL
            # 전일 예수금 = 전전일 예수금 - 전일 매수총액 + 전전일 매수총액 + 전일 실현손익
            prvs_excc_amt = base_capital - pchs_amt + prev_pchs + total_profit_loss_amt
            tot_evlu_amt  = prvs_excc_amt + user_evlu_amt
            asst_icdc_amt = (tot_evlu_amt - prev_tot) if prev_tot is not None else 0

            cur_dly.execute("""
                INSERT INTO dly_acct_balance_simul (
                    acct, dt, dnca_tot_amt, prvs_excc_amt,
                    td_buy_amt, td_sell_amt, td_tex_amt,
                    user_evlu_amt, tot_evlu_amt, nass_amt,
                    pchs_amt, evlu_amt, evlu_pfls_amt,
                    ytdt_tot_evlu_amt, asst_icdc_amt,
                    total_profit_loss_amt, buy_psbl_amt, last_chg_date
                ) VALUES (
                    %s, %s, %s, %s, 0, 0, 0,
                    %s, %s, %s, %s, %s, %s, 0, %s, %s, 0, %s
                )
                ON CONFLICT (acct, dt) DO UPDATE SET
                    dnca_tot_amt          = EXCLUDED.dnca_tot_amt,
                    prvs_excc_amt         = EXCLUDED.prvs_excc_amt,
                    user_evlu_amt         = EXCLUDED.user_evlu_amt,
                    tot_evlu_amt          = EXCLUDED.tot_evlu_amt,
                    nass_amt              = EXCLUDED.nass_amt,
                    pchs_amt              = EXCLUDED.pchs_amt,
                    evlu_amt              = EXCLUDED.evlu_amt,
                    evlu_pfls_amt         = EXCLUDED.evlu_pfls_amt,
                    asst_icdc_amt         = EXCLUDED.asst_icdc_amt,
                    total_profit_loss_amt = EXCLUDED.total_profit_loss_amt,
                    last_chg_date         = EXCLUDED.last_chg_date
            """, (
                SIMUL_DLY_ACCT, prev_date,
                base_capital, prvs_excc_amt,
                user_evlu_amt, tot_evlu_amt, tot_evlu_amt,
                pchs_amt, user_evlu_amt, evlu_pfls_amt,
                asst_icdc_amt, total_profit_loss_amt, datetime.now()
            ))
            conn.commit()
            print(
                f"[SIMUL] dly_acct_balance_simul 업데이트 ({prev_date}) "
                f"보유:{len(active_rows)}종목 매수총액:{pchs_amt:,} 평가총액:{user_evlu_amt:,} "
                f"현금:{prvs_excc_amt:,} 자산총액:{tot_evlu_amt:,} "
                f"수익총액:{evlu_pfls_amt:,} 당일실현:{total_profit_loss_amt:,} 누적수익:{cum_profit:,}"
            )
        cur_dly.close()
    except Exception as e_dly:
        conn.rollback()
        print(f"[SIMUL] dly_acct_balance_simul 업데이트 오류: {e_dly}")

except Exception as e:
    print(f"[SIMUL] 추적준비 등록 오류\n{traceback.format_exc()}")

finally:
    conn.close()
