from flask import Flask, request, jsonify, send_from_directory
import psycopg2 as db
from datetime import datetime, timedelta
import os
import io
import subprocess
import requests
import json
import re
import threading as _threading
import pandas as pd
import urllib3
from concurrent.futures import ThreadPoolExecutor
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = Flask(__name__)

CONN_STRING  = "dbname='fund_risk_mng' host='100.123.201.50' port='5432' user='postgres' password='asdf1234'"
_SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
_EXPORT_DIR  = os.path.join(_SCRIPT_DIR, 'simul_exports')
SIMUL_TABLES = ['dly_trading_balance_simul', 'trading_trail_simul', 'dly_acct_balance_simul']

# ── KRX 종목 목록 (reservebot.py 동일 방식) ─────────────────────────
_krx_df      = None
_krx_df_lock = _threading.Lock()

def _load_krx() -> pd.DataFrame | None:
    """KRX에서 전체 종목 목록 로드 (최초 1회, 이후 캐시 반환)."""
    global _krx_df
    with _krx_df_lock:
        if _krx_df is not None:
            return _krx_df
    try:
        res = requests.get(
            'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download',
            timeout=15
        )
        res.encoding = 'EUC-KR'
        df = pd.read_html(res.text, header=0)[0][['회사명', '종목코드']]
        df = df.rename(columns={'회사명': 'company', '종목코드': 'code'})
        df['code'] = df['code'].apply(
            lambda c: str(c).strip().lstrip('A').zfill(6)[-6:]
        )
        df = df[df['code'].str.isdigit()].reset_index(drop=True)
        with _krx_df_lock:
            _krx_df = df
        return df
    except Exception as e:
        print(f"[KRX] 종목목록 로드 오류: {e}")
        return None


def _run_script(script_name: str, args: list, timeout: int = 120):
    env = os.environ.copy()
    env['PYTHONIOENCODING'] = 'utf-8'
    try:
        result = subprocess.run(
            ['python', os.path.join(_SCRIPT_DIR, script_name)] + args,
            capture_output=True, timeout=timeout,
            cwd=_SCRIPT_DIR, env=env
        )
        return (result.stdout.decode('utf-8', errors='replace'),
                result.stderr.decode('utf-8', errors='replace'),
                result.returncode)
    except subprocess.TimeoutExpired:
        return '', f'실행 시간 초과 ({timeout}초)', -1


def get_conn():
    return db.connect(CONN_STRING)


@app.route('/')
def index():
    return send_from_directory(os.path.join(app.root_path, 'templates'), 'simul.html')


@app.route('/api/stock-name')
def stock_name():
    """종목코드로 종목명 조회 (KRX DataFrame)."""
    code = request.args.get('code', '').strip().zfill(6)
    if not code or len(code) != 6:
        return jsonify({'name': ''})
    df = _load_krx()
    if df is None:
        return jsonify({'name': '', 'error': 'KRX 종목목록 로드 실패'})
    rows = df[df['code'] == code]
    return jsonify({'name': rows.iloc[0]['company'].strip() if not rows.empty else ''})


@app.route('/api/stock-code')
def stock_code():
    """종목명으로 종목코드 조회 (KRX DataFrame, 정확일치 → 앞부분 일치)."""
    name = request.args.get('name', '').strip()
    if not name:
        return jsonify({'code': ''})
    df = _load_krx()
    if df is None:
        return jsonify({'code': '', 'error': 'KRX 종목목록 로드 실패'})
    rows = df[df['company'] == name]
    if rows.empty:
        rows = df[df['company'].str.startswith(name, na=False)]
    return jsonify({'code': rows.iloc[0]['code'].strip() if not rows.empty else ''})


def calc_target_price(buy_price: int) -> int:
    """매수가 대비 5% 이상의 주문 가능한 최소 가격 (호가 단위 올림)."""
    raw = int(buy_price * 1.05)
    if raw < 1000:        tick = 1
    elif raw < 5000:      tick = 5
    elif raw < 10000:     tick = 10
    elif raw < 50000:     tick = 50
    elif raw < 100000:    tick = 100
    elif raw < 500000:    tick = 500
    else:                 tick = 1000
    return ((raw + tick - 1) // tick) * tick


@app.route('/api/preview', methods=['POST'])
def preview():
    """매수금액 기준 계산 미리보기 (reservebot.py menuNum=71, 매수금액 선택 로직)"""
    data = request.json or {}
    try:
        code       = data['code'].strip().zfill(6)
        name       = (data.get('name') or '').strip() or code
        buy_price  = int(data['buy_price'])
        loss_price = int(data['loss_price'])
        buy_amount = int(data['buy_amount'])
        buy_date   = str(data['buy_date']).replace('-', '')  # YYYYMMDD

        if buy_price <= 0 or loss_price <= 0 or buy_amount <= 0:
            return jsonify({'error': '매수가, 이탈가, 매수금액은 0보다 커야 합니다.'}), 400
        if buy_price <= loss_price:
            return jsonify({'error': f'매수가({buy_price:,})가 이탈가({loss_price:,}) 이하입니다.'}), 400

        loss_rate     = round((100 - (loss_price / buy_price) * 100) * -1, 2)
        amt_buy_qty   = int(round(buy_amount / buy_price))
        amt_buy_amt   = buy_price * amt_buy_qty
        amt_item_loss = (buy_price - loss_price) * amt_buy_qty
        target_price  = calc_target_price(buy_price)
        target_rate   = round((target_price / buy_price - 1) * 100, 2)

        return jsonify({
            'code':         code,
            'name':         name,
            'buy_price':    buy_price,
            'loss_price':   loss_price,
            'loss_rate':    loss_rate,
            'buy_date':     buy_date,
            'amt_buy_qty':  amt_buy_qty,
            'amt_buy_amt':  amt_buy_amt,
            'amt_item_loss': amt_item_loss,
            'target_price': target_price,
            'target_rate':  target_rate,
        })
    except KeyError as e:
        return jsonify({'error': f'필수 입력값 누락: {e}'}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 400

def get_previous_business_day(day):
    conn = get_conn()
    cur100 = conn.cursor()
    cur100.execute("select prev_business_day_char(%s)", (day,))
    result_one00 = cur100.fetchall()
    cur100.close()

    return result_one00[0][0]

@app.route('/api/save', methods=['POST'])
def save():
    """매수금액 기준 계산 결과를 trading_trail_simul 에 저장 (trade_tp='M')"""
    data = request.json or {}
    try:
        code       = data['code'].strip().zfill(6)
        name       = (data.get('name') or '').strip() or code
        buy_price  = int(data['buy_price'])
        loss_price = int(data['loss_price'])
        buy_amount = int(data['buy_amount'])
        buy_date   = str(data['buy_date']).replace('-', '')

        amt_buy_qty   = int(round(buy_amount / buy_price))
        amt_buy_amt   = buy_price * amt_buy_qty
        amt_item_loss = (buy_price - loss_price) * amt_buy_qty
        target_price  = calc_target_price(buy_price)

        # ── 시장비율·현금 초과 검증 ───────────────────────────────────
        conn_chk = get_conn()
        cur_chk  = conn_chk.cursor()

        # 매수일 이전 가장 최근 잔고 (정확 날짜 대신 최근 레코드)
        cur_chk.execute("""
            SELECT prvs_excc_amt, tot_evlu_amt, market_ratio, dt
            FROM dly_acct_balance_simul
            WHERE acct = '74346047' AND dt < %s
            ORDER BY dt DESC LIMIT 1
        """, (buy_date,))
        dly_row = cur_chk.fetchone()

        # 매수일 신규 등록 매수 총금액 (이번 매수 제외, 전일 이월 레코드 제외)
        # trail_dtm = '090000' 은 kis_trading_set_simul.py 이월 레코드 → db_prvs 에 이미 반영됨
        cur_chk.execute("""
            SELECT COALESCE(SUM(basic_price * basic_qty), 0)
            FROM trading_trail_simul
            WHERE acct_no = 'SIMUL' AND trail_day = %s
              AND trail_tp IN ('1','2','3','L','P')
              AND trail_dtm != '090000'
        """, (buy_date,))
        tot_buy_row       = cur_chk.fetchone()
        total_buy_on_date = int(tot_buy_row[0]) if tot_buy_row else 0

        cur_chk.close()
        conn_chk.close()

        if dly_row:
            db_prvs  = int(dly_row[0] or 0)
            db_tot   = int(dly_row[1] or 0)
            db_mr    = float(dly_row[2] or 0)
            ref_date = dly_row[3]

            # 매수일 기존 매수 반영 후 잔여 예수금
            avail_prvs = db_prvs - total_buy_on_date

            # 예수금 잔액 초과 체크
            if amt_buy_amt > avail_prvs:
                return jsonify({
                    'error': (
                        f"매수금액({amt_buy_amt:,}원)이 잔여 예수금({avail_prvs:,}원)을 초과합니다.\n"
                        f"기준잔고: {db_prvs:,}원 | 매수일 기존매수: {total_buy_on_date:,}원 | "
                        f"잔여예수금: {avail_prvs:,}원 (기준일: {ref_date})"
                    )
                }), 400

            # 시장비율 초과 체크 (매수일 전체 매수 포함)
            if db_tot > 0 and db_mr > 0:
                new_cash         = avail_prvs - amt_buy_amt
                new_invest_ratio = 100 - (new_cash / db_tot * 100)
                if new_invest_ratio > db_mr:
                    max_buy    = max(0, avail_prvs - int(db_tot * (100 - db_mr) / 100))
                    excess_buy = amt_buy_amt - max_buy
                    return jsonify({
                        'error': (
                            f"시장비율({db_mr:.0f}%) 초과 — 매수 후 현재비율 {new_invest_ratio:.1f}%\n"
                            f"기존매수 포함 총매수: {total_buy_on_date + amt_buy_amt:,}원 | "
                            f"초과금액: {excess_buy:,}원 | 허용 추가매수: {max_buy:,}원 (기준일: {ref_date})"
                        )
                    }), 400

        now       = datetime.now()
        buy_time  = str(data.get('buy_time') or '').strip()
        trail_dtm = buy_time if (len(buy_time) == 6 and buy_time.isdigit()) else now.strftime('%H%M%S')

        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            WITH ins AS (
                INSERT INTO trading_trail_simul (
                    acct_no, code, name, trail_day, trail_dtm, trail_tp,
                    basic_price, basic_qty, basic_amt,
                    stop_price, target_price, proc_min,
                    trade_tp, exit_price, loss_amt, crt_dt, mod_dt
                )
                SELECT %s,%s,%s,%s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s,%s,%s
                ON CONFLICT (acct_no, code, trail_day, trail_dtm, trail_tp) DO NOTHING
                RETURNING 1 AS flag
            )
            SELECT flag FROM ins;
        """, (
            'SIMUL', code, name, buy_date, trail_dtm, '1',
            buy_price, amt_buy_qty, amt_buy_amt,
            loss_price, target_price, trail_dtm,
            'M', loss_price, amt_item_loss, now, now,
        ))
        inserted = cur.fetchone() is not None
        conn.commit()
        cur.close()
        conn.close()

        return jsonify({'success': True, 'inserted': inserted,
                        'message': '저장 완료' if inserted else '이미 존재하는 데이터입니다.'})
    except KeyError as e:
        return jsonify({'error': f'필수 입력값 누락: {e}'}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/sell-preview', methods=['POST'])
def sell_preview():
    """매도 미리보기: 보유 포지션 조회 후 매도 수량·금액·수익률 계산."""
    data = request.json or {}
    try:
        code       = data['code'].strip().zfill(6)
        sell_date  = str(data['sell_date']).replace('-', '')
        sell_price = int(data['sell_price'])
        sell_ratio = float(data.get('sell_ratio', 100))

        if sell_price <= 0:
            return jsonify({'error': '매도가는 0보다 커야 합니다.'}), 400
        if not (1 <= sell_ratio <= 100):
            return jsonify({'error': '매도비율은 1~100% 사이여야 합니다.'}), 400

        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT name, basic_price, basic_qty, trail_dtm, trail_tp
            FROM trading_trail_simul
            WHERE acct_no = 'SIMUL' AND trail_day = %s AND code = %s
              AND trail_tp IN ('1','2','3','L','P')
            ORDER BY trail_dtm DESC LIMIT 1
        """, (sell_date, code))
        row = cur.fetchone()
        cur.close()
        conn.close()

        if not row:
            return jsonify({'error': f"{sell_date} 기준 {code} 활성 포지션 없음"}), 404

        name_db, basic_price, basic_qty, trail_dtm, trail_tp = row
        basic_price   = int(basic_price)
        basic_qty     = int(basic_qty)
        trail_qty     = max(1, round(basic_qty * sell_ratio / 100))
        trail_amt     = sell_price * trail_qty
        trail_rate    = round((sell_price - basic_price) / basic_price * 100, 2)
        remaining_qty = basic_qty - trail_qty
        new_trail_tp  = '4' if remaining_qty <= 0 else '3'

        return jsonify({
            'code':          code,
            'name':          name_db or code,
            'sell_date':     sell_date,
            'trail_dtm':     trail_dtm,
            'trail_tp_cur':  trail_tp,
            'basic_price':   basic_price,
            'basic_qty':     basic_qty,
            'sell_price':    sell_price,
            'sell_ratio':    sell_ratio,
            'trail_qty':     trail_qty,
            'trail_amt':     trail_amt,
            'trail_rate':    trail_rate,
            'remaining_qty': remaining_qty,
            'new_trail_tp':  new_trail_tp,
        })
    except KeyError as e:
        return jsonify({'error': f'필수 입력값 누락: {e}'}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/sell', methods=['POST'])
def sell():
    """매도 처리: trading_trail_simul 매도 업데이트 (_do_simul_sell_update 동일 로직)."""
    data = request.json or {}
    try:
        code          = data['code'].strip().zfill(6)
        sell_date     = str(data['sell_date']).replace('-', '')
        sell_price    = int(data['sell_price'])
        trail_qty     = int(data['trail_qty'])
        trail_amt     = int(data['trail_amt'])
        trail_rate    = float(data['trail_rate'])
        remaining_qty = int(data['remaining_qty'])
        new_trail_tp  = str(data['new_trail_tp'])
        trail_dtm     = str(data['trail_dtm'])
        sell_ratio    = float(data.get('sell_ratio', 100))

        now       = datetime.now()
        sell_time = str(data.get('sell_time') or '').strip()
        proc_min  = sell_time if (len(sell_time) == 6 and sell_time.isdigit()) else now.strftime('%H%M%S')

        conn = get_conn()
        cur  = conn.cursor()

        cur.execute("""
            SELECT basic_price FROM trading_trail_simul
            WHERE acct_no = 'SIMUL' AND trail_day = %s AND code = %s AND trail_dtm = %s
              AND trail_tp IN ('1','2','3','L','P')
        """, (sell_date, code, trail_dtm))
        bp_row      = cur.fetchone()
        basic_price = int(bp_row[0]) if bp_row else 0
        new_basic_amt = basic_price * remaining_qty

        cur.execute("""
            UPDATE trading_trail_simul SET
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
            WHERE acct_no = 'SIMUL' AND code = %s
              AND trail_day = %s AND trail_dtm = %s
              AND trail_tp IN ('1','2','3','L','P')
        """, (
            sell_price, trail_qty, trail_amt, trail_rate,
            int(sell_ratio), new_trail_tp, proc_min,
            remaining_qty, new_basic_amt, '매도처리', now,
            code, sell_date, trail_dtm,
        ))
        updated = cur.rowcount > 0
        conn.commit()
        cur.close()
        conn.close()

        if not updated:
            return jsonify({'error': '업데이트된 레코드 없음 (이미 처리됐거나 조건 불일치)'}), 404

        return jsonify({
            'success': True,
            'message': (
                f"매도 처리 완료: {code} {sell_price:,}원 × {trail_qty:,}주"
                f" 수익률:{trail_rate:+.2f}% (trail_tp={new_trail_tp})"
            )
        })
    except KeyError as e:
        return jsonify({'error': f'필수 입력값 누락: {e}'}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/list')
def list_records():
    """저장된 시뮬레이션 내역 최근 100건 조회"""
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT acct_no, code, name, trail_day, trail_dtm, trail_tp,
                   basic_price, basic_qty, basic_amt,
                   stop_price, target_price, trade_tp, loss_amt, crt_dt, mod_dt,
                   exit_price, proc_min,
                   trail_plan, trail_price, trail_qty, trail_amt, trail_rate, trade_result
            FROM trading_trail_simul
            ORDER BY crt_dt DESC
            LIMIT 100
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify([{
            'acct_no':      r[0],
            'code':         r[1],
            'name':         r[2],
            'trail_day':    r[3],
            'trail_dtm':    r[4],
            'trail_tp':     r[5],
            'basic_price':  r[6],
            'basic_qty':    r[7],
            'basic_amt':    r[8],
            'stop_price':   r[9],
            'target_price': r[10],
            'trade_tp':     r[11],
            'loss_amt':     r[12],
            'crt_dt':       r[13].strftime('%Y-%m-%d %H:%M:%S') if r[13] else '',
            'mod_dt':       r[14].strftime('%Y-%m-%d %H:%M:%S') if r[14] else '',
            'exit_price':   r[15],
            'proc_min':     r[16],
            'trail_plan':   r[17],
            'trail_price':  r[18],
            'trail_qty':    r[19],
            'trail_amt':    r[20],
            'trail_rate':   r[21],
            'trade_result': r[22],
        } for r in rows])
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/delete/<trail_day>/<code>/<trail_tp>', methods=['DELETE'])
def delete_record(trail_day, code, trail_tp):
    """시뮬레이션 레코드 삭제"""
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(
            "DELETE FROM trading_trail_simul WHERE acct_no='SIMUL' AND trail_day=%s AND code=%s AND trail_tp=%s",
            (trail_day, code, trail_tp)
        )
        deleted = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'success': True, 'deleted': deleted})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/run-set', methods=['POST'])
def run_set():
    """추적등록: kis_trading_set_simul.py 를 지정 날짜로 실행."""
    data     = request.json or {}
    sim_date = str(data.get('sim_date', '')).replace('-', '')
    if len(sim_date) != 8 or not sim_date.isdigit():
        return jsonify({'error': '수행일자 형식 오류 (YYYYMMDD)'}), 400
    try:
        stdout, stderr, rc = _run_script('kis_trading_set_simul.py', [sim_date], timeout=120)
        return jsonify({'success': rc == 0, 'output': stdout, 'error_output': stderr, 'returncode': rc})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/run-trail', methods=['POST'])
def run_trail():
    """추적실행: kis_trading_trail_vol_state_simul.py 를 지정 날짜로 실행."""
    data     = request.json or {}
    sim_date = str(data.get('sim_date', '')).replace('-', '')
    if len(sim_date) != 8 or not sim_date.isdigit():
        return jsonify({'error': '수행일자 형식 오류 (YYYYMMDD)'}), 400
    try:
        stdout, stderr, rc = _run_script('kis_trading_trail_vol_state_simul.py', [sim_date], timeout=600)
        return jsonify({'success': rc == 0, 'output': stdout, 'error_output': stderr, 'returncode': rc})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/update-trail', methods=['POST'])
def update_trail():
    """매매추적 변경: trading_trail_simul 레코드를 지정 필드만 업데이트."""
    data = request.json or {}

    change_date = str(data.get('change_date', '')).replace('-', '')
    if len(change_date) != 8 or not change_date.isdigit():
        return jsonify({'error': '변경일자 형식 오류 (YYYYMMDD)'}), 400

    def _int(key):
        v = data.get(key)
        return int(v) if v not in (None, '', 0) else None

    code         = data.get('code', '').strip().zfill(6) if data.get('code', '').strip() else None
    proc_min     = str(data.get('proc_min', '')).strip() or None
    stop_price   = _int('stop_price')
    target_price = _int('target_price')
    exit_price   = _int('exit_price')
    basic_price  = _int('basic_price')
    basic_qty    = _int('basic_qty')
    trail_tp     = str(data.get('trail_tp', '')).strip() or None

    sets, params = [], []

    if proc_min:                 sets.append("proc_min     = %s"); params.append(proc_min)
    if stop_price   is not None: sets.append("stop_price   = %s"); params.append(stop_price)
    if target_price is not None: sets.append("target_price = %s"); params.append(target_price)
    if exit_price   is not None: sets.append("exit_price   = %s"); params.append(exit_price)
    if basic_price  is not None: sets.append("basic_price  = %s"); params.append(basic_price)
    if basic_qty    is not None: sets.append("basic_qty    = %s"); params.append(basic_qty)
    if trail_tp:                 sets.append("trail_tp     = %s"); params.append(trail_tp)

    if basic_price is not None and basic_qty is not None:
        sets.append("basic_amt = %s"); params.append(basic_price * basic_qty)
    if basic_price is not None and exit_price is not None and basic_qty is not None:
        sets.append("loss_amt  = %s"); params.append((basic_price - exit_price) * basic_qty)

    if not sets:
        return jsonify({'error': '변경할 값이 없습니다.'}), 400

    sets.append("mod_dt = %s"); params.append(datetime.now())

    where = "acct_no = 'SIMUL' AND trail_day = %s"
    params.append(change_date)
    if code:
        where += " AND code = %s"; params.append(code)
    else:
        return jsonify({'success': False, 'updated': 0,
                            'message': f'종목코드 없음'})    

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(f"UPDATE trading_trail_simul SET {', '.join(sets)} WHERE {where}", params)
        updated = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        if updated == 0:
            return jsonify({'success': False, 'updated': 0,
                            'message': f'변경 대상 없음'})
        return jsonify({'success': True, 'updated': updated,
                        'message': f'변경 완료'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

KIS_BASE_URL = "https://openapi.koreainvestment.com:9443"


def _get_api_account():
    """phills2 계좌 인증 정보 조회 (필요 시 토큰 갱신)."""
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("""
        SELECT acct_no, access_token, app_key, app_secret, token_publ_date
        FROM "stockAccount_stock_account" WHERE nick_name = 'phills2'
    """)
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row:
        return None
    acct_no, access_token, app_key, app_secret, token_publ_date = row
    today_str = datetime.now().strftime('%Y%m%d')
    token_day = (token_publ_date or '')[:8]
    needs_refresh = (
        token_day != today_str or
        (datetime.now() - datetime.strptime(token_publ_date, '%Y%m%d%H%M%S')).days >= 1
    )
    if needs_refresh:
        try:
            res = requests.post(
                f"{KIS_BASE_URL}/oauth2/tokenP",
                headers={"content-type": "application/json"},
                data=json.dumps({"grant_type": "client_credentials",
                                 "appkey": app_key, "appsecret": app_secret}),
                verify=False, timeout=10
            )
            d = res.json()
            if "access_token" in d:
                access_token    = d["access_token"]
                token_publ_date = datetime.now().strftime('%Y%m%d%H%M%S')
                conn2 = get_conn()
                cur2  = conn2.cursor()
                cur2.execute("""
                    UPDATE "stockAccount_stock_account"
                    SET access_token=%s, token_publ_date=%s, last_chg_date=%s
                    WHERE acct_no=%s
                """, (access_token, token_publ_date, datetime.now(), acct_no))
                conn2.commit()
                cur2.close()
                conn2.close()
        except Exception:
            pass
    return {'app_key': app_key, 'app_secret': app_secret, 'access_token': access_token}


def _kis_headers(ac, tr_id):
    return {
        "Content-Type": "application/json",
        "authorization": f"Bearer {ac['access_token']}",
        "appkey":    ac['app_key'],
        "appsecret": ac['app_secret'],
        "tr_id":     tr_id,
        "custtype":  "P",
    }


@app.route('/api/dashboard')
def dashboard():
    """대시보드: dly_acct_balance_simul(자산) + dly_trading_balance_simul(종목별) 조회."""
    sim_date = request.args.get('date', '').replace('-', '')
    if not sim_date or len(sim_date) != 8 or not sim_date.isdigit():
        return jsonify({'error': '날짜 형식 오류'}), 400
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT dnca_tot_amt, prvs_excc_amt, pchs_amt,
                   user_evlu_amt, tot_evlu_amt, evlu_pfls_amt,
                   total_profit_loss_amt, asst_icdc_amt
            FROM dly_acct_balance_simul
            WHERE acct = '74346047' AND dt = %s
        """, (sim_date,))
        acct_row = cur.fetchone()
        cur.execute("""
            SELECT code, name, balance_price, balance_qty, balance_amt,
                   value_rate, value_amt, sell_qty
            FROM public.dly_trading_balance_simul
            WHERE acct_no = '74346047' AND balance_day = %s AND use_yn = 'Y'
            ORDER BY code
        """, (sim_date,))
        stock_rows = cur.fetchall()
        cur.close()
        conn.close()
        acct = None
        if acct_row:
            acct = {
                'dnca_tot_amt':          int(acct_row[0] or 0),
                'prvs_excc_amt':         int(acct_row[1] or 0),
                'pchs_amt':              int(acct_row[2] or 0),
                'user_evlu_amt':         int(acct_row[3] or 0),
                'tot_evlu_amt':          int(acct_row[4] or 0),
                'evlu_pfls_amt':         int(acct_row[5] or 0),
                'total_profit_loss_amt': int(acct_row[6] or 0),
                'asst_icdc_amt':         int(acct_row[7] or 0),
            }
        stocks = [{
            'code':          r[0],
            'name':          r[1],
            'balance_price': int(r[2] or 0),
            'balance_qty':   int(r[3] or 0),
            'balance_amt':   int(r[4] or 0),
            'value_rate':    float(r[5] or 0),
            'value_amt':     int(r[6] or 0),
            'sell_qty':      int(r[7] or 0),
        } for r in stock_rows]
        return jsonify({'acct': acct, 'stocks': stocks, 'date': sim_date})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/stock-info')
def stock_info():
    """KIS API로 종목 기본정보(시장구분·업종·시가총액·매수금액 제안) 조회."""
    code     = request.args.get('code', '').strip().zfill(6)
    buy_date = request.args.get('buy_date', '').strip().replace('-', '')  # YYYYMMDD
    if not code or not code.isdigit():
        return jsonify({'error': '유효한 종목코드가 필요합니다.'}), 400
    ac = _get_api_account()
    if not ac:
        return jsonify({'error': 'API 계좌 정보 없음'}), 500
    try:
        res = requests.get(
            f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price",
            headers=_kis_headers(ac, "FHKST01010100"),
            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code},
            verify=False, timeout=10
        )
        data = res.json()
        if data.get('rt_cd') != '0' or not data.get('output'):
            return jsonify({'error': f"조회 실패: {data.get('msg1', '')}"}), 404
        out      = data['output']
        market   = out.get('rprs_mrkt_kor_name', '')
        industry = out.get('bstp_kor_isnm', '')
        try:
            mktcap = int(str(out.get('hts_avls', '0')).replace(',', ''))
        except Exception:
            mktcap = 0

        if   mktcap >= 10000: size = '대형주'
        elif mktcap >= 3000:  size = '중형주'
        else:                 size = '소형주'

        if size == '대형주':
            amt_min, amt_max, amt_desc = 2000000, 10000000, '유동성 높음, 안정적'
        elif size == '중형주':
            amt_min, amt_max, amt_desc = 1000000, 5000000, '유동성 보통, 중간 변동성'
        else:
            amt_min, amt_max, amt_desc = 500000, 2000000, '변동성 높음, 소액 분산 권장'

        # 손절금액 제안: market_ratio 기준 선형보간 (5만~25만원)
        suggest_loss_amt          = 50_000
        suggest_loss_market_ratio = 0
        suggest_loss_base_dt      = ''
        try:
            conn_mr = get_conn()
            cur_mr  = conn_mr.cursor()

            if len(buy_date) == 8:
                # buy_date 영업일 기준 market_ratio 조회
                prev_biz = get_previous_business_day(buy_date)
                cur_mr.execute("""
                    SELECT market_ratio, %s AS dt FROM "stockFundMng_stock_fund_mng" WHERE acct_no = '74346047' 
                    UNION ALL
                    SELECT market_ratio, dt FROM dly_acct_balance WHERE acct = '74346047' AND dt = %s
                    ORDER BY dt LIMIT 1
                """, (datetime.now().strftime('%Y%m%d'), prev_biz,))
            else:
                # buy_date 미입력 시 최신 레코드
                cur_mr.execute("""
                    SELECT market_ratio, %s AS dt FROM "stockFundMng_stock_fund_mng" WHERE acct_no = '74346047' 
                """, (datetime.now().strftime('%Y%m%d'),))

            mr_row = cur_mr.fetchone()
            cur_mr.close()
            conn_mr.close()
            if mr_row and mr_row[0]:
                suggest_loss_market_ratio = float(mr_row[0])
                suggest_loss_base_dt      = mr_row[1]
                suggest_loss_amt = int(
                    max(50_000, min(250_000,
                        50_000 + (suggest_loss_market_ratio / 100) * 200_000
                    ))
                )
        except Exception:
            pass

        suggest_buy_amt = int(
            max(amt_min, min(amt_max,
                amt_min + (suggest_loss_market_ratio / 100) * (amt_max - amt_min)
            ))
        ) if suggest_loss_market_ratio > 0 else amt_min

        return jsonify({
            'code': code, 'market': market, 'size': size,
            'industry': industry, 'mktcap': mktcap,
            'mktcap_str': f"{mktcap:,}억원" if mktcap else '',
            'amt_min': amt_min, 'amt_max': amt_max, 'amt_desc': amt_desc,
            'suggest_buy_amt': suggest_buy_amt,
            'suggest_buy_amt_str': f"{suggest_buy_amt:,}원",
            'suggest_loss_amt': suggest_loss_amt,
            'suggest_loss_amt_str': f"{suggest_loss_amt:,}원",
            'suggest_loss_market_ratio': suggest_loss_market_ratio,
            'suggest_loss_base_dt': suggest_loss_base_dt,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/import-csv', methods=['POST'])
def api_import_csv():
    """CSV 파일을 업로드하여 해당 테이블 데이터 교체."""
    if 'file' not in request.files:
        return jsonify({'error': '파일이 없습니다.'}), 400
    f     = request.files['file']
    fname = f.filename or ''
    table = next((t for t in SIMUL_TABLES if fname.startswith(t)), None)
    if table is None:
        names = ' / '.join(SIMUL_TABLES)
        return jsonify({'error': f'파일명에서 테이블을 인식할 수 없습니다. ({names} 중 하나로 시작해야 합니다)'}), 400
    try:
        raw = f.read()
        if raw.startswith(b'\xef\xbb\xbf'):
            raw = raw[3:]
        buf = io.BytesIO(raw)
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(f"DELETE FROM {table}")
        deleted = cur.rowcount
        cur.copy_expert(f"COPY {table} FROM STDIN WITH CSV HEADER NULL ''", buf)
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'message': f'{table} 가져오기 완료 (기존 {deleted}건 삭제)', 'table': table})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


_SAVE_SQLS = {
    'dly_trading_balance_simul': """
        INSERT INTO dly_trading_balance_simul_save(
            save_dtm, acct_no, code, name, balance_day, balance_price, balance_qty,
            balance_amt, value_rate, value_amt, buy_qty, sell_qty, crt_dt, mod_dt, use_yn
        )
        SELECT TO_CHAR(NOW(),'YYYYMMDDHH24MISS'),
            acct_no, code, name, balance_day, balance_price, balance_qty,
            balance_amt, value_rate, value_amt, buy_qty, sell_qty, crt_dt, mod_dt, use_yn
        FROM dly_trading_balance_simul
    """,
    'trading_trail_simul': """
        INSERT INTO trading_trail_simul_save(
            save_dtm, acct_no, name, code, trail_day, trail_dtm, trail_tp,
            basic_price, basic_qty, basic_amt, volumn, stop_price, target_price,
            proc_min, trade_tp, exit_price, loss_amt, crt_dt, mod_dt,
            order_no, order_type, order_dt, order_tmd, order_price, order_amount,
            complete_qty, remain_qty, trail_plan, trail_price, trail_qty,
            trail_amt, trail_rate, trade_result, last_alert_keys
        )
        SELECT TO_CHAR(NOW(),'YYYYMMDDHH24MISS'),
            acct_no, name, code, trail_day, trail_dtm, trail_tp,
            basic_price, basic_qty, basic_amt, volumn, stop_price, target_price,
            proc_min, trade_tp, exit_price, loss_amt, crt_dt, mod_dt,
            order_no, order_type, order_dt, order_tmd, order_price, order_amount,
            complete_qty, remain_qty, trail_plan, trail_price, trail_qty,
            trail_amt, trail_rate, trade_result, last_alert_keys
        FROM trading_trail_simul
    """,
    'dly_acct_balance_simul': """
        INSERT INTO dly_acct_balance_simul_save(
            save_dtm, acct, dt, dnca_tot_amt, prvs_excc_amt, td_buy_amt, td_sell_amt,
            td_tex_amt, user_evlu_amt, tot_evlu_amt, nass_amt, pchs_amt, evlu_amt,
            evlu_pfls_amt, ytdt_tot_evlu_amt, asst_icdc_amt, last_chg_date,
            total_profit_loss_amt, buy_psbl_amt, cash_rate, market_ratio,
            kospi_short, kosdak_short, kospi_mid, kosdak_mid, kospi_long, kosdak_long
        )
        SELECT TO_CHAR(NOW(),'YYYYMMDDHH24MISS'),
            acct, dt, dnca_tot_amt, prvs_excc_amt, td_buy_amt, td_sell_amt,
            td_tex_amt, user_evlu_amt, tot_evlu_amt, nass_amt, pchs_amt, evlu_amt,
            evlu_pfls_amt, ytdt_tot_evlu_amt, asst_icdc_amt, last_chg_date,
            total_profit_loss_amt, buy_psbl_amt, cash_rate, market_ratio,
            kospi_short, kosdak_short, kospi_mid, kosdak_mid, kospi_long, kosdak_long
        FROM dly_acct_balance_simul
    """,
}


@app.route('/api/save-all', methods=['POST'])
def api_save_all():
    """3개 테이블 데이터를 _save 테이블 및 CSV 파일로 저장."""
    os.makedirs(_EXPORT_DIR, exist_ok=True)
    now_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    results = []
    conn = None
    try:
        conn = get_conn()
        cur  = conn.cursor()
        for table in SIMUL_TABLES:
            cur.execute(_SAVE_SQLS[table])
            inserted = cur.rowcount
            fname = f'{table}_{now_str}.csv'
            fpath = os.path.join(_EXPORT_DIR, fname)
            buf = io.BytesIO()
            cur.copy_expert(f"COPY {table} TO STDOUT WITH CSV HEADER", buf)
            with open(fpath, 'wb') as fout:
                fout.write(b'\xef\xbb\xbf')
                fout.write(buf.getvalue())
            results.append({'table': table, 'inserted': inserted, 'file': fname})
        conn.commit()
        cur.close()
        conn.close()
        for r in results:
            r['download_url'] = f"/api/download/{r['file']}"
        return jsonify({'message': '전체저장 완료', 'results': results})
    except Exception as e:
        if conn:
            try: conn.rollback()
            except: pass
        return jsonify({'error': str(e)}), 500


@app.route('/api/download/<path:filename>', methods=['GET'])
def api_download(filename):
    """simul_exports 디렉터리의 CSV 파일 다운로드."""
    return send_from_directory(_EXPORT_DIR, filename, as_attachment=True)


@app.route('/api/download-list', methods=['GET'])
def api_download_list():
    """저장된 CSV 파일 목록 반환."""
    if not os.path.isdir(_EXPORT_DIR):
        return jsonify([])
    files = sorted(
        [f for f in os.listdir(_EXPORT_DIR) if f.endswith('.csv')],
        reverse=True
    )
    return jsonify([{'file': f, 'download_url': f'/api/download/{f}'} for f in files])


@app.route('/api/delete-all', methods=['POST'])
def api_delete_all():
    """3개 테이블 전체 데이터 삭제."""
    try:
        conn = get_conn()
        cur  = conn.cursor()
        counts = {}
        for table in SIMUL_TABLES:
            cur.execute(f"DELETE FROM {table}")
            counts[table] = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        total = sum(counts.values())
        return jsonify({'message': f'전체삭제 완료 (총 {total}건)', 'counts': counts})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


def _fetch_daily_ohlcv(ac, code):
    """FHKST01010400: 일봉 OHLCV 최근 100거래일 (output 리스트, 최신→과거 내림차순).
    필드: stck_bsop_date, stck_clpr, stck_oprc, stck_hgpr, stck_lwpr, acml_vol"""
    try:
        r = requests.get(
            f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-price",
            headers=_kis_headers(ac, "FHKST01010400"),
            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code,
                    "FID_PERIOD_DIV_CODE": "D", "FID_ORG_ADJ_PRC": "1"},
            verify=False, timeout=10
        )
        d = r.json()
        rows = d.get('output') or []
        return rows if isinstance(rows, list) else []
    except Exception:
        return []

def _fetch_short_selling(ac, code):
    """FHPST04830000: 일별 공매도 추이 (output2, 최신→과거).
    필드: stck_bsop_date, ssts_vol_rlim(공매도비율%)"""
    from datetime import timedelta
    today  = datetime.now().strftime('%Y%m%d')
    d60ago = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')
    try:
        r = requests.get(
            f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-short-over",
            headers=_kis_headers(ac, "FHPST04830000"),
            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code,
                    "FID_INPUT_DATE_1": d60ago, "FID_INPUT_DATE_2": today},
            verify=False, timeout=10
        )
        d = r.json()
        if d.get('rt_cd') == '0':
            rows = d.get('output2') or []
            return rows if isinstance(rows, list) else []
        return []
    except Exception:
        return []

def _fetch_investor(ac, code):
    """FHKST01010900: 최근 30일 투자자별 거래 리스트 (최신→과거, frgn/orgn_ntby_tr_pbmn 포함)."""
    try:
        r = requests.get(
            f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-investor",
            headers=_kis_headers(ac, "FHKST01010900"),
            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code},
            verify=False, timeout=10
        )
        d = r.json()
        if d.get('rt_cd') == '0' and isinstance(d.get('output'), list):
            return d['output']
        return []
    except Exception:
        return []

def _fetch_cur_price_out(ac, code):
    try:
        r = requests.get(
            f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price",
            headers=_kis_headers(ac, "FHKST01010100"),
            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code},
            verify=False, timeout=10
        )
        d = r.json()
        return d['output'] if d.get('rt_cd') == '0' and d.get('output') else None
    except Exception:
        return None

def _adx(highs, lows, closes, period=14):
    """Wilder's ADX. 입력은 오름차순(과거→최신). (adx, +DI, -DI) 반환."""
    n = len(highs)
    if n < period * 2 + 1:
        return None, None, None
    trs, pDMs, mDMs = [], [], []
    for i in range(1, n):
        h, l, pc = highs[i], lows[i], closes[i-1]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
        up, dn = highs[i] - highs[i-1], lows[i-1] - lows[i]
        pDMs.append(up   if up > dn and up > 0   else 0.0)
        mDMs.append(dn   if dn > up and dn > 0   else 0.0)
    def ws(arr, p):
        s = sum(arr[:p]); res = [s]
        for x in arr[p:]:
            s = s - s / p + x; res.append(s)
        return res
    def ws_avg(arr, p):
        s = sum(arr[:p]) / p; res = [s]
        for x in arr[p:]:
            s = s + (x - s) / p; res.append(s)
        return res
    atr_s = ws(trs, period); pdi_s = ws(pDMs, period); mdi_s = ws(mDMs, period)
    dxs = []
    for a, p, m in zip(atr_s, pdi_s, mdi_s):
        pd_ = p / a * 100 if a else 0; md_ = m / a * 100 if a else 0
        dxs.append(abs(pd_ - md_) / (pd_ + md_) * 100 if (pd_ + md_) else 0)
    adx_s    = ws_avg(dxs, period)
    a_last   = atr_s[-1]
    plus_di  = pdi_s[-1] / a_last * 100 if a_last else 0
    minus_di = mdi_s[-1] / a_last * 100 if a_last else 0
    return adx_s[-1], plus_di, minus_di

def _obv_trend(closes, volumes, n=5):
    """최근 n일 OBV 변화율(%). closes/volumes는 최신→과거(내림차순)."""
    cls  = list(reversed(closes))
    vols = list(reversed(volumes))
    obv  = 0; obs = [0]
    for i in range(1, len(cls)):
        if   cls[i] > cls[i-1]: obv += vols[i]
        elif cls[i] < cls[i-1]: obv -= vols[i]
        obs.append(obv)
    if len(obs) < n + 1: return 0.0
    prev = obs[-(n+1)]
    return (obs[-1] - prev) / abs(prev) * 100 if abs(prev) > 1 else 0.0

def _calc_chart_score(rows):
    """rows: FHKST01010400 output (최신→과거 내림차순)."""
    if not rows or len(rows) < 25:
        return {'score': None, 'detail': {}}
    def _f(v):
        try: return float(v)
        except: return 0.0
    closes  = [_f(r.get('stck_clpr', 0)) for r in rows]
    highs   = [_f(r.get('stck_hgpr', 0)) for r in rows]
    lows    = [_f(r.get('stck_lwpr', 0)) for r in rows]
    volumes = [_f(r.get('acml_vol',  0)) for r in rows]
    cur = closes[0]
    ma5  = sum(closes[:5])  / 5
    ma20 = sum(closes[:20]) / 20
    ma60 = sum(closes[:60]) / 60 if len(closes) >= 60 else None

    # ── 추세강도 MA5/20/60 (30점) ──────────────────────────────
    if ma60:
        if   ma5 > ma20 > ma60:                          trend_sc = 30
        elif ma5 > ma20 and ma20 < ma60:                 trend_sc = 22
        elif ma5 > ma60 and ma5 <= ma20:                 trend_sc = 16
        elif abs(ma5 - ma20) / ma20 < 0.01:             trend_sc = 10
        elif ma5 < ma20 and ma20 > ma60:                 trend_sc =  5
        else:                                            trend_sc =  0
    else:
        if   ma5 > ma20 * 1.02:  trend_sc = 22
        elif ma5 > ma20:         trend_sc = 16
        elif ma5 > ma20 * 0.99:  trend_sc = 10
        else:                    trend_sc =  0

    # ── ADX (25점) ──────────────────────────────────────────────
    asc_h = list(reversed(highs)); asc_l = list(reversed(lows)); asc_c = list(reversed(closes))
    adx, plus_di, minus_di = _adx(asc_h, asc_l, asc_c)
    if adx is None:
        adx_sc = 8
    elif adx >= 40 and plus_di > minus_di:   adx_sc = 25
    elif adx >= 25 and plus_di > minus_di:   adx_sc = 20
    elif adx >= 25 and plus_di <= minus_di:  adx_sc =  5
    elif adx >= 20:                          adx_sc = 12
    else:                                    adx_sc =  8

    # ── 이격도 MA20 (20점) ───────────────────────────────────────
    deviation = (cur - ma20) / ma20 * 100 if ma20 else 0.0
    d = deviation
    if   -3  <= d <=  5:   dev_sc = 20
    elif  5  <  d <= 10:   dev_sc = 15
    elif -8  <= d <  -3:   dev_sc = 15
    elif -15 <= d <  -8:   dev_sc = 12
    elif 10  <  d <= 15:   dev_sc = 10
    elif  d < -15:         dev_sc =  8
    else:                  dev_sc =  5   # d > 15

    # ── 거래량비율 5일/20일 (15점) ────────────────────────────────
    vol_avg5  = sum(volumes[:5])  / 5
    vol_avg20 = sum(volumes[:20]) / 20
    vol_ratio = vol_avg5 / vol_avg20 * 100 if vol_avg20 else 100
    v = vol_ratio
    if   v > 150:  vol_sc = 15
    elif v > 120:  vol_sc = 12
    elif v >  90:  vol_sc =  9
    elif v >  70:  vol_sc =  6
    else:          vol_sc =  3

    # ── 전일대비거래량 (10점) ─────────────────────────────────────
    vod = volumes[0] / volumes[1] * 100 if len(volumes) >= 2 and volumes[1] > 0 else 100
    if   vod > 150:  vod_sc = 10
    elif vod > 120:  vod_sc =  8
    elif vod >  80:  vod_sc =  6
    elif vod >  50:  vod_sc =  4
    else:            vod_sc =  2

    return {
        'score': trend_sc + adx_sc + dev_sc + vol_sc + vod_sc,
        'detail': {
            'ma5': round(ma5), 'ma20': round(ma20),
            'ma60': round(ma60) if ma60 else None,
            'trend_score': trend_sc,
            'adx': round(adx, 1) if adx else None,
            'plus_di': round(plus_di, 1) if plus_di else None,
            'minus_di': round(minus_di, 1) if minus_di else None,
            'adx_score': adx_sc,
            'deviation': round(deviation, 2), 'deviation_score': dev_sc,
            'vol_ratio_5_20': round(vol_ratio, 1), 'vol_score': vol_sc,
            'vod_ratio': round(vod, 1), 'vod_score': vod_sc,
        }
    }

def _calc_supply_score(ohlcv_rows, inv_rows, price_out, ssts_rows=None):
    """ohlcv_rows: FHKST01010400 output (OHLCV, OBV용)
       inv_rows:   inquire-investor output list (외국인/기관 거래대금)
       price_out:  inquire-price output (대차잔고비율)
       ssts_rows:  공매도 일별 추이 rows (ssts_vol_rlim 포함, 없으면 None)"""
    if not inv_rows:
        return {'score': None, 'detail': {}}

    def _si(v):
        try: return int(v)
        except: return 0

    def _sf(v):
        try: return float(v)
        except: return 0.0

    n5 = min(5, len(inv_rows))
    # 외국인/기관 5일 순매수 거래대금 (백만원 → 억원, 빈 문자열 안전 처리)
    frgn_5d = sum(_si(r.get('frgn_ntby_tr_pbmn', 0)) for r in inv_rows[:n5]) / 100
    orgn_5d = sum(_si(r.get('orgn_ntby_tr_pbmn', 0)) for r in inv_rows[:n5]) / 100

    # 공매도 5일 평균비율 (ssts_rows 없으면 0)
    if ssts_rows:
        nd5 = min(5, len(ssts_rows))
        ssts_avg = sum(_sf(r.get('ssts_vol_rlim', 0)) for r in ssts_rows[:nd5]) / nd5
    else:
        ssts_avg = 0.0

    # 대차잔고비율 (당일)
    loan_rate = _sf((price_out or {}).get('whol_loan_rmnd_rate', 0))

    # OBV 5일 변화율 (OHLCV 없으면 0)
    obv_chg = 0.0
    if ohlcv_rows and len(ohlcv_rows) >= 6:
        closes  = [_sf(r.get('stck_clpr', 0)) for r in ohlcv_rows]
        volumes = [_sf(r.get('acml_vol',  0)) for r in ohlcv_rows]
        obv_chg = _obv_trend(closes, volumes, n=5)

    # ── 외국인 5일 거래대금 (30점) ───────────────────────────────
    fr = frgn_5d
    if   fr >  200:  frgn_sc = 30
    elif fr >   50:  frgn_sc = 24
    elif fr >   10:  frgn_sc = 18
    elif fr >    0:  frgn_sc = 14
    elif fr >  -10:  frgn_sc =  8
    elif fr >  -50:  frgn_sc =  3
    else:            frgn_sc =  0

    # ── 기관 5일 거래대금 (25점) ─────────────────────────────────
    og = orgn_5d
    if   og >  200:  orgn_sc = 25
    elif og >   50:  orgn_sc = 20
    elif og >   10:  orgn_sc = 15
    elif og >    0:  orgn_sc = 11
    elif og >  -10:  orgn_sc =  6
    elif og >  -50:  orgn_sc =  2
    else:            orgn_sc =  0

    # ── 공매도 5일 평균비율 (20점, 역배점) ───────────────────────
    sv = ssts_avg
    if   sv <  1:  ssts_sc = 20
    elif sv <  2:  ssts_sc = 16
    elif sv <  3:  ssts_sc = 12
    elif sv <  5:  ssts_sc =  8
    elif sv < 10:  ssts_sc =  4
    else:          ssts_sc =  0

    # ── 대차잔고비율 (15점, 역배점) ──────────────────────────────
    lr = loan_rate
    if   lr <  0.5:  loan_sc = 15
    elif lr <  1.0:  loan_sc = 12
    elif lr <  2.0:  loan_sc =  9
    elif lr <  5.0:  loan_sc =  5
    elif lr < 10.0:  loan_sc =  2
    else:            loan_sc =  0

    # ── OBV 5일 추세 (10점) ─────────────────────────────────────
    if   obv_chg >  3:  obv_sc = 10
    elif obv_chg >  0:  obv_sc =  7
    elif obv_chg > -3:  obv_sc =  5
    else:               obv_sc =  2

    return {
        'score': frgn_sc + orgn_sc + ssts_sc + loan_sc + obv_sc,
        'detail': {
            'frgn_5d_eok':  round(frgn_5d, 1),  'frgn_score': frgn_sc,
            'orgn_5d_eok':  round(orgn_5d, 1),  'orgn_score': orgn_sc,
            'ssts_5d_avg':  round(ssts_avg, 2),  'ssts_score': ssts_sc,
            'loan_rate':    loan_rate,            'loan_score': loan_sc,
            'obv_chg_pct':  round(obv_chg, 2),   'obv_score':  obv_sc,
        }
    }


@app.route('/api/stock-scores')
def stock_scores():
    """종목 수급점수(외국인·기관·공매도·대차잔고·OBV) + 차트점수(추세·ADX·이격도·거래량·전일대비거래량) 계산."""
    code = request.args.get('code', '').strip().zfill(6)
    if not code or not code.isdigit():
        return jsonify({'error': '유효한 종목코드가 필요합니다.'}), 400
    ac = _get_api_account()
    if not ac:
        return jsonify({'error': 'API 계좌 정보 없음'}), 500
    try:
        with ThreadPoolExecutor(max_workers=4) as ex:
            f_ohlcv = ex.submit(_fetch_daily_ohlcv,    ac, code)
            f_ssts  = ex.submit(_fetch_short_selling,   ac, code)
            f_inv   = ex.submit(_fetch_investor,        ac, code)
            f_price = ex.submit(_fetch_cur_price_out,   ac, code)
        ohlcv = f_ohlcv.result()
        ssts  = f_ssts.result()
        inv   = f_inv.result()
        price = f_price.result()
        chart  = _calc_chart_score(ohlcv)
        supply = _calc_supply_score(ohlcv, inv, price, ssts_rows=ssts)

        def _pf(v):
            try: return float(v) if str(v).strip() not in ('', '0', '0.00') else None
            except: return None

        def _pf_pos(v):
            try:
                s = str(v).strip()
                return round(max(0.0, float(s)), 2) if s else None
            except: return None

        per_v = _pf_pos((price or {}).get('per'))
        pbr_v = _pf_pos((price or {}).get('pbr'))
        eps   = _pf((price or {}).get('eps'))
        bps   = _pf((price or {}).get('bps'))
        roe   = round(eps / bps * 100, 1) if eps and bps else None

        # 공매도(%): ssts_rows 최신일 값 우선, 없으면 price 당일 체결량/거래량 추정
        ssts_pct = None
        if ssts:
            ssts_pct = _pf(ssts[0].get('ssts_vol_rlim'))
        if ssts_pct is None and price:
            last_ssts = _pf(price.get('last_ssts_cntg_qty'))
            acml_vol  = _pf(price.get('acml_vol'))
            if last_ssts and acml_vol:
                ssts_pct = round(last_ssts / acml_vol * 100, 2)

        info = {
            'per':  per_v if per_v is not None else 0.0,
            'pbr':  pbr_v if pbr_v is not None else 0.0,
            'roe':  roe,
            'ssts': ssts_pct,
        }
        return jsonify({'chart': chart, 'supply': supply, 'info': info})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/active-stocks')
def api_active_stocks():
    """매도/변경 대상 활성 종목 조회 (trail_tp IN ('1','2','L'))."""
    date = request.args.get('date', '').replace('-', '')
    if not date or len(date) != 8 or not date.isdigit():
        return jsonify({'error': '날짜 형식 오류'}), 400
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT code, name, trail_tp, basic_price, basic_qty,
                   stop_price, target_price, exit_price
            FROM trading_trail_simul
            WHERE acct_no = 'SIMUL' AND trail_day = %s
              AND trail_tp IN ('1','2','L')
            ORDER BY code
        """, (date,))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify([
            {
                'code':         r[0],
                'name':         r[1],
                'trail_tp':     r[2],
                'basic_price':  int(r[3]) if r[3] else 0,
                'basic_qty':    int(r[4]) if r[4] else 0,
                'stop_price':   int(r[5]) if r[5] else None,
                'target_price': int(r[6]) if r[6] else None,
                'exit_price':   int(r[7]) if r[7] else None,
            }
            for r in rows
        ])
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── 네이버 검색 Open API (developers.naver.com 에서 발급) ─────────────────
NAVER_CLIENT_ID     = 'erADnKdeL7tt0dMK5K7N'   # Application Client ID
NAVER_CLIENT_SECRET = 'LvdYnYi6Yc'   # Application Client Secret

# ── DART Open API ──────────────────────────────────────────────────────────
DART_API_KEY  = 'a86677be2f044d30757379f277024be9b0989823'
DART_BASE     = 'https://opendart.fss.or.kr/api'
_dart_corp_map: dict | None = None
_dart_corp_name_map: dict | None = None   # corp_name → corp_code
_dart_corp_map_lock = _threading.Lock()
_dart_biz_cache: dict = {}   # corp_code → biz summary
_dart_exec_vote_cache: dict = {}  # corp_code → {name: voting_shares}

def _load_dart_corp_map() -> tuple[dict, dict]:
    """corpCode.xml 다운로드 → ({stock_code: corp_code}, {corp_name: corp_code}) 캐시 (최초 1회)."""
    global _dart_corp_map, _dart_corp_name_map
    with _dart_corp_map_lock:
        if _dart_corp_map is not None:
            return _dart_corp_map, _dart_corp_name_map
    import zipfile, io, xml.etree.ElementTree as ET
    try:
        r = requests.get(
            f"{DART_BASE}/corpCode.xml",
            params={'crtfc_key': DART_API_KEY},
            timeout=30
        )
        z    = zipfile.ZipFile(io.BytesIO(r.content))
        data = z.read(z.namelist()[0])
        root = ET.fromstring(data)
        m  = {}
        nm = {}
        for item in root.findall('.//list'):
            sc = (item.findtext('stock_code') or '').strip()
            cc = (item.findtext('corp_code')  or '').strip()
            cn = (item.findtext('corp_name')  or '').strip()
            if sc and cc:
                m[sc] = cc
            if cn and cc:
                nm[cn] = cc
        with _dart_corp_map_lock:
            _dart_corp_map      = m
            _dart_corp_name_map = nm
        return m, nm
    except Exception as e:
        print(f"[DART] corpCode 로드 오류: {e}")
        return {}, {}

def _dart_stock_to_corp(stock_code: str, stock_name: str = '') -> str | None:
    """종목코드(6자리) → DART corp_code(8자리) 변환.
    corpCode.xml에 없으면 stock_name으로 이름 기반 검색 (종목코드 불일치 대응)."""
    m, nm = _load_dart_corp_map()
    sc = stock_code.zfill(6)
    if sc in m:
        return m[sc]
    # corpCode.xml 종목코드 불일치 → 종목명으로 검색
    if stock_name and nm:
        if stock_name in nm:
            corp_code = nm[stock_name]
            m[sc] = corp_code  # 캐시에 추가
            print(f"[DART] 이름 매핑: {stock_code}({stock_name}) → {corp_code}")
            return corp_code
    return None

def _dart_req(endpoint, params, timeout=12):
    p = {**params, 'crtfc_key': DART_API_KEY}
    try:
        r = requests.get(f"{DART_BASE}/{endpoint}", params=p, timeout=timeout)
        d = r.json()
        return d if d.get('status') == '000' else None
    except Exception:
        return None

def _to_eok(v):
    if not v: return None
    try:
        return round(int(str(v).replace(',', '').replace(' ', '')) / 100_000_000, 1)
    except Exception:
        return None

def _growth(cur, prev):
    if cur is None or not prev: return None
    return round((cur - prev) / abs(prev) * 100, 1)

def _get_is_account(rows, *keywords, exact=None):
    """IS/CIS 구분의 행 중 keywords를 우선순위 순으로 탐색해 첫 매칭 행 반환.

    행 순서(ord)가 키워드 우선순위보다 먼저 적용되면 부속/파생 계정이
    본계정보다 먼저 매칭되는 문제가 있었음. 확인된 오매칭 사례:
      - '5. 기타의영업수익' → '영업수익' 키워드에 매칭 (카카오뱅크)
      - '기타영업수익' → '영업수익' 키워드에 매칭 (포스코퓨처엠)
      - '기본주당분기순이익' → '분기순이익' 키워드에 매칭, EPS값이 총액으로
        오인식되어 사실상 0에 가까운 값이 됨 (금양그린파워)
    키워드를 outer loop로 돌려 우선순위를 지키고, '기타'(부속손익) 또는
    '주당'(주당순이익=EPS) 이 포함된 계정은 1차 탐색에서 제외한다.

    exact: '매출'처럼 접미사 없는 단독 계정명(삼아알미늄 등 일부 종목의
    구버전 XBRL 레이블). 부분매칭으로 추가하면 '매출원가'/'매출총이익' 등에
    오매칭되므로 완전일치로만, 키워드 매칭이 모두 실패한 뒤 최후에 시도한다.
    """
    is_rows = [r for r in rows if r.get('sj_div') in ('IS', 'CIS')]
    def _is_sub_account(nm):
        return '기타' in nm or '주당' in nm
    for kw in keywords:
        for r in is_rows:
            nm = (r.get('account_nm') or '').replace(' ', '')
            if kw in nm and not _is_sub_account(nm):
                return r
    for kw in keywords:
        for r in is_rows:
            nm = (r.get('account_nm') or '').replace(' ', '')
            if kw in nm:
                return r
    for ex in (exact or []):
        for r in is_rows:
            nm = (r.get('account_nm') or '').replace(' ', '')
            if nm == ex:
                return r
    return None

def _dart_fin_one(corp_code, year, reprt_code):
    for fs in ['CFS', 'OFS']:
        d = _dart_req('fnlttSinglAcntAll.json', {
            'corp_code': corp_code, 'bsns_year': str(year),
            'reprt_code': reprt_code, 'fs_div': fs
        })
        if d and d.get('list'):
            return d['list']
    return []

_REPRT_TYPE_KR = {'11013': '분기', '11012': '반기', '11014': '분기', '11011': '연간'}


def _dart_fin_annual(corp_code: str) -> list:
    """사업보고서 3건 호출 → 최근 9개년 연간 실적 중 최근 8개년 반환 (억원)."""
    now = datetime.now()
    target_year  = now.year - 1 if now.month >= 4 else now.year - 2
    older_year   = target_year - 3   # 3년 전 → bfefrmtrm/frmtrm/thstrm = target-5, target-4, target-3
    oldest_year  = target_year - 6   # 6년 전 → target-8, target-7, target-6

    def _parse(rows, base_year):
        if not rows:
            return []
        rev = _get_is_account(rows, '매출액', '영업수익', exact=['매출'])
        op  = _get_is_account(rows, '영업이익', '영업손익')
        ni  = _get_is_account(rows, '당기순이익', '당기순손익', '분기순이익', '반기순이익')
        if not any([rev, op, ni]):
            return []
        out = []
        for yr_off, col in ((-2, 'bfefrmtrm_amount'), (-1, 'frmtrm_amount'), (0, 'thstrm_amount')):
            rev_e = _to_eok((rev or {}).get(col))
            op_e  = _to_eok((op  or {}).get(col))
            ni_e  = _to_eok((ni  or {}).get(col))
            if not any(x is not None for x in [rev_e, op_e, ni_e]):
                continue
            out.append({
                'period':      str(base_year + yr_off),
                'revenue':     rev_e,
                'op_profit':   op_e,
                'net_profit':  ni_e,
                'op_margin':   round(op_e / rev_e * 100, 1) if rev_e and op_e else None,
                'is_estimate': False,
            })
        return out

    with ThreadPoolExecutor(max_workers=3) as ex:
        f_latest  = ex.submit(_dart_fin_one, corp_code, target_year,  '11011')
        f_older   = ex.submit(_dart_fin_one, corp_code, older_year,   '11011')
        f_oldest  = ex.submit(_dart_fin_one, corp_code, oldest_year,  '11011')
    latest_data  = _parse(f_latest.result(),  target_year)
    older_data   = _parse(f_older.result(),   older_year)
    oldest_data  = _parse(f_oldest.result(),  oldest_year)

    seen, combined = set(), []
    for r in oldest_data + older_data + latest_data:
        if r['period'] not in seen:
            seen.add(r['period'])
            combined.append(r)
    return sorted(combined, key=lambda x: x['period'])[-8:]


def _dart_fin_quarters(corp_code: str) -> list:
    """DART 분기별 독립 실적 재구성 (억원).

    DART 보고서별 thstrm_amount / thstrm_add_amount 의미:
      11013: thstrm = Q1 단독 (YTD 동일)
      11012: 반기순이익 계정 → thstrm = Q2 단독, thstrm_add = H1 누적
             당기순이익 계정 → thstrm = H1 누적, thstrm_add = Q2 단독(있으면)
      11014: 분기순이익 계정 → thstrm = Q3 단독
             당기순이익 계정 → thstrm = Jan-Sep YTD
      11011: thstrm = 연간
    """
    now = datetime.now()
    cy, cm = now.year, now.month

    def _fetch(year, rc):
        if year == cy:
            if rc == '11011': return None, None, ''
            if rc == '11013' and cm < 5:  return None, None, ''
            if rc == '11012' and cm < 8:  return None, None, ''
            if rc == '11014' and cm < 11: return None, None, ''
        rows = _dart_fin_one(corp_code, year, rc)
        if not rows:
            return None, None, ''
        rev_row = _get_is_account(rows, '매출액', '영업수익', exact=['매출'])
        op_row  = _get_is_account(rows, '영업이익', '영업손익')
        ni_row  = _get_is_account(rows, '당기순이익', '당기순손익', '분기순이익', '반기순이익')
        ni_acct = (ni_row or {}).get('account_nm', '')
        def _v(row, field):
            return _to_eok((row or {}).get(field))
        data_a = (_v(rev_row, 'thstrm_amount'), _v(op_row, 'thstrm_amount'), _v(ni_row, 'thstrm_amount'))
        data_b = (_v(rev_row, 'thstrm_add_amount'), _v(op_row, 'thstrm_add_amount'), _v(ni_row, 'thstrm_add_amount'))
        va = any(x is not None for x in data_a)
        vb = any(x is not None for x in data_b)
        return (data_a if va else None), (data_b if vb else None), ni_acct

    def _sub(a, b):
        if a is None or b is None:
            return None
        t = tuple(
            round(av - bv, 1) if av is not None and bv is not None else None
            for av, bv in zip(a, b)
        )
        return t if any(x is not None for x in t) else None

    quarters = []
    for year in (cy - 2, cy - 1, cy):    # 3개년 → 최대 9분기, 뒤에서 8개 취함
        q1_sa,    _,      _        = _fetch(year, '11013')  # thstrm = Q1 단독
        h1_thstrm, h1_add, _       = _fetch(year, '11012')  # 크기 비교로 Q2단독/H1누적 판별
        q3_r,     _,      q3_acct  = _fetch(year, '11014')  # 계정명에 따라 단독/YTD
        ann_r,    _,      _        = _fetch(year, '11011')  # thstrm = 연간

        # 반기보고서(11012): 매출액 크기로 thstrm/thstrm_add 의미 판별
        # H1누적(6개월) > Q2단독(3개월) 이므로 작은 쪽이 Q2단독
        h1_rev_a = h1_thstrm[0] if h1_thstrm is not None else None
        h1_rev_b = h1_add[0]    if h1_add    is not None else None
        if h1_rev_a is not None and h1_rev_b is not None and h1_rev_a < h1_rev_b:
            # thstrm < thstrm_add → thstrm = Q2 단독, thstrm_add = H1 누적
            q2_r   = h1_thstrm
            h1_cum = h1_add
        else:
            # thstrm = H1 누적(기본), thstrm_add = Q2 단독(있으면)
            h1_cum = h1_thstrm
            vb = h1_add is not None and any(x is not None for x in h1_add)
            q2_r = h1_add if vb else _sub(h1_cum, q1_sa)

        # 11014 thstrm_amount가 Q3 단독(Jul-Sep)인지 YTD(Jan-Sep)인지 판별:
        #  1) NI 계정명이 '분기순이익'이면 단독 (분기보고서 전용 계정)
        #  2) Q3 매출액 < H1 매출액이면 단독 확정 (YTD는 H1보다 항상 크거나 같음)
        q3_is_standalone = '분기순이익' in q3_acct.replace(' ', '')
        if not q3_is_standalone and q3_r is not None and h1_cum is not None:
            if q3_r[0] is not None and h1_cum[0] is not None and q3_r[0] < h1_cum[0]:
                q3_is_standalone = True

        if q3_is_standalone:
            q3_alone = q3_r                               # Q3 단독 직접 사용
            q4_r     = _sub(_sub(ann_r, h1_cum), q3_r)   # Annual - H1누적 - Q3단독 = Q4
        else:
            q3_alone = _sub(q3_r,  h1_cum)  # YTD(Jan-Sep) - H1누적 = Q3 단독
            q4_r     = _sub(ann_r, q3_r)    # Annual - YTD = Q4

        for label, data in (
            (f'{year}Q1', q1_sa),
            (f'{year}Q2', q2_r),
            (f'{year}Q3', q3_alone),
            (f'{year}Q4', q4_r),
        ):
            if data is None:
                continue
            rev, op, ni = data
            quarters.append({
                'period':      label,
                'revenue':     rev,
                'op_profit':   op,
                'net_profit':  ni,
                'op_margin':   round(op / rev * 100, 1) if rev and op else None,
                'is_estimate': False,
                'op_growth':   None,
            })

    for i in range(1, len(quarters)):
        quarters[i]['op_growth'] = _growth(
            quarters[i].get('op_profit'), quarters[i - 1].get('op_profit')
        )

    return quarters[-8:]   # 최근 8분기


def _dart_shareholders_latest(corp_code):
    """주주에 관한 사항 — 1분기→반기→3분기→사업보고서 순으로 최신 우선.
    데이터가 실제로 존재하는 보고서를 찾을 때까지 순서대로 시도."""
    now = datetime.now()
    _lbl = {'11011': '사업보고서', '11012': '반기보고서',
            '11013': '1분기보고서', '11014': '3분기보고서'}
    _order = [
        (now.year,     '11013'),
        (now.year,     '11012'),
        (now.year - 1, '11011'),
        (now.year - 1, '11014'),
        (now.year - 1, '11012'),
        (now.year - 1, '11013'),
        (now.year - 2, '11011'),
    ]

    for y, rc in _order:
        lbl = _lbl.get(rc, rc)
        d = _dart_req('hyslrSttus.json', {
            'corp_code': corp_code, 'bsns_year': str(y), 'reprt_code': rc
        })
        if not (d and d.get('list')):
            print(f"[DART] 주주현황 없음: {y}년 {lbl}, 다음 시도")
            continue

        major = [{
            'name':        r.get('nm', ''),
            'relate':      r.get('relate', ''),
            'stock_knd':   r.get('stock_knd', '보통주'),
            'start_qty':   r.get('bsis_posesn_stock_co', '-'),
            'start_ratio': r.get('bsis_posesn_stock_qota_rt', '-'),
            'end_qty':     r.get('trmend_posesn_stock_co', '-'),
            'end_ratio':   r.get('trmend_posesn_stock_qota_rt', '-'),
            'change':      r.get('change_on', ''),
        } for r in d['list'] if r.get('nm')]

        if not major:
            print(f"[DART] 주주현황 필터 후 빈 결과: {y}년 {lbl}, 다음 시도")
            continue

        print(f"[DART] 주주현황 조회 성공: {y}년 {lbl} ({len(major)}건)")

        minority = None
        e = _dart_req('elestock.json', {
            'corp_code': corp_code, 'bsns_year': str(y), 'reprt_code': rc
        })
        if e and e.get('list'):
            el = e['list'][0]
            minority = {
                'count':  el.get('shrholdr_co', ''),
                'shares': el.get('hold_stock_co', ''),
                'ratio':  el.get('hold_ratio', ''),
            }

        return {'period': f"{y}년 {lbl}", 'list': major, 'major': major, 'minority': minority}

    print(f"[DART] 주주현황: 조회 가능한 보고서 없음 ({corp_code})")
    return {'period': '', 'list': [], 'major': [], 'minority': None}

def _dart_executives_latest(corp_code):
    """임원 및 직원등의 현황 — 1분기→반기→3분기→사업보고서 순으로 최신 우선.
    데이터가 실제로 존재하는 보고서를 찾을 때까지 순서대로 시도."""
    now = datetime.now()
    _lbl = {'11011': '사업보고서', '11012': '반기보고서',
            '11013': '1분기보고서', '11014': '3분기보고서'}
    result = {'period': '', 'list': [], 'employees': None}

    _order = [
        (now.year,     '11013'),
        (now.year,     '11012'),
        (now.year - 1, '11011'),
        (now.year - 1, '11014'),
        (now.year - 1, '11012'),
        (now.year - 1, '11013'),
        (now.year - 2, '11011'),
    ]

    # ① 임원현황 (exctvSttus) — 유효한 데이터가 있는 첫 번째 보고서에서 중단
    for y, rc in _order:
        lbl = _lbl.get(rc, rc)
        d = _dart_req('exctvSttus.json', {
            'corp_code': corp_code, 'bsns_year': str(y), 'reprt_code': rc
        })
        if not (d and d.get('list')):
            print(f"[DART] 임원현황 없음: {y}년 {lbl}, 다음 시도")
            continue
        # ① XML 파싱으로 의결권있는 주식수 맵 (SH5_STK_VOTY)
        voting_map = _dart_exec_voting_map(corp_code)
        # ② XML 실패 시 hyslrSttus 보통주 교차조회로 fallback
        if not voting_map:
            shr = _dart_req('hyslrSttus.json', {
                'corp_code': corp_code, 'bsns_year': str(y), 'reprt_code': rc
            })
            if shr and shr.get('list'):
                for s in shr['list']:
                    if s.get('stock_knd', '') == '보통주' and s.get('nm'):
                        voting_map[s['nm'].strip()] = s.get('trmend_posesn_stock_co', '')
        execs = [{
            'name':     r.get('nm', ''),
            'position': r.get('ofcps', ''),
            'shares':   voting_map.get(r.get('nm', '').strip(), ''),
            'career':   r.get('main_career', ''),
        } for r in d['list'] if r.get('nm')]
        if not execs:
            print(f"[DART] 임원현황 필터 후 빈 결과: {y}년 {lbl}, 다음 시도")
            continue
        print(f"[DART] 임원현황 조회 성공: {y}년 {lbl} ({len(execs)}건)")
        result['period'] = f"{y}년 {lbl}"
        result['list']   = execs
        break

    # ② 직원현황 (empSttus) — 유효한 데이터가 있는 첫 번째 보고서에서 중단
    for y, rc in _order:
        lbl = _lbl.get(rc, rc)
        d = _dart_req('empSttus.json', {
            'corp_code': corp_code, 'bsns_year': str(y), 'reprt_code': rc
        })
        if not (d and d.get('list')):
            print(f"[DART] 직원현황 없음: {y}년 {lbl}, 다음 시도")
            continue

        # 부문·유형별 그룹핑
        groups: dict = {}
        male_total = female_total = 0
        avg_sal = ''
        for r in d['list']:
            dept     = (r.get('fo_bbm') or '전사').strip()
            emp_tp   = (r.get('emp_tp') or '').strip()
            gender   = r.get('sexdstn', '')
            try:
                cnt = int(str(r.get('jan_nd_cnt', '0')).replace(',', '') or '0')
            except Exception:
                cnt = 0
            key = f"{dept}||{emp_tp}"
            if key not in groups:
                groups[key] = {
                    'dept': dept, 'type': emp_tp,
                    'male': 0, 'female': 0,
                    'avg_salary': r.get('jan_salary_am', ''),
                }
            if gender == '남':
                groups[key]['male'] += cnt
                male_total += cnt
            elif gender == '여':
                groups[key]['female'] += cnt
                female_total += cnt
            if not avg_sal and r.get('jan_salary_am'):
                avg_sal = r.get('jan_salary_am', '')

        rows = [{'dept': v['dept'], 'type': v['type'],
                 'male': v['male'], 'female': v['female'],
                 'total': v['male'] + v['female'],
                 'avg_salary': v['avg_salary']}
                for v in groups.values()]
        if male_total + female_total == 0:
            print(f"[DART] 직원현황 합산 인원 0: {y}년 {lbl}, 다음 시도")
            continue

        print(f"[DART] 직원현황 조회 성공: {y}년 {lbl} (총 {male_total+female_total}명)")
        result['employees'] = {
            'period': f"{y}년 {lbl}",
            'total': male_total + female_total,
            'male': male_total, 'female': female_total,
            'avg_salary': avg_sal,
            'rows': rows,
        }
        break

    return result

def _extract_dart_biz_keywords(text: str) -> list:
    """사업의 개요 텍스트에서 주요 사업부문 및 매출 키워드 추출 (최대 4개)."""
    import re
    keywords = []
    seen = set()
    skip = {'당사', '자사', '동사', '회사', '본사', '연결', '별도', '주요', '기타',
            '국내', '해외', '글로벌', '사업', '영업', '기존', '신규', '전체', '해당',
            '각', '등의', '제품', '서비스', '부문', '사업부', '관련', '통해', '통한'}
    if not text:
        return keywords

    # 1. "XX부문", "XX 사업부문", "XX 사업부", "XX사업" 패턴 (공백 없이도 매칭)
    for m in re.finditer(
            r'([가-힣A-Za-z0-9]{2,12}(?:[\s·][가-힣A-Za-z]{1,8})?)\s*'
            r'(사업부문|사업부|부문|사업)', text):
        base = m.group(1).strip().rstrip('·')
        suffix = m.group(2)
        if base not in seen and base not in skip and 2 <= len(base) <= 14:
            seen.add(base)
            keywords.append(f"{base} {suffix}" if suffix not in base else base)

    # 2. 괄호 안 주력 제품/사업 설명 — "DS부문(반도체)", "IM부문(스마트폰)" 형태
    if len(keywords) < 4:
        for m in re.finditer(
                r'([가-힣A-Za-z0-9]{2,10}부문|[가-힣A-Za-z0-9]{2,10}사업부?)\s*'
                r'\(([가-힣A-Za-z0-9·,\s]{2,20})\)', text):
            inner = m.group(2).split(',')[0].strip()
            if inner not in seen and inner not in skip and 2 <= len(inner) <= 12:
                seen.add(inner)
                keywords.append(inner)

    # 3. "XX 제품/소재/솔루션/서비스" — 주력 품목
    if len(keywords) < 4:
        for m in re.finditer(
                r'([가-힣A-Za-z0-9·]{2,15})\s*'
                r'(?:반도체|디스플레이|배터리|화학|철강|소재|소자|모듈|부품|장비|시스템|솔루션|서비스|제품)'
                r'\s*(?:의|을|를|이|가|은|는|,|을\s|를\s|을\s통)', text):
            base = m.group(1).strip()
            if base not in seen and base not in skip and 2 <= len(base) <= 15:
                seen.add(base)
                keywords.append(base)

    # 4. "XX 매출" — 매출 구성 키워드
    if len(keywords) < 4:
        for m in re.finditer(
                r'([가-힣A-Za-z]{2,10})\s*(?:부문|사업)?\s*매출\s*(?:비중|비율|구성|은|이|의|액)', text):
            base = m.group(1).strip()
            if base not in seen and base not in skip and 2 <= len(base) <= 10:
                seen.add(base)
                keywords.append(f"{base} 매출")

    return keywords[:4]


def _dart_exec_voting_map(corp_code: str) -> dict:
    """사업보고서 XML ACODE=SH5_STK_VOTY 파싱 → {임원명: 의결권있는주식수(str)} 반환."""
    if corp_code in _dart_exec_vote_cache:
        return _dart_exec_vote_cache[corp_code]

    import zipfile, io as _io, re as _re2

    now = datetime.now()
    d = _dart_req('list.json', {
        'corp_code': corp_code,
        'bgn_de': f'{now.year - 2}0101',
        'pblntf_detail_ty': 'A001',
        'sort': 'date', 'sort_mth': 'desc',
        'page_count': '3',
    }, timeout=12)
    if not d or not d.get('list'):
        _dart_exec_vote_cache[corp_code] = {}
        return {}

    rcept_no = d['list'][0]['rcept_no']
    try:
        res = requests.get(f"{DART_BASE}/document.xml",
                           params={'crtfc_key': DART_API_KEY, 'rcept_no': rcept_no},
                           timeout=60)
        z = zipfile.ZipFile(_io.BytesIO(res.content))
        xml_files = sorted(
            [(n, z.getinfo(n).file_size) for n in z.namelist() if n.endswith('.xml')],
            key=lambda x: x[1], reverse=True
        )
        content = z.read(xml_files[0][0]).decode('utf-8', errors='replace')
    except Exception as e:
        print(f"[DART] exec voting XML error: {e}")
        _dart_exec_vote_cache[corp_code] = {}
        return {}

    # SH5_STK_VOTY(의결권있는 주식) 컬럼이 포함된 TBODY 추출
    voty_pos = content.find('SH5_STK_VOTY')
    if voty_pos < 0:
        _dart_exec_vote_cache[corp_code] = {}
        return {}

    tbody_start = content.rfind('<TBODY>', 0, voty_pos)
    tbody_end   = content.find('</TBODY>', voty_pos)
    if tbody_start < 0 or tbody_end < 0:
        _dart_exec_vote_cache[corp_code] = {}
        return {}

    section = content[tbody_start:tbody_end + 8]
    result = {}
    for tr in _re2.findall(r'<TR\b[^>]*ACOPY="Y"[^>]*>(.*?)</TR>', section, _re2.DOTALL):
        nm_m  = _re2.search(r'ACODE="SH5_NM_T"[^>]*>([^<]+)', tr)
        vt_m  = _re2.search(r'ACODE="SH5_STK_VOTY"[^>]*>(.*?)</TE>', tr, _re2.DOTALL)
        if not nm_m:
            continue
        name = nm_m.group(1).strip()
        if not name:
            continue
        shares = ''
        if vt_m:
            nums = _re2.findall(r'[0-9,]+', vt_m.group(1))
            shares = nums[0] if nums else ''
        result[name] = shares

    print(f"[DART] exec voting map: {len(result)}건 ({corp_code})")
    _dart_exec_vote_cache[corp_code] = result
    return result


_naver_reports_cache: dict = {}

def _naver_reports(code: str, max_reports: int = 10) -> dict:
    """네이버증권 모바일 API — 종목별 리서치 리포트 (최근 10건).
    반환: {reports, consensus, invest_points}"""
    import re
    from collections import Counter

    # 캐시 키에 현재 분기 포함 — 분기가 바뀌거나 서버 재시작 없이도 분기 필터 갱신
    _now_pre  = datetime.now()
    _q_pre    = (_now_pre.month - 1) // 3 + 1
    _cache_key = f"{code}:{str(_now_pre.year)[2:]}Q{_q_pre}"
    if _cache_key in _naver_reports_cache:
        return _naver_reports_cache[_cache_key]

    empty = {'reports': [], 'consensus': None, 'invest_points': []}
    try:
        r = requests.get(
            f'https://m.stock.naver.com/api/research/stock/{code}',
            params={'pageSize': max_reports, 'page': 1},
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
                     'Referer': 'https://m.stock.naver.com/'},
            timeout=8
        )
        if r.status_code != 200:
            return empty
        items = json.loads(r.content.decode('utf-8'))
        if not isinstance(items, list) or not items:
            return empty
    except Exception:
        return empty

    OPINION_KW = [
        ('강력매수', '강력매수'), ('적극매수', '적극매수'), ('비중확대', '비중확대'),
        ('매수', '매수'), ('Trading Buy', '매수'), ('OutPerform', '매수'), ('BUY', '매수'),
        ('중립', '중립'), ('HOLD', '중립'), ('Hold', '중립'), ('시장수익률', '중립'),
        ('매도', '매도'), ('SELL', '매도'),
    ]

    def _parse_target(text):
        m = re.search(r'목표주가[를\s:]*(\d[\d,]+)\s*원', text)
        if m:
            return int(m.group(1).replace(',', ''))
        m2 = re.search(r'목표주가[를\s:]*(\d+)\s*만원', text)
        if m2:
            return int(m2.group(1)) * 10000
        return None

    def _parse_opinion(text):
        for kw, label in OPINION_KW:
            if kw in text:
                return label
        return None

    reports, targets, opinions, full_previews = [], [], [], []

    for item in items:
        preview = item.get('previewContent', '')
        target  = _parse_target(preview)
        opinion = _parse_opinion(preview)
        rid     = item.get('researchId')
        reports.append({
            'id':      rid,
            'title':   item.get('title', ''),
            'firm':    item.get('brokerName', ''),
            'date':    item.get('writeDate', ''),
            'target':  target,
            'opinion': opinion,
            'preview': preview[:150].rstrip(),
            'url':     f'https://finance.naver.com/research/company_read.naver?nid={rid}',
        })
        full_previews.append(preview)
        if target:  targets.append(target)
        if opinion: opinions.append(opinion)

    # 컨센서스
    consensus = None
    if reports:
        top_opinion = Counter(opinions).most_common(1)[0][0] if opinions else None
        consensus = {
            'opinion':      top_opinion,
            'target_avg':   round(sum(targets) / len(targets)) if targets else None,
            'target_high':  max(targets) if targets else None,
            'target_low':   min(targets) if targets else None,
            'report_count': len(reports),
        }

    # 투자포인트: 완성된 문장/표현으로만 구성 (실적·매출 전망 우선)
    PERF_KW      = re.compile(r'영업이익|매출액|매출|순이익|실적|분기|반기|연간|성장|전망|YoY|QoQ|전년|전분기|증가|감소|흑자|적자|기대|예상|가이던스|EPS|BPS|ROE|수익성|원가|마진|점유율|출하|수주|수요|공급')
    SENT_BOILER  = re.compile(r'목표주가|투자의견|매수의견|(?:매수|중립|매도|BUY|HOLD|SELL)[^\w]|유지|상향|하향')
    TITLE_BOILER = re.compile(r'목표주가|투자의견|매수의견|(?:매수|중립|매도|BUY|HOLD|SELL)[^\w]')  # 제목엔 상향/하향 허용
    QUANT_KW     = re.compile(r'영업이익|매출액|순이익|EPS|BPS|ROE|YoY|QoQ|\d+조|\d+억|\d+%')
    _SENT_SP     = re.compile(r'(?<!\d)\.(?!\d)|。|\n|[■▶●◆▷►•◉▪]')

    def _clean_sent(s: str) -> str:
        s = re.sub(r'^\s*[\[【\(][^\]】\)]{1,20}[\]】\)]\s*', '', s)
        s = re.sub(r'^\s*[■▶●◆▷►•◉▪\-\*]+\s*', '', s)
        s = re.sub(r'^\s*\d[A-Z0-9F]*\s*(?:Preview|Review|Update|Comment)\s*[:：]\s*', '', s, flags=re.IGNORECASE)
        m = re.match(r'^([^:：]{2,40}[:：])\s*(.*)', s, re.DOTALL)
        if m and not QUANT_KW.search(m.group(1)) and len(m.group(2)) > 10:
            s = m.group(2)
        return s.strip()

    def _trim_to_complete(s: str, min_len: int = 20) -> str:
        """마지막 완성 문장 어미까지 자른다. 완성 불가능하면 빈 문자열."""
        s = s.strip()
        if len(s) >= min_len and re.search(r'[가-힣]\s*$', s):
            return s
        pos_list = [m.end() for m in re.finditer(r'[다요]\s*(?=[^가-힣]|$)', s)]
        if pos_list:
            t = s[:pos_list[-1]].rstrip('.,! ')
            if len(t) >= min_len and re.search(r'[가-힣]\s*$', t):
                return t
        return ''

    def _scrape_report_para(nid) -> str:
        """네이버 리서치 리포트 본문 첫 완성 문장 스크래핑."""
        if not nid:
            return ''
        try:
            url = f'https://finance.naver.com/research/company_read.naver?nid={nid}&page=1'
            rv = requests.get(url, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
                'Referer': 'https://finance.naver.com/research/',
            }, timeout=7)
            if rv.status_code != 200:
                return ''
            body = rv.text
            raw = ''
            for pat in [
                r'<td[^>]+class="view_cnt"[^>]*>(.*?)</td>',
                r'<td[^>]+class="[^"]*view_cnt[^"]*"[^>]*>(.*?)</td>',
                r'<div[^>]+class="[^"]*view_cnt[^"]*"[^>]*>(.*?)</div>',
            ]:
                m2 = re.search(pat, body, re.DOTALL)
                if m2:
                    raw = m2.group(1)
                    break
            if not raw:
                return ''
            raw = re.sub(r'<script[^>]*>.*?</script>', '', raw, flags=re.DOTALL)
            raw = re.sub(r'<style[^>]*>.*?</style>', '', raw, flags=re.DOTALL)
            text = re.sub(r'<[^>]+>', ' ', raw)
            text = re.sub(r'\s+', ' ', text).strip()
            # 완성 문장 2개 추출
            sents = re.split(r'(?<=[다요])\s+', text)
            parts = [s.strip() for s in sents if len(s.strip()) >= 20][:2]
            return ' '.join(parts)[:200] if parts else ''
        except Exception:
            return ''

    # ── 현재 분기 범위 계산 ────────────────────────────────────────────────
    _now      = datetime.now()
    _q_num    = (_now.month - 1) // 3 + 1          # 1~4
    _q_yr2    = str(_now.year)[2:]                  # '26'
    _pq_num   = _q_num - 1 if _q_num > 1 else 4
    _pq_yr2   = _q_yr2 if _q_num > 1 else str(int(_q_yr2) - 1)
    _q_start  = datetime(_now.year, (_q_num - 1) * 3 + 1, 1).strftime('%Y-%m-%d')

    # 이전 분기만 언급하는 문장 패턴 (현재/미래 키워드가 없으면 제외)
    PREV_Q_PAT  = re.compile(
        rf'{_pq_num}Q{_pq_yr2}|{_pq_yr2}년?\s*{_pq_num}분기|전분기|직전분기'
    )
    CURR_FWD_PAT = re.compile(
        rf'{_q_num}Q{_q_yr2}|{_q_yr2}년?\s*{_q_num}분기'
        rf'|{_q_yr2}F|FY{_q_yr2}|전망|예상|기대|가이던스|하반기|연간|향후|올해'
    )

    def _is_prev_q_only(text: str) -> bool:
        """이전 분기만 언급하고 현재·미래 관련 내용이 없으면 True."""
        return bool(PREV_Q_PAT.search(text)) and not bool(CURR_FWD_PAT.search(text))

    # ── 현재 분기 리포트만 사용 (없으면 invest_points 비움) ────────────────
    cq_pairs    = [(r, p) for r, p in zip(reports, full_previews)
                   if r.get('date', '') >= _q_start]
    if not cq_pairs:
        result = {'reports': reports, 'consensus': consensus, 'invest_points': []}
        _naver_reports_cache[code] = result
        return result

    cq_reports  = [r for r, _ in cq_pairs]
    cq_previews = [p for _, p in cq_pairs]

    invest_points: list = []
    seen_pts: set = set()

    def _near_dup_pt(text: str) -> bool:
        """첫 20자 또는 15자 포함 관계로 기존 항목과 유사 여부 확인."""
        pre20 = text[:20]
        pre15 = text[:15]
        for s in seen_pts:
            if pre20 == s[:20]:
                return True
            if len(pre15) >= 15 and (pre15 in s or s[:15] in text):
                return True
        return False

    # 1순위: 실적·전망 키워드 포함 리포트 제목 (상향/하향 허용)
    for rpt in cq_reports:
        title = rpt['title'].strip()
        if (title and len(title) >= 6
                and PERF_KW.search(title) and not TITLE_BOILER.search(title)
                and not _is_prev_q_only(title)
                and not _near_dup_pt(title)):
            seen_pts.add(title)
            invest_points.append(title)
        if len(invest_points) >= 4:
            break

    # 2순위: previewContent 완성 문장
    for full_prev in cq_previews:
        if len(invest_points) >= 4:
            break
        for raw in _SENT_SP.split(full_prev):
            sent     = _clean_sent(raw)
            complete = _trim_to_complete(sent)
            if (complete
                    and PERF_KW.search(complete)
                    and not SENT_BOILER.search(complete)
                    and not _is_prev_q_only(complete)
                    and not _near_dup_pt(complete)):
                seen_pts.add(complete)
                invest_points.append(complete[:130])
                break

    # 3순위: 리포트 본문 스크래핑 → 완성 실적·전망 문장 (현재 분기 상위 3건 병렬)
    if len(invest_points) < 4:
        scrape_ids = [rpt['id'] for rpt in cq_reports[:3]]
        with ThreadPoolExecutor(max_workers=3) as scr_ex:
            scraped_texts = list(scr_ex.map(_scrape_report_para, scrape_ids))
        for s_text in scraped_texts:
            if len(invest_points) >= 4 or not s_text:
                break
            for raw in _SENT_SP.split(s_text):
                sent     = _clean_sent(raw)
                complete = _trim_to_complete(sent)
                if (complete
                        and PERF_KW.search(complete)
                        and not SENT_BOILER.search(complete)
                        and not _is_prev_q_only(complete)
                        and not _near_dup_pt(complete)):
                    seen_pts.add(complete)
                    invest_points.append(complete[:130])
                    break

    # 현재 분기 내용이 없으면 표시 안 함 (4순위 일반 보완 없음)

    result = {'reports': reports, 'consensus': consensus, 'invest_points': invest_points[:4]}
    _naver_reports_cache[_cache_key] = result
    return result


def _dart_biz_summary(corp_code: str) -> dict:
    """DART 최신 사업보고서 ZIP에서 주요 사업 내용 텍스트 + 키워드 추출 (결과 캐시)."""
    if corp_code in _dart_biz_cache:
        return _dart_biz_cache[corp_code]

    import zipfile, io, re

    # 최신 사업보고서 rcept_no 조회
    now = datetime.now()
    d = _dart_req('list.json', {
        'corp_code': corp_code,
        'bgn_de': f'{now.year - 3}0101',
        'pblntf_detail_ty': 'A001',
        'sort': 'date', 'sort_mth': 'desc',
        'page_count': '5',
    }, timeout=12)
    empty = {'text': '', 'period': ''}
    if not d or not d.get('list'):
        _dart_biz_cache[corp_code] = empty
        return empty

    report   = d['list'][0]
    rcept_no = report['rcept_no']
    period   = f"{report.get('rcept_dt', '')[:4]}년 사업보고서"

    # 문서 ZIP 다운로드
    try:
        res  = requests.get(f"{DART_BASE}/document.xml",
                            params={'crtfc_key': DART_API_KEY, 'rcept_no': rcept_no},
                            timeout=60)
        z    = zipfile.ZipFile(io.BytesIO(res.content))
        # 파일 크기 내림차순 → 가장 큰 XML 파일이 본문 내용
        xml_infos = [(n, z.getinfo(n).file_size)
                     for n in z.namelist() if n.endswith('.xml')]
        xml_infos.sort(key=lambda x: x[1], reverse=True)
        content = z.read(xml_infos[0][0]).decode('utf-8', errors='replace')
    except Exception:
        result = {'text': '', 'period': period}
        _dart_biz_cache[corp_code] = result
        return result

    # 섹션 마커 우선순위
    MARKERS = [
        '라. 주요 사업의 내용',
        '1. 사업의 개요',
        '사업의 개요',
        'II. 사업의 내용',
        'Ⅱ. 사업의 내용',
        '2. 주요 제품 및 서비스',
    ]

    biz_text = ''
    for marker in MARKERS:
        pos = content.find(marker)
        if pos < 0:
            continue
        chunk = content[pos: pos + 40000]
        nxt = re.search(r'<TITLE[^>]+ATOC="Y"', chunk[100:])
        bound = (nxt.start() + 100) if nxt else len(chunk)
        chunk = chunk[:bound]

        texts, total = [], 0
        # P, SPAN, TD, LI 태그에서 텍스트 수집 (4자 이상)
        for m in re.finditer(r'<(?:P|SPAN|TD|LI|DIV)(?:\s[^>]*)?>([^<]{4,})</', chunk):
            t = re.sub(r'\s+', ' ', m.group(1)).strip()
            if len(t) < 4 or re.fullmatch(r'[\d\s%,\.]+', t):
                continue
            texts.append(t)
            total += len(t)
            if total >= 4000 or len(texts) >= 30:
                break
        if texts:
            biz_text = '\n'.join(texts)
            break

    result = {'text': biz_text, 'period': period, 'rcept_no': rcept_no,
              'keywords': _extract_dart_biz_keywords(biz_text)}
    _dart_biz_cache[corp_code] = result
    return result


_fnguide_cache: dict = {}  # code → fnguide data

_wr_cf1002_cache: dict = {}


def _wr_cf1002_estimates(code: str, frq: str) -> list:
    """WiseReport cF1002.aspx(cTB25) — 실적 3기 + 추정(E) 2기, 매출액/영업이익/당기순이익 포함.

    finance.naver.com/item/coinfo.naver 의 Financial Summary 그리드가 이 AJAX로
    로드된다. thead가 단순(재무년월 1개 + 지표별 고정 컬럼)해서 cTB26보다 훨씬
    안전하게 파싱 가능. frq='0'=연간, frq='1'=분기. 컬럼 순서(재무년월 다음부터):
    매출액금액, 매출액YoY, 영업이익, 당기순이익, EPS, PER, PBR, ROE, EV/EBITDA, 순부채비율, 주재무제표.
    """
    cache_key = f'{code}:{frq}'
    if cache_key in _wr_cf1002_cache:
        return _wr_cf1002_cache[cache_key]

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': f'https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd={code}',
    }
    try:
        r = requests.get(
            f'https://navercomp.wisereport.co.kr/v2/company/cF1002.aspx'
            f'?cmp_cd={code}&finGubun=MAIN&frq={frq}&rpt=0&finAcctClass=&cn=',
            headers=headers, timeout=10)
        r.encoding = 'utf-8'
        html = r.text
    except Exception:
        _wr_cf1002_cache[cache_key] = []
        return []

    def _num(raw):
        s = re.sub(r'<[^>]+>', '', raw).replace(',', '').strip()
        if not s or s in ('N/A', '-'):
            return None
        try:
            return float(s)
        except Exception:
            return None

    rows = []
    for row_m in re.finditer(r"<td class='center'>([^<]+)</td>(.*?)</tr>", html, re.DOTALL):
        period_raw = row_m.group(1).strip()
        is_est = '(E)' in period_raw
        period = period_raw.replace('(A)', '').replace('(E)', '').strip()
        tds = re.findall(r"<td class='num'>(.*?)</td>", row_m.group(2), re.DOTALL)
        if len(tds) < 4:
            continue
        rev, op, ni = _num(tds[0]), _num(tds[2]), _num(tds[3])
        if rev is None and op is None and ni is None:
            continue
        rows.append({
            'period': period, 'is_estimate': is_est,
            'revenue': rev, 'op_profit': op, 'net_profit': ni,
            'op_margin': round(op / rev * 100, 1) if op is not None and rev else None,
        })

    _wr_cf1002_cache[cache_key] = rows
    return rows


def _fnguide_data(code: str) -> dict:
    """FnGuide에서 제품비율(키워드), 시장점유율, 투자의견, 목표주가 수집."""
    import xml.etree.ElementTree as ET, re as _re
    if code in _fnguide_cache:
        return _fnguide_cache[code]

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://comp.fnguide.com/',
        'Accept-Language': 'ko-KR,ko;q=0.9',
    }
    result = {'keywords': [], 'products': [], 'market_shares': [], 'consensus': None,
              'next_earnings': None, 'financial_highlight': []}

    # ── XML 데이터 (제품비율 / 시장점유율) ──────────────────
    try:
        xr = requests.get(
            f'https://comp.fnguide.com/SVO2/xml/corp_ifrs/{code}.xml',
            headers=headers, timeout=12
        )
        xml_text = xr.content.decode('euc-kr').replace('encoding="euc-kr"', 'encoding="utf-8"')
        root = ET.fromstring(xml_text.encode('utf-8'))

        pr = root.find('product_rate')
        if pr is not None:
            for rec in pr.findall('record'):
                name  = (rec.findtext('name')  or '').strip()
                value = (rec.findtext('value') or '').strip()
                if not name or '내부거래' in name or name.startswith('기타'):
                    continue
                try:
                    pct = float(value.replace(',', ''))
                    if pct > 0:
                        result['products'].append({'name': name, 'pct': round(pct, 1)})
                except Exception:
                    pass

        imr = root.find('imp_mkt_ratio')
        if imr is not None:
            for rec in imr.findall('record'):
                pl = (rec.findtext('prod_list')  or '').strip()
                pv = (rec.findtext('prod_ratio') or '').strip()
                if pl:
                    result['market_shares'].append({'product': pl, 'share': pv})

        # 주요 사업영역 키워드 4개: product_rate 카테고리명 우선
        seen, keywords = set(), []
        for p in result['products']:
            kw = _re.split(r'[,·ㆍ/]', p['name'])[0].strip()
            kw = _re.sub(r'\s*(등|및)$', '', kw).strip()
            if kw and 2 <= len(kw) <= 15 and kw not in seen:
                seen.add(kw)
                keywords.append(kw)
        result['keywords'] = keywords[:4]
    except Exception as e:
        print(f'[FnGuide XML] 오류: {e}')

    # ── FnGuide SVD_Main: Financial Summary (연결 분기/연간) + WiseReport 컨센서스 ──
    try:
        import re as _re2, html as _html_esc2

        _fng_hdr = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/124.0 Safari/537.36',
            'Referer':    'https://comp.fnguide.com/',
            'Accept-Language': 'ko-KR,ko;q=0.9',
        }
        _wr_hdr = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/124.0 Safari/537.36',
            'Referer':    'https://navercomp.wisereport.co.kr/',
            'Accept-Language': 'ko-KR,ko;q=0.9',
        }

        def _fng_fetch(url):
            r = requests.get(url, headers=_fng_hdr, timeout=15)
            ct = r.headers.get('content-type', '').lower()
            r.encoding = 'utf-8' if 'utf' in ct else 'euc-kr'
            return r.text

        def _wr_fetch(url):
            r = requests.get(url, headers=_wr_hdr, timeout=12)
            r.encoding = 'utf-8'
            return r.text

        def _extract_table(html_text, tid, window=25000):
            p = html_text.find(f'id="{tid}"')
            if p < 0:
                return ''
            e = html_text.find('</table>', p)
            return html_text[p: e + 8] if e > 0 else html_text[p: p + window]

        def _extract_div_table(html_text, div_id):
            """div_id 내 첫 번째 <table>...</table> 추출."""
            p = html_text.find(f'id="{div_id}"')
            if p < 0:
                return ''
            ts = html_text.find('<table', p)
            if ts < 0 or ts > p + 1000:
                return ''
            te = html_text.find('</table>', ts)
            return html_text[ts: te + 8] if te > 0 else ''

        def _parse_fng_table(tbl_html, re_mod, today_ym):
            """FnGuide SVD_Main highlight table 파싱 → fh_list."""
            if not tbl_html:
                return []
            thead_m = re_mod.search(r'<thead[^>]*>(.*?)</thead>', tbl_html, re_mod.DOTALL)
            thead_content = thead_m.group(1) if thead_m else tbl_html[:2000]
            # <th> 내 HTML 태그를 제거한 뒤 YYYY/MM 과 (E) 마커 감지
            # → <span>(E)</span>, <em>(E)</em>, 개행 등 어떤 HTML 구조에도 대응
            all_th = re_mod.findall(r'<th[^>]*>(.*?)</th>', thead_content, re_mod.DOTALL)
            periods = []
            for _th in all_th:
                _plain = re_mod.sub(r'<[^>]+>', '', _th).replace('\xa0', '').strip()
                _plain = re_mod.sub(r'\s+', ' ', _plain).strip()
                _m = re_mod.search(r'(\d{4}/\d{2})', _plain)
                if _m:
                    _p  = _m.group(1)
                    _e  = '(E)' in _plain or '(e)' in _plain.lower()
                    periods.append((_p, _e))
            if not periods:
                return []

            fin_data = {}
            for row_m in re_mod.finditer(r'<tr[^>]*>(.*?)</tr>', tbl_html, re_mod.DOTALL):
                row_html = row_m.group(1)
                th_m = re_mod.search(
                    r'<th[^>]*scope=["\']row["\'][^>]*>(.*?)</th>', row_html, re_mod.DOTALL)
                if not th_m:
                    continue
                th_content = th_m.group(1)
                # 행 레이블: FnGuide는 <a>태그 안에 지표명, <span class="unt">는 단위
                a_m = re_mod.search(r'<a[^>]*>\s*([^<]+?)\s*</a>', th_content)
                if a_m:
                    name = a_m.group(1).strip()
                else:
                    raw = re_mod.sub(r'<[^>]+>', ' ', th_content)
                    raw = re_mod.sub(r'\s+', ' ', raw).strip()
                    # 단위 문자열 "(억원)", "(%)" 등 제거
                    name = re_mod.sub(r'\s*\([^)]+\)\s*$', '', raw).strip()
                name = re_mod.sub(r'\s+', ' ', name)
                if not name:
                    continue
                tds = re_mod.findall(r'<td[^>]*>(.*?)</td>', row_html, re_mod.DOTALL)
                vals = []
                for td in tds:
                    # <span>숫자</span> 우선, 없으면 <td> 직접 텍스트
                    sp2 = re_mod.search(r'<span[^>]*>([-\d,\.]+)</span>', td)
                    if sp2:
                        try:
                            vals.append(float(sp2.group(1).replace(',', '')))
                        except Exception:
                            vals.append(None)
                    else:
                        clean = re_mod.sub(r'<[^>]+>', '', td).strip().replace(',', '')
                        if clean and re_mod.match(r'^-?[\d.]+$', clean):
                            try:
                                vals.append(float(clean))
                            except Exception:
                                vals.append(None)
                        else:
                            vals.append(None)
                fin_data[name] = vals

            def _pick(*names):
                for n in names:
                    if n in fin_data and any(v is not None for v in fin_data[n]):
                        return fin_data[n]
                return []

            rev_v = _pick('매출액', '영업수익', '이자수익', '보험료수익')
            op_v  = _pick('영업이익', '영업이익(발표기준)', '영업이익(손실)')
            ni_v  = _pick('당기순이익', '당기순이익(지배)', '당기순이익(지배주주)',
                          '지배주주순이익', '당기순이익(손실)')

            fh = []
            for i, (period, is_est) in enumerate(periods):
                rev = rev_v[i] if i < len(rev_v) else None
                op  = op_v[i]  if i < len(op_v)  else None
                ni  = ni_v[i]  if i < len(ni_v)  else None
                if not any(v is not None for v in (rev, op, ni)):
                    continue
                fh.append({
                    'period':      period,
                    'is_estimate': is_est or period > today_ym,
                    'revenue':     rev,
                    'op_profit':   op,
                    'net_profit':  ni,
                    'op_margin':   round(op / rev * 100, 1) if op and rev and rev > 0 else None,
                    'op_growth':   None,
                })
            return fh

        with ThreadPoolExecutor(max_workers=2) as _wr_ex:
            _f_cf = _wr_ex.submit(
                _fng_fetch,
                f'https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp'
                f'?pGB=1&gicode=A{code}&cID=&MenuYn=Y&ReportGB=&NewMenuID=101&stkGb=701')
            _f_c1 = _wr_ex.submit(
                _wr_fetch,
                f'https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd={code}&cn=')
        cf_html = _f_cf.result()
        c1_html = _f_c1.result()

        # ── ① Financial Summary: FnGuide SVD_Main highlight 테이블 ──────────
        today_ym = datetime.now().strftime('%Y/%m')

        # 연결/분기: FnGuide div ID 후보 순서대로 시도 (사이트 버전별 ID 차이 대응)
        qtr_tbl = ''
        _tried_q = set()
        for _qid in ('highlight_D_E', 'highlight_D_Q', 'highlight_A_E', 'highlight_A_Q'):
            _tried_q.add(_qid)
            qtr_tbl = _extract_div_table(cf_html, _qid)
            if qtr_tbl:
                print(f'[FnGuide] {code} 분기 div={_qid}')
                break
        if not qtr_tbl:
            # 동적 탐색: cf_html에 있는 모든 highlight_ div 순차 시도
            for _dm in _re2.finditer(r'id="(highlight_[^"]+)"', cf_html):
                _did = _dm.group(1)
                if _did in _tried_q:
                    continue
                _tried_q.add(_did)
                _t = _extract_div_table(cf_html, _did)
                if not _t:
                    continue
                _fh_t = _parse_fng_table(_t, _re2, today_ym)
                if not _fh_t:
                    continue
                # 분기 판별: 기간들의 월이 다양하면 분기 테이블
                _months = {p.split('/')[1] for p, _ in
                           [(_r['period'], None) for _r in _fh_t] if '/' in p}
                if len(_months) > 1:
                    qtr_tbl = _t
                    print(f'[FnGuide] {code} 분기 div={_did} (auto-discovered)')
                    break

        ann_tbl = ''
        _tried_a = set()
        for _aid in ('highlight_D_A', 'highlight_A_A'):
            _tried_a.add(_aid)
            ann_tbl = _extract_div_table(cf_html, _aid)
            if ann_tbl:
                print(f'[FnGuide] {code} 연간 div={_aid}')
                break
        if not ann_tbl:
            # 동적 탐색: cf_html에 있는 모든 highlight_ div 순차 시도
            for _dm in _re2.finditer(r'id="(highlight_[^"]+)"', cf_html):
                _did = _dm.group(1)
                if _did in _tried_q or _did in _tried_a:
                    continue
                _tried_a.add(_did)
                _t = _extract_div_table(cf_html, _did)
                if not _t:
                    continue
                _fh_t = _parse_fng_table(_t, _re2, today_ym)
                if not _fh_t:
                    continue
                # 연간 판별: 기간들의 월이 모두 같으면 연간 테이블
                _months = {p.split('/')[1] for p, _ in
                           [(_r['period'], None) for _r in _fh_t] if '/' in p}
                if len(_months) == 1:
                    ann_tbl = _t
                    print(f'[FnGuide] {code} 연간 div={_did} (auto-discovered)')
                    break

        qtr_fh  = _parse_fng_table(qtr_tbl, _re2, today_ym)
        ann_fh  = _parse_fng_table(ann_tbl, _re2, today_ym)

        if not qtr_fh and not ann_fh:
            # fallback: WiseReport cTB26 (연간4 + 분기4)
            wr_html = _wr_fetch(
                f'https://navercomp.wisereport.co.kr/v2/company/cF1001.aspx?cmp_cd={code}&cn=')
            tb26 = _extract_table(wr_html, 'cTB26')
            if tb26:
                # thead 구조가 20개 이상 컬럼으로 복잡 → 연간 레이블만 신뢰 가능
                # 연간: `<th class="sub line">\nYYYY/12<br>` 패턴 (결산월 기준)
                # data 행의 <td>는 [0-3]=연간실적, [4-7]=분기실적 → v[4]는 연간추정 아님
                # all_periods[4]='2026/12'은 thead에만 존재하는 레이블이므로 [:4]만 사용
                all_periods = _re2.findall(r'(\d{4}/\d{2})<br', tb26)
                ann_periods = all_periods[:4]
                fin_data_wr = {}
                for row_m in _re2.finditer(
                    r"<th scope='row'[^>]*>\s*([^<]+?)\s*</th>(.*?)</tr>",
                    tb26, _re2.DOTALL
                ):
                    nm = _re2.sub(r'\s+', ' ', row_m.group(1)).strip()
                    tds_html = _re2.findall(r'<td[^>]*>(.*?)</td>', row_m.group(2), _re2.DOTALL)
                    vals = []
                    for td in tds_html:
                        sp = _re2.search(r'<span[^>]*>([-\d,\.]+)</span>', td)
                        if sp:
                            try: vals.append(float(sp.group(1).replace(',', '')))
                            except: vals.append(None)
                        else:
                            vals.append(None)
                    if nm:
                        fin_data_wr[nm] = vals

                _lbl_rev = ('매출액', '영업수익', '이자수익', '보험료수익')
                _lbl_op  = ('영업이익', '영업이익(발표기준)', '영업이익(손실)')
                _lbl_ni  = ('당기순이익', '당기순이익(지배)', '당기순이익(지배주주)',
                            '지배주주순이익', '당기순이익(손실)')

                n_ann = len(ann_periods)
                _ann = {k: v[:n_ann] for k, v in fin_data_wr.items() if len(v) >= 1}

                def _pk(d, *names):
                    for n in names:
                        if n in d and any(v is not None for v in d[n]):
                            return d[n]
                    return []

                rev_a = _pk(_ann, *_lbl_rev); op_a = _pk(_ann, *_lbl_op); ni_a = _pk(_ann, *_lbl_ni)

                def _row_wr(rv, ov, nv, period, i, is_est=False):
                    rev = rv[i] if i < len(rv) else None
                    op  = ov[i] if i < len(ov) else None
                    ni  = nv[i] if i < len(nv) else None
                    if not any(v is not None for v in (rev, op, ni)):
                        return None
                    return {
                        'period': period, 'is_estimate': is_est or period > today_ym,
                        'revenue': rev, 'op_profit': op, 'net_profit': ni,
                        'op_margin': round(op / rev * 100, 1) if op and rev and rev > 0 else None,
                        'op_growth': None,
                    }

                ann_fh = [r for r in (
                    _row_wr(rev_a, op_a, ni_a, p, i) for i, p in enumerate(ann_periods)) if r]
                # cTB26 분기 레이블이 신뢰 불가 (thead 복잡) → DART 사용
                qtr_fh = []
                print(f'[WiseReport cTB26 fallback] {code} ann={len(ann_fh)} '
                      f'ann_periods={ann_periods}')

                # ── (E) 보완: 여러 WiseReport 소스 순차 탐색 ─────────────────────
                def _scan_tables_for_fh(html_text, src_label, skip_ids=None):
                    """html_text 내 모든 cTBxx 테이블을 탐색 → (ann_fh, qtr_fh) 반환."""
                    _a, _q = [], []
                    skip_ids = skip_ids or set()
                    for _tm in _re2.finditer(r'id="(cTB\d+)"', html_text):
                        _tid = _tm.group(1)
                        if _tid in skip_ids:
                            continue
                        _t = _extract_table(html_text, _tid)
                        if not _t:
                            continue
                        _fh = _parse_fng_table(_t, _re2, today_ym)
                        if not _fh or len(_fh) < 2:
                            continue
                        if not any(r.get('revenue') is not None for r in _fh):
                            continue
                        _mo = {p.split('/')[1] for p in (r['period'] for r in _fh)
                               if '/' in p}
                        if len(_mo) == 1 and not _a:
                            _a = _fh
                            print(f'[{src_label}] {code} {_tid} 연간 {len(_fh)}건')
                        elif len(_mo) > 1 and not _q:
                            _q = _fh
                            print(f'[{src_label}] {code} {_tid} 분기 {len(_fh)}건')
                        if _a and _q:
                            break
                    return _a, _q

                # ① c1_html (c1010001.aspx) — 이미 fetch됨
                _ex_ann, _ex_qtr = _scan_tables_for_fh(
                    c1_html, 'c1_html', skip_ids={'cTB15'})

                # ② wr_html (cF1001.aspx) — cTB26 외 다른 테이블
                if not _ex_ann and not _ex_qtr:
                    _ex_ann, _ex_qtr = _scan_tables_for_fh(
                        wr_html, 'cF1001', skip_ids={'cTB26'})

                # ③ WiseReport cF2001.aspx (재무제표 상세) 추가 시도
                if not _ex_ann and not _ex_qtr:
                    _wr2 = _wr_fetch(
                        f'https://navercomp.wisereport.co.kr/v2/company/'
                        f'cF2001.aspx?cmp_cd={code}&cn=')
                    _ex_ann, _ex_qtr = _scan_tables_for_fh(_wr2, 'cF2001')

                # 발견된 추정치를 기존 cTB26 실적 위에 merge
                if _ex_ann:
                    _ex_periods = {r['period'] for r in _ex_ann}
                    _extra = [r for r in ann_fh if r['period'] not in _ex_periods]
                    ann_fh = sorted(_extra + _ex_ann, key=lambda x: x['period'])
                if _ex_qtr:
                    qtr_fh = _ex_qtr
            else:
                ann_fh, qtr_fh = [], []

        if qtr_fh or ann_fh:
            for j in range(1, len(qtr_fh)):
                qtr_fh[j]['op_growth'] = _growth(
                    qtr_fh[j].get('op_profit'), qtr_fh[j - 1].get('op_profit'))
            result['annual_highlight']    = ann_fh
            result['financial_highlight'] = qtr_fh
            _ann_e = sum(1 for r in ann_fh if r.get('is_estimate'))
            _qtr_e = sum(1 for r in qtr_fh if r.get('is_estimate'))
            _src   = 'FnGuide' if (qtr_tbl or ann_tbl) else 'cTB26+보완'
            print(f'[재무하이라이트] {code} src={_src} '
                  f'ann={len(ann_fh)}(E:{_ann_e}) qtr={len(qtr_fh)}(E:{_qtr_e})')
        else:
            print(f'[재무하이라이트] {code} 재무 데이터 미발견')

        # ── 예상실적: WiseReport cF1002.aspx(cTB25) — 매출액/영업이익/당기순이익
        # 실적3기+추정(E)2기를 안전하게 제공 (thead 단순, cTB26보다 신뢰도 높음).
        # 분기 우선, 분기 추정이 전혀 없는 종목은 연간으로 폴백.
        cf_qtr = [r for r in _wr_cf1002_estimates(code, '1') if r['is_estimate']]
        if cf_qtr:
            result['next_earnings'] = {'freq': 'quarter', 'periods': cf_qtr[:2]}
        else:
            cf_ann = [r for r in _wr_cf1002_estimates(code, '0') if r['is_estimate']]
            if cf_ann:
                result['next_earnings'] = {'freq': 'annual', 'periods': cf_ann[:2]}

        # ── ② 컨센서스: cTB15 (투자의견/목표주가/EPS/PER/추정기관수) ────
        pos_tb15 = c1_html.find('id="cTB15"')
        tb15 = _extract_table(c1_html, 'cTB15', window=3000) if pos_tb15 >= 0 else ''
        if tb15:
            # 기준일: cTB15 앞 8000자에서 [??:YYYY.MM.DD] 패턴 (마지막=가장 근접)
            cons_date = None
            before_tb15 = c1_html[max(0, pos_tb15 - 8000): pos_tb15]
            date_hits = _re2.findall(r'\[.{0,6}:\s*(\d{4}\.\d{2}\.\d{2})\]', before_tb15)
            if date_hits:
                cons_date = date_hits[-1]

            trs = _re2.findall(r'<tr[^>]*>(.*?)</tr>', tb15, _re2.DOTALL)
            data_trs = [t for t in trs if '<td' in t]
            if data_trs:
                data_tr = data_trs[-1]
                tds = _re2.findall(r'<td[^>]*>(.*?)</td>', data_tr, _re2.DOTALL)
                def _td_clean(raw):
                    t = _re2.sub(r'<[^>]+>', '', raw)
                    t = _html_esc2.unescape(t).replace('\xa0', '').strip()
                    return t if t and t not in ('-', '–', '—') else None
                clean = [_td_clean(td) for td in tds]
                opin_lbl  = {'1': '강력매도', '2': '매도', '3': '중립', '4': '매수', '5': '강력매수'}
                opin_rev  = {v: k for k, v in opin_lbl.items()}
                op_raw = clean[0] if clean else None
                op_num = ''
                if op_raw:
                    if op_raw in opin_rev:
                        op_num = opin_rev[op_raw]
                    elif op_raw.replace('.', '').lstrip('-').isdigit():
                        op_num = str(round(float(op_raw)))
                cons = {
                    'opinion':       op_num,
                    'opinion_label': opin_lbl.get(op_num, op_raw or ''),
                    'target_price':  clean[1] if len(clean) > 1 else None,
                    'eps':           clean[2] if len(clean) > 2 else None,
                    'per':           clean[3] if len(clean) > 3 else None,
                    'analyst_count': clean[4] if len(clean) > 4 else None,
                    'date':          cons_date,
                }
                if any(v for v in cons.values() if v):
                    result['consensus'] = cons
                print(f'[WiseReport cTB15] {code} consensus={cons}')

    except Exception as e:
        print(f'[WiseReport] 오류: {e}')
        import traceback; traceback.print_exc()

    _fnguide_cache[code] = result
    return result


# 제목에 반드시 있어야 하는 제품·실적 키워드 (단독 단어로 매칭)
_STRICT_PROD = re.compile(
    r'|매출액?|영업이익|실적|순이익|마진|원가'
    r'|수주|계약|공급량?|출하|양산|신제품|신기술'
    r'|공장|증설|팹|클러스터|설비투자'
    r'|시장점유율|업황|수요|공급망|가이던스|전망치'
    r'|인수합병|특허|MOU|협약'
)
# 주가·지수 움직임 중심 패턴 — 제목에 있으면 제외
# 급락/급등은 단독 시 상품 수요 맥락도 있으므로 '주가/지수+급락/급등' 조합만 제외
_PRICE_MOVE = re.compile(
    r'코스피|코스닥|증시|지수|서킷브레이커'
    r'|외국인\s*(?:순매수|순매도|매수|매도)'
    r'|기관\s*(?:순매수|순매도|매수|매도)'
    r'|개인\s*(?:순매수|순매도|매수|매도)'
    r'|매물|변동성|쏠림|하락세|상승세|낙폭'
    r'|주가[^\d가-힣]'                               # 주가 뒤 숫자/한글 아닌 경우
    r'|(?:주가|지수)\s*(?:급락|급등|폭락|폭등)'      # 주가/지수+급락/급등 조합
)

_naver_issue_cache: dict = {}




def _trim_complete_sent(text: str, max_len: int = 160) -> str:
    """마지막 완성 문장 어미까지 잘라 반환. 완성 불가능하면 원문 그대로."""
    t = (text or '').strip()[:max_len]
    if not t:
        return ''
    if re.search(r'[가-힣]\s*$', t):
        return t
    pos_list = [m.end() for m in re.finditer(r'[다요]\s*(?=[^가-힣]|$)', t)]
    if pos_list:
        trimmed = t[:pos_list[-1]].rstrip('.,! ')
        if len(trimmed) >= 15:
            return trimmed
    # 마지막 한글 위치까지
    last_ko = None
    for last_ko in re.finditer(r'[가-힣]', t):
        pass
    if last_ko and last_ko.end() >= 15:
        return t[:last_ko.end()].strip()
    return t


def _naver_issue_news(code: str, stock_name: str = '') -> list:
    """네이버 검색 Open API — 종목명으로 최근 뉴스 수집 (제품·실적 관련 5건)."""
    import html as _html
    from email.utils import parsedate_to_datetime

    cache_key = f"{code}:{stock_name}"
    if cache_key in _naver_issue_cache:
        return _naver_issue_cache[cache_key]

    if not stock_name:
        return []

    # ── Naver 언론사 OID → 이름 (주요 경제·IT 매체) ──────────────────────
    _OID_MEDIA = {
        '001': '연합뉴스',  '003': '뉴시스',    '008': '머니투데이',
        '009': '매일경제',  '011': '서울신문',   '013': '헤럴드경제',
        '014': '파이낸셜뉴스', '015': '한국경제', '018': '이데일리',
        '020': '동아일보',  '022': '세계일보',   '023': '조선일보',
        '025': '중앙일보',  '028': '한겨레',     '030': '전자신문',
        '032': '경향신문',  '040': 'YTN',        '047': 'SBS',
        '057': 'MBC',       '060': 'KBS',        '066': '서울경제',
        '077': '중앙일보',  '082': '한경비즈니스','092': '한국경제',
        '101': '뉴스1',     '138': '한국경제',   '144': '조선비즈',
        '215': '이코노믹리뷰', '277': '아시아경제', '297': '비즈니스포스트',
        '421': '뉴데일리',  '449': 'MBN',
    }

    # 종목명 → 제목 매칭용 패턴 (전체 이름 + 한글만 추출한 부분)
    _ko_only = re.sub(r'[^가-힣]', '', stock_name)
    _name_variants = [stock_name]
    if len(_ko_only) >= 3 and _ko_only != stock_name:
        _name_variants.append(_ko_only)
    _name_pat = re.compile('|'.join(re.escape(n) for n in _name_variants))

    try:
        res = requests.get(
            'https://openapi.naver.com/v1/search/news.json',
            params={'query': stock_name, 'display': 100, 'start': 1, 'sort': 'date'},
            headers={
                'X-Naver-Client-Id':     NAVER_CLIENT_ID,
                'X-Naver-Client-Secret': NAVER_CLIENT_SECRET,
                'User-Agent': 'Mozilla/5.0',
            },
            timeout=8,
        )
        if res.status_code != 200:
            return []   # 오류는 캐시하지 않음 — 재시도 가능하도록
        items = res.json().get('items', [])
    except Exception:
        return []

    # ── 종목 주요사업 키워드로 _STRICT_PROD 보강 ─────────────────────────
    # fnguide 캐시에서 읽기 (메인 API 병렬 호출이 이미 채웠을 가능성 높음)
    _fng = _fnguide_data(code)
    _biz_kws  = _fng.get('keywords', [])
    _prod_kws = [p['name'] for p in _fng.get('products', []) if p.get('name')]
    _extra    = list({k for k in _biz_kws + _prod_kws if len(k) >= 2})
    if _extra:
        _prod_pat = re.compile(
            _STRICT_PROD.pattern + '|' + '|'.join(re.escape(k) for k in _extra)
        )
    else:
        _prod_pat = _STRICT_PROD

    _SUB_EXCL  = re.compile(r'코스피|코스닥|증시|서킷브레이커')
    seen_prefix: set  = set()
    prod_news:   list = []

    for item in items:
        title = _html.unescape(re.sub(r'<[^>]+>', '', item.get('title', '')))
        desc  = _html.unescape(re.sub(r'<[^>]+>', '', item.get('description', '') or ''))
        link  = item.get('link', '') or item.get('originallink', '')

        if not title:
            continue

        # 날짜 파싱 (RFC 2822 → YYYY-MM-DD)
        try:
            date_str = parsedate_to_datetime(item.get('pubDate', '')).strftime('%Y-%m-%d')
        except Exception:
            date_str = ''

        # oid 추출 → 언론사명 매핑
        m_oid = re.search(r'/article/(\d+)/', link)
        media = _OID_MEDIA.get(m_oid.group(1), '') if m_oid else ''

        # ① 종목명이 제목에 직접 언급된 경우만 통과 (설명에만 있는 관련 뉴스 제외)
        if not _name_pat.search(title):
            continue

        # ② 제품·실적 키워드 필수 / 시장 지수 뉴스 제외
        has_prod  = bool(_prod_pat.search(title) or _prod_pat.search(desc[:100]))
        has_price = bool(_PRICE_MOVE.search(title)  or _SUB_EXCL.search(desc[:80]))
        if not has_prod or has_price:
            continue

        # 제목 앞 20자 중복 제거
        key = title[:20]
        if key in seen_prefix:
            continue
        seen_prefix.add(key)

        prod_news.append({
            'title':   title,
            'date':    date_str,
            'media':   media,
            'url':     link,
            'summary': _trim_complete_sent(desc),   # 검색 API description 직접 사용
        })

    result = prod_news[:5]
    _naver_issue_cache[cache_key] = result
    return result


# 반복성 높은 지분/임원 관련 공시 — 사업·제품 현황과 무관하므로 제외
_DISCL_EXCLUDE_PAT = re.compile(
    r'주식등의대량보유상황보고서|임원.주요주주특정증권등|특정증권등소유상황보고서|'
    r'최대주주.*변경|주식소각결정|전환청구권행사|신주인수권행사|'
    r'자기주식취득|자기주식처분|자기주식.*신탁계약|의결권.*불통일|'
    r'주주총회소집|사외이사.*선임|기타경영사항'
)


def _dart_recent_disclosures(corp_code: str) -> list:
    """DART 최근 공시 목록(최근 4개월) — 지분·임원 등 반복성 공시 제외, 최대 8건."""
    now = datetime.now()
    d = _dart_req('list.json', {
        'corp_code': corp_code,
        'bgn_de':    (now - timedelta(days=120)).strftime('%Y%m%d'),
        'end_de':    now.strftime('%Y%m%d'),
        'sort':      'date', 'sort_mth': 'desc',
        'page_count': '30',
    }, timeout=12)
    if not d or not d.get('list'):
        return []

    out = []
    for item in d['list']:
        nm = item.get('report_nm', '')
        if _DISCL_EXCLUDE_PAT.search(nm):
            continue
        rcept_no = item.get('rcept_no', '')
        raw_dt   = item.get('rcept_dt', '')
        date_fmt = f'{raw_dt[:4]}-{raw_dt[4:6]}-{raw_dt[6:]}' if len(raw_dt) == 8 else raw_dt
        out.append({
            'date':      date_fmt,
            'title':     nm.strip(),
            'rcept_no':  rcept_no,
            'url':       f'https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}',
        })
        if len(out) >= 8:
            break
    return out


@app.route('/api/dart/company-info')
def dart_company_info():
    """DART Open API 기반 기업정보(기본정보·실적·주주·경영진) + Naver 뉴스 조회."""
    code       = request.args.get('code', '').strip().zfill(6)
    stock_name = request.args.get('name', '').strip()
    if not code or not code.isdigit():
        return jsonify({'error': '유효한 종목코드 필요'}), 400

    # stock_code(6) → corp_code(8) 변환 (corpCode.xml 불일치 시 종목명으로 fallback)
    corp_code = _dart_stock_to_corp(code, stock_name)
    if not corp_code:
        return jsonify({'error': f'DART corp_code 없음 ({code})'}), 404

    # 기업 기본정보 + 병렬 데이터 수집
    cls_map = {'Y': '유가증권', 'K': '코스닥', 'N': '코넥스', 'E': '기타'}
    with ThreadPoolExecutor(max_workers=10) as ex:
        f_corp    = ex.submit(_dart_req, 'company.json', {'corp_code': corp_code})
        f_shr     = ex.submit(_dart_shareholders_latest, corp_code)
        f_exec    = ex.submit(_dart_executives_latest, corp_code)
        f_reports = ex.submit(_naver_reports, code)
        f_issues  = ex.submit(_naver_issue_news, code, stock_name)
        f_fng     = ex.submit(_fnguide_data, code)
        f_ann     = ex.submit(_dart_fin_annual, corp_code)
        f_qtr     = ex.submit(_dart_fin_quarters, corp_code)
        f_discl   = ex.submit(_dart_recent_disclosures, corp_code)
        corp_d         = f_corp.result() or {}
        shareholders   = f_shr.result()
        executives     = f_exec.result()
        invest_summary = f_reports.result()
        issues         = f_issues.result()
        fnguide        = f_fng.result()
        dart_annual    = f_ann.result()
        dart_quarters  = f_qtr.result()
        disclosures    = f_discl.result()

    # ── Financial Highlight 폴백 처리 ─────────────────────────────────────
    # 캐시된 dict를 직접 수정하지 않도록 얕은 복사
    fnguide = dict(fnguide)

    # ── DART 기간 정규화: '2022' → '2022/12', '2025Q1' → '2025/03' ────────
    acc_mt = str(corp_d.get('acc_mt', '12') or '12').zfill(2)

    for r in dart_annual:
        if re.match(r'^\d{4}$', r.get('period', '')):
            r['period'] = f"{r['period']}/{acc_mt}"

    fy_end_int = int(acc_mt)
    fy_start   = (fy_end_int % 12) + 1          # 결산월 다음달 = 사업연도 시작월
    for r in dart_quarters:
        qm = re.match(r'^(\d{4})Q(\d)$', r.get('period', ''))
        if qm:
            yr, q = int(qm.group(1)), int(qm.group(2))
            offset    = (fy_start - 1) + (q * 3) - 1
            end_month = offset % 12 + 1
            end_year  = yr + offset // 12
            r['period'] = f'{end_year}/{end_month:02d}'

    def _has_data(q):
        return (q.get('revenue') is not None
                or q.get('op_profit') is not None
                or q.get('net_profit') is not None)

    fh_qtr  = [q for q in fnguide.get('financial_highlight', []) if _has_data(q)]
    fh_ann  = [q for q in fnguide.get('annual_highlight',    []) if _has_data(q)]

    def _cap_estimates(rows, max_est=3):
        """(E) 항목을 max_est개까지만 포함. actual은 전부 유지."""
        actual = [r for r in rows if not r.get('is_estimate')]
        est    = [r for r in rows if r.get('is_estimate')]
        return actual + est[:max_est]

    if fh_qtr or fh_ann:
        # ① WiseReport 우선 + DART 이전 데이터 보완
        # 연간: WiseReport 4년 + DART 이전 연도 (날짜순, 중복 제거)
        wr_ann_set  = {r['period'] for r in fh_ann}
        extra_ann   = [r for r in dart_annual if r['period'] not in wr_ann_set]
        combined_ann = sorted(extra_ann + fh_ann, key=lambda x: x['period'])

        # 분기: DART 연속 분기 + FnGuide 미래 예정치 (중복 제거, 날짜순)
        dart_qtr_set  = {r['period'] for r in dart_quarters}
        wr_future     = [r for r in fh_qtr
                         if r.get('is_estimate') and r['period'] not in dart_qtr_set]
        combined_qtr  = sorted(dart_quarters + wr_future, key=lambda x: x['period'])
        for j in range(1, len(combined_qtr)):
            combined_qtr[j]['op_growth'] = _growth(
                combined_qtr[j].get('op_profit'), combined_qtr[j - 1].get('op_profit'))

        # 연간(E) 최대 3년, 분기(E) 최대 3분기 제한 → 마지막 8개
        fnguide['annual_highlight']    = _cap_estimates(combined_ann, 3)[-8:]
        fnguide['financial_highlight'] = _cap_estimates(combined_qtr, 3)[-8:]
        fnguide['fh_source'] = 'FnGuide'
    elif dart_annual or dart_quarters:
        # ② DART만 있는 경우
        combined_qtr = sorted(dart_quarters, key=lambda x: x['period'])
        for j in range(1, len(combined_qtr)):
            combined_qtr[j]['op_growth'] = _growth(
                combined_qtr[j].get('op_profit'), combined_qtr[j - 1].get('op_profit'))
        fnguide['financial_highlight'] = combined_qtr[-8:]
        fnguide['annual_highlight']    = sorted(dart_annual, key=lambda x: x['period'])
        fnguide['fh_source'] = 'DART'
    else:
        fnguide['financial_highlight'] = []
        fnguide['annual_highlight']    = []
        fnguide['fh_source'] = 'none'

    est = corp_d.get('est_dt', '')
    if len(est) == 8:
        est = f"{est[:4]}-{est[4:6]}-{est[6:]}"

    return jsonify({
        'corp_code':    corp_code,
        'corp_name':    corp_d.get('corp_name', ''),
        'corp_cls':     cls_map.get(corp_d.get('corp_cls', ''), ''),
        'est_dt':       est,
        'hm_url':       corp_d.get('hm_url', ''),
        'adres':        corp_d.get('adres', ''),
        'acc_mt':       corp_d.get('acc_mt', ''),
        'phn_no':       corp_d.get('phn_no', ''),
        'ceo_nm':       corp_d.get('ceo_nm', ''),
        'shareholders': shareholders,
        'executives':   executives,
        'invest_summary': {**invest_summary, 'issues': issues},
        'disclosures':  disclosures,
        'fnguide':      fnguide,
    })



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5050, debug=True, use_reloader=False)
