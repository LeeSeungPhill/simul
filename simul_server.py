from flask import Flask, request, jsonify, send_from_directory
import psycopg2 as db
from datetime import datetime, timedelta
import os
import io
import subprocess
import requests
import json
import threading as _threading
import pandas as pd
import urllib3
from concurrent.futures import ThreadPoolExecutor
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = Flask(__name__)

CONN_STRING  = "dbname='fund_risk_mng' host='192.168.50.81' port='5432' user='postgres' password='asdf1234'"
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


# ── DART Open API ──────────────────────────────────────────────────────────
DART_API_KEY  = 'a86677be2f044d30757379f277024be9b0989823'
DART_BASE     = 'https://opendart.fss.or.kr/api'
_dart_corp_map: dict | None = None
_dart_corp_map_lock = _threading.Lock()
_dart_biz_cache: dict = {}   # corp_code → biz summary
_dart_exec_vote_cache: dict = {}  # corp_code → {name: voting_shares}

def _load_dart_corp_map() -> dict:
    """corpCode.xml 다운로드 → {stock_code: corp_code} 캐시 (최초 1회)."""
    global _dart_corp_map
    with _dart_corp_map_lock:
        if _dart_corp_map is not None:
            return _dart_corp_map
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
        m    = {}
        for item in root.findall('.//list'):
            sc = (item.findtext('stock_code') or '').strip()
            cc = (item.findtext('corp_code')  or '').strip()
            if sc and cc:
                m[sc] = cc
        with _dart_corp_map_lock:
            _dart_corp_map = m
        return m
    except Exception as e:
        print(f"[DART] corpCode 로드 오류: {e}")
        return {}

def _dart_stock_to_corp(stock_code: str) -> str | None:
    """종목코드(6자리) → DART corp_code(8자리) 변환."""
    m = _load_dart_corp_map()
    return m.get(stock_code.zfill(6))

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

def _get_is_account(rows, keyword):
    for r in rows:
        nm = (r.get('account_nm') or '').replace(' ', '')
        if keyword in nm and r.get('sj_div') == 'IS':
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

def _dart_financials_multi(corp_code):
    now = datetime.now()
    cy  = now.year
    PERIODS = [
        (f'{cy}년 1분기',   cy,   '11013'),
        (f'{cy-1}년 연간',  cy-1, '11011'),
        (f'{cy-1}년 3분기', cy-1, '11014'),
        (f'{cy-1}년 반기',  cy-1, '11012'),
    ]
    def _fetch(label, y, rc):
        rows = _dart_fin_one(corp_code, y, rc)
        if not rows: return None
        rev = _get_is_account(rows, '매출액')
        op  = _get_is_account(rows, '영업이익')
        net = _get_is_account(rows, '당기순이익')
        if not any([rev, op, net]): return None
        rev_eok = _to_eok(rev.get('thstrm_amount') if rev else None)
        op_eok  = _to_eok(op.get('thstrm_amount')  if op  else None)
        net_eok = _to_eok(net.get('thstrm_amount') if net else None)
        prev_op = _to_eok(op.get('frmtrm_amount')  if op  else None)
        return {
            'period':     label,
            'revenue':    rev_eok,
            'op_profit':  op_eok,
            'net_profit': net_eok,
            'op_margin':  round(op_eok / rev_eok * 100, 1) if rev_eok and op_eok else None,
            'op_growth':  _growth(op_eok, prev_op),
        }
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = [(lbl, ex.submit(_fetch, lbl, y, rc)) for lbl, y, rc in PERIODS]
    return [f.result() for _, f in futs if f.result()]

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
              'next_earnings': None, 'financial_highlight': [], 'peers': []}

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

    # ── Main HTML (투자의견 / 목표주가 / 예상실적 / Financial Highlight) ──
    try:
        import re as _re2
        mr = requests.get(
            f'https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode=A{code}'
            '&cID=&MenuYn=Y&ReportGB=D&NewMenuID=101&stkGb=701',
            headers=headers, timeout=15
        )
        mr.encoding = 'utf-8'
        html = mr.text

        def _tds(b):
            tds = _re2.findall(r'<td[^>]*>(.*?)</td>', b, _re2.DOTALL)
            return [_re2.sub(r'<[^>]+>', '', td).strip() for td in tds]

        # ① 컨센서스 / 목표주가
        pos9 = html.find('id="svdMainGrid9"')
        if pos9 >= 0:
            vals = _tds(html[pos9: pos9 + 800])
            if len(vals) >= 5:
                opin_num = vals[0]
                opin_label_map = {'1': '강력매도', '2': '매도', '3': '중립', '4': '매수', '5': '강력매수'}
                try:
                    ok = str(round(float(opin_num)))
                except Exception:
                    ok = ''
                result['consensus'] = {
                    'opinion': opin_num,
                    'opinion_label': opin_label_map.get(ok, ''),
                    'target_price': vals[1],
                    'eps': vals[2],
                    'per': vals[3],
                    'analyst_count': vals[4],
                }

        # ② 실적발표 예정
        pos2 = html.find('id="svdMainGrid2"')
        if pos2 >= 0:
            vals = _tds(html[pos2: pos2 + 800])
            if len(vals) >= 4:
                result['next_earnings'] = {
                    'date': vals[0], 'est_op': vals[1],
                    'vs_3m': vals[2], 'vs_ly': vals[3],
                }

        # ③ Financial Highlight (연결, 분기 4개)
        fh_pos = html.find('highlight_D_A')
        if fh_pos >= 0:
            fh_end = html.find('</table>', fh_pos)
            fh_block = html[fh_pos: fh_end + 8] if fh_end > 0 else html[fh_pos: fh_pos + 16000]

            # 기간 헤더: thead 안에서만 추출 (연 4 + 분기 4 = 8)
            thead_m = _re2.search(r'<thead>(.*?)</thead>', fh_block, _re2.DOTALL)
            periods_raw = _re2.findall(r'\d{4}/\d{2}', thead_m.group(1)) if thead_m else []
            # 중복 허용 (2025/12 가 연간 + 분기 양쪽에 나올 수 있음)
            all_periods = periods_raw[:8]  # 최대 8개

            # 현재일 기준 미래 여부
            from datetime import datetime as _dt
            today_ym = _dt.now().strftime('%Y/%m')
            def _is_est(p): return p >= today_ym

            # 행 데이터
            fin_rows = {}
            for row in _re2.finditer(
                    r'<th scope="row" class="clf"><div>([^&<][^<]*)</div></th>(.*?)</tr>',
                    fh_block, _re2.DOTALL):
                name = row.group(1).strip()
                titles = _re2.findall(r'title="([^"]*)"', row.group(2))
                vals = []
                for t in titles:
                    t = t.strip()
                    try:
                        vals.append(round(float(t.replace(',', ''))) if t else None)
                    except Exception:
                        vals.append(None)
                if name:
                    fin_rows[name] = vals

            rev_v = fin_rows.get('매출액', [])
            op_v  = fin_rows.get('영업이익', [])
            net_v = fin_rows.get('당기순이익', [])
            q_periods = all_periods[4:8] if len(all_periods) >= 8 else all_periods[len(all_periods)//2:]

            quarterly_fh = []
            for i, period in enumerate(q_periods):
                qi = 4 + i
                rev = rev_v[qi] if qi < len(rev_v) else None
                op  = op_v[qi]  if qi < len(op_v)  else None
                net = net_v[qi] if qi < len(net_v) else None
                # QoQ 증감률 (2번째 분기부터)
                op_growth = None
                if i > 0:
                    prev_op = op_v[4 + i - 1] if (4 + i - 1) < len(op_v) else None
                    if op and prev_op and prev_op != 0:
                        op_growth = round((op - prev_op) / abs(prev_op) * 100, 1)
                quarterly_fh.append({
                    'period': period,
                    'is_estimate': _is_est(period),
                    'revenue':    rev,
                    'op_profit':  op,
                    'net_profit': net,
                    'op_margin':  round(op / rev * 100, 1) if op and rev and rev > 0 else None,
                    'op_growth':  op_growth,
                })
            result['financial_highlight'] = quarterly_fh

        # ④ 동종업체 비교 (svdMainGrid10)
        pos10 = html.find('svdMainGrid10')
        if pos10 >= 0:
            peer_block = html[pos10: pos10 + 4000]
            # 종목명 링크 또는 th clf 셀에서 추출
            names = _re2.findall(r'gicode=A(\d{6})[^"]*"[^>]*>([^<\n]{1,20})</a>', peer_block)
            if not names:
                names_raw = _re2.findall(r'class="clf"[^>]*>\s*(?:<div[^>]*>)?\s*([가-힣A-Za-z0-9&·\s]{2,20}?)\s*(?:</div>)?</th>', peer_block)
                result['peers'] = [n.strip() for n in names_raw if n.strip()][:5]
            else:
                result['peers'] = [{'code': c, 'name': n.strip()} for c, n in names[:5]]

    except Exception as e:
        print(f'[FnGuide HTML] 오류: {e}')

    _fnguide_cache[code] = result
    return result


_INVEST_KW = {'실적', '영업이익', '매출', '수주', '배당', '목표주가', '전망', '투자', '계약',
              '증익', '성장', '어닝', '흑자', '적자', '공시', '호실적', '상향', '하향',
              '발표', '분기', '매출액', '순이익', '영업', '수익', '주가', '펀드',
              '업황', '업계', '시장', '산업', '수요', '공급', '동향', '경기', '점유율',
              '전망', '가이던스', '출하', '판매', '가격', '원가', '감산', '증산'}

def _naver_news(code):
    """Naver 모바일 API — 투자 관련 키워드 뉴스 우선 5건."""
    try:
        res = requests.get(
            'https://m.stock.naver.com/api/news/list',
            params={'stockCode': code, 'pageSize': 15, 'page': 1},
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
                     'Referer': 'https://m.stock.naver.com/'},
            timeout=8
        )
        if res.status_code == 200:
            items = res.json()
            if not isinstance(items, list):
                return []
            invest, others = [], []
            for item in items[:15]:
                dt = str(item.get('dt', ''))
                date_str = f"{dt[:4]}-{dt[4:6]}-{dt[6:8]}" if len(dt) >= 8 else ''
                entry = {'title': item.get('tit', ''), 'date': date_str, 'media': item.get('ohnm', '')}
                if any(kw in entry['title'] for kw in _INVEST_KW):
                    invest.append(entry)
                else:
                    others.append(entry)
            return (invest + others)[:5]
    except Exception:
        pass
    return []


@app.route('/api/dart/company-info')
def dart_company_info():
    """DART Open API 기반 기업정보(기본정보·실적·주주·경영진) + Naver 뉴스 조회."""
    code = request.args.get('code', '').strip().zfill(6)
    if not code or not code.isdigit():
        return jsonify({'error': '유효한 종목코드 필요'}), 400

    # stock_code(6) → corp_code(8) 변환
    corp_code = _dart_stock_to_corp(code)
    if not corp_code:
        return jsonify({'error': f'DART corp_code 없음 ({code})'}), 404

    # 기업 기본정보 + 병렬 데이터 수집
    cls_map = {'Y': '유가증권', 'K': '코스닥', 'N': '코넥스', 'E': '기타'}
    with ThreadPoolExecutor(max_workers=7) as ex:
        f_corp = ex.submit(_dart_req, 'company.json', {'corp_code': corp_code})
        f_news = ex.submit(_naver_news, code)
        f_fin  = ex.submit(_dart_financials_multi, corp_code)
        f_shr  = ex.submit(_dart_shareholders_latest, corp_code)
        f_exec = ex.submit(_dart_executives_latest, corp_code)
        f_biz  = ex.submit(_dart_biz_summary, corp_code)
        f_fng  = ex.submit(_fnguide_data, code)
        corp_d       = f_corp.result() or {}
        news         = f_news.result()
        financials   = f_fin.result()
        shareholders = f_shr.result()
        executives   = f_exec.result()
        biz_summary  = f_biz.result()
        fnguide      = f_fng.result()

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
        'news':         news,
        'financials':   financials,
        'shareholders': shareholders,
        'executives':   executives,
        'biz_summary':  biz_summary,
        'fnguide':      fnguide,
    })


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5050, debug=True, use_reloader=False)
