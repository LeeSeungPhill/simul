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
                # buy_date 전영업일 기준 market_ratio 조회
                business_day = f"{buy_date[:4]}-{buy_date[4:6]}-{buy_date[6:]}"
                prev_biz = get_previous_business_day(
                    (datetime.strptime(business_day, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
                )
                cur_mr.execute("""
                    SELECT market_ratio, dt FROM dly_acct_balance_simul
                    WHERE acct = '74346047' AND dt = %s
                """, (prev_biz,))
            else:
                # buy_date 미입력 시 최신 레코드
                cur_mr.execute("""
                    SELECT market_ratio, dt FROM dly_acct_balance_simul
                    WHERE acct = '74346047' AND market_ratio > 0
                    ORDER BY dt DESC LIMIT 1
                """)

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


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5050, debug=True, use_reloader=False)
