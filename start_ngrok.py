#!/usr/bin/env python3
"""
ngrok 터널을 시작하고 공개 URL을 Slack으로 전송합니다.
"""
import os
import sys
import time
import json
import signal
import subprocess
import urllib.request
import urllib.error
from pathlib import Path


def _load_env(path: str = ".env") -> None:
    env_path = Path(__file__).parent / path
    if not env_path.exists():
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            os.environ.setdefault(key.strip(), val.strip())


_load_env()

# ── 설정 ──────────────────────────────────────────────────────────────────────
NGROK_AUTH_TOKEN  = os.environ.get("NGROK_AUTH_TOKEN",  "")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
FLASK_PORT        = 5050
NGROK_API         = "http://localhost:4040/api/tunnels"
# ─────────────────────────────────────────────────────────────────────────────


def get_ngrok_url(retries: int = 10, delay: float = 1.0) -> str:
    for _ in range(retries):
        try:
            with urllib.request.urlopen(NGROK_API, timeout=3) as resp:
                data = json.loads(resp.read())
                tunnels = data.get("tunnels", [])
                for t in tunnels:
                    if t.get("proto") == "https":
                        return t["public_url"]
        except (urllib.error.URLError, json.JSONDecodeError):
            pass
        time.sleep(delay)
    raise RuntimeError("ngrok 터널 URL을 가져오지 못했습니다.")


def send_slack(url: str) -> None:
    payload = json.dumps({
        "text": f":rocket: *시뮬 서버 ngrok 접속 주소*\n{url}"
    }).encode("utf-8")
    req = urllib.request.Request(
        SLACK_WEBHOOK_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        if resp.status != 200:
            print(f"Slack 전송 실패: {resp.status}")
        else:
            print(f"Slack 전송 완료: {url}")


def main():
    if not NGROK_AUTH_TOKEN:
        print("오류: .env 파일에 NGROK_AUTH_TOKEN을 설정하세요.")
        sys.exit(1)
    if not SLACK_WEBHOOK_URL:
        print("오류: .env 파일에 SLACK_WEBHOOK_URL을 설정하세요.")
        sys.exit(1)

    # auth token 등록
    subprocess.run(["ngrok", "config", "add-authtoken", NGROK_AUTH_TOKEN], check=True)

    # ngrok 터널 시작
    proc = subprocess.Popen(
        ["ngrok", "http", str(FLASK_PORT), "--log=stdout"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    def _shutdown(sig, frame):
        print("\nngrok 종료 중...")
        proc.terminate()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    print(f"ngrok 시작 중 (포트 {FLASK_PORT})...")
    try:
        public_url = get_ngrok_url()
        print(f"공개 URL: {public_url}")
        send_slack(public_url)
    except RuntimeError as e:
        print(f"오류: {e}")
        proc.terminate()
        sys.exit(1)

    print("ngrok 실행 중... 종료하려면 Ctrl+C")
    proc.wait()


if __name__ == "__main__":
    main()
