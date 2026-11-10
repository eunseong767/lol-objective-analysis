import os
import time
import json
import logging
import requests
import psycopg2
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("RIOT_API_KEY")
DB_PASSWORD = os.getenv("DB_PASSWORD")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("collect.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
import pathlib
import datetime as dt

KST = dt.timezone(dt.timedelta(hours=9))
RAW_ROOT = pathlib.Path(__file__).resolve().parent.parent / "data" / "raw"


def save_raw(match_id, data):
    today = dt.datetime.now(KST).date().isoformat()
    folder = RAW_ROOT / today
    folder.mkdir(parents=True, exist_ok=True)
    (folder / f"{match_id}.json").write_text(
        json.dumps(data, ensure_ascii=False), encoding="utf-8"
    )
HEADERS = {"X-Riot-Token": API_KEY}
SLEEP = 1.0
BATCH = 500


class KeyExpired(Exception):
    pass


def fetch_match(match_id):
    url = f"https://asia.api.riotgames.com/lol/match/v5/matches/{match_id}"

    for attempt in range(3):
        res = requests.get(url, headers=HEADERS, timeout=15)

        if res.status_code == 200:
            return res.json()

        if res.status_code == 429:
            wait = int(res.headers.get("Retry-After", 10))
            logging.warning(f"429 — {wait}초 대기")
            time.sleep(wait)
            continue

        if res.status_code in (401, 403):
            raise KeyExpired(f"인증 실패 {res.status_code} — 키 확인 필요")

        res.raise_for_status()

    raise Exception("429 3회 재시도 실패")


def save_match(cur, match_id, data):
    info = data["info"]

    cur.execute(
        """
        INSERT INTO match (match_id, queue_id, game_duration, game_version, game_start, raw_json)
        VALUES (%s, %s, %s, %s, to_timestamp(%s), %s)
        ON CONFLICT (match_id) DO NOTHING
        """,
        (match_id, info["queueId"], info["gameDuration"], info["gameVersion"],
         info["gameStartTimestamp"] / 1000, json.dumps(data))
    )

    for team in info["teams"]:
        obj = team["objectives"]
        cur.execute(
            """
            INSERT INTO match_team
                (match_id, team_id, win, horde_first, horde_kills,
                 dragon_first, dragon_kills, baron_first, tower_first, champion_first)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (match_id, team_id) DO NOTHING
            """,
            (match_id, team["teamId"], team["win"],
             obj["horde"]["first"], obj["horde"]["kills"],
             obj["dragon"]["first"], obj["dragon"]["kills"],
             obj["baron"]["first"], obj["tower"]["first"], obj["champion"]["first"])
        )


def main():
    conn = psycopg2.connect(
        dbname="lol_db", user="postgres", password=DB_PASSWORD,
        host="localhost", port=5432
    )
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM match_queue WHERE status = 'pending'")
    remaining = cur.fetchone()[0]
    logging.info(f"남은 작업 {remaining}건 — 예상 {remaining * 2.0 / 3600:.1f}시간")

    done = 0
    while True:
        cur.execute(
            "SELECT match_id FROM match_queue WHERE status = 'pending' LIMIT %s",
            (BATCH,)
        )
        rows = cur.fetchall()
        if not rows:
            break

        for (match_id,) in rows:
            try:
                data = fetch_match(match_id)
                save_raw(match_id, data) 
                save_match(cur, match_id, data)
                cur.execute(
                    "UPDATE match_queue SET status='done', fetched_at=NOW() WHERE match_id=%s",
                    (match_id,)
                )
                conn.commit()
                done += 1

            except KeyExpired as ex:
                conn.rollback()
                logging.error(str(ex))
                logging.error(f"중단 — {done}건 처리 후 종료")
                cur.close()
                conn.close()
                return

            except Exception as ex:
                conn.rollback()
                cur.execute(
                    """
                    UPDATE match_queue
                    SET status='failed', retry_count=retry_count+1, last_error=%s
                    WHERE match_id=%s
                    """,
                    (str(ex)[:200], match_id)
                )
                conn.commit()
                logging.error(f"{match_id} 실패 — {ex}")

            if done % 100 == 0 and done > 0:
                logging.info(f"{done}/{remaining} 완료")

            time.sleep(SLEEP)

    logging.info(f"수집 종료 — 총 {done}건")
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()