import os
import time
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

HEADERS = {"X-Riot-Token": API_KEY}
MATCH_COUNT = 20


def fetch_match_ids(puuid):
    url = f"https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids"
    params = {"start": 0, "count": MATCH_COUNT, "queue": 420}
    res = requests.get(url, headers=HEADERS, params=params, timeout=10)
    res.raise_for_status()
    return res.json()


def main():
    conn = psycopg2.connect(
        dbname="lol_db", user="postgres", password=DB_PASSWORD,
        host="localhost", port=5432
    )
    cur = conn.cursor()

    cur.execute("SELECT puuid FROM summoner WHERE NOT inactive")
    puuids = [row[0] for row in cur.fetchall()]
    logging.info(f"대상 {len(puuids)}명")

    for i, puuid in enumerate(puuids, 1):
        try:
            ids = fetch_match_ids(puuid)
            for match_id in ids:
                cur.execute(
                    "INSERT INTO match_queue (match_id) VALUES (%s) ON CONFLICT DO NOTHING",
                    (match_id,)
                )
            conn.commit()

            if i % 50 == 0:
                cur.execute("SELECT COUNT(*) FROM match_queue")
                total = cur.fetchone()[0]
                logging.info(f"{i}/{len(puuids)} 진행 — 큐 누적 {total}건")

        except Exception as ex:
            conn.rollback()
            logging.error(f"{puuid[:12]}... 실패 — {ex}")

        time.sleep(1.3)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()