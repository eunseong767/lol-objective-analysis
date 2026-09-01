import os
import time
import logging
import requests
import psycopg2
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("RIOT_API_KEY")
DB_PASSWORD = os.getenv("DB_PASSWORD")
HEADERS = {"X-Riot-Token": API_KEY}

TARGETS = [
    ("MASTER", "I"),
    ("DIAMOND", "I"),
    ("EMERALD", "I"),
]
TARGET_COUNT = 450

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("collect.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)


def fetch_entries(tier, division, target):
    """페이지를 넘겨가며 target명을 채울 때까지 수집"""
    url = f"https://kr.api.riotgames.com/lol/league/v4/entries/RANKED_SOLO_5x5/{tier}/{division}"
    collected = []
    page = 1

    while len(collected) < target:
        res = requests.get(url, headers=HEADERS, params={"page": page}, timeout=15)
        res.raise_for_status()
        entries = res.json()

        if not entries:
            logging.warning(f"{tier} {division}: {page}페이지가 비어 있음 — 조기 종료")
            break

        collected.extend(entries)
        logging.info(f"{tier} {division} {page}페이지 — {len(entries)}명 (누적 {len(collected)})")
        page += 1
        time.sleep(1.3)

    return collected[:target]


def save_summoners(conn, entries):
    cur = conn.cursor()
    for e in entries:
        cur.execute(
            """
            INSERT INTO summoner (puuid, tier, league_points, wins, losses, inactive)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (puuid) DO UPDATE SET
                tier = EXCLUDED.tier,
                league_points = EXCLUDED.league_points,
                collected_at = NOW()
            """,
            (e["puuid"], e["tier"], e["leaguePoints"], e["wins"], e["losses"], e["inactive"]),
        )
    conn.commit()
    cur.close()


def collect_match_ids(conn, entries, tier):
    cur = conn.cursor()
    new_total = 0

    for i, e in enumerate(entries, 1):
        url = f"https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/{e['puuid']}/ids"
        try:
            res = requests.get(
                url, headers=HEADERS,
                params={"start": 0, "count": 20, "queue": 420},
                timeout=15,
            )
            res.raise_for_status()
            for mid in res.json():
                cur.execute(
                    """
                    INSERT INTO match_queue (match_id, source_tier)
                    VALUES (%s, %s) ON CONFLICT DO NOTHING
                    """,
                    (mid, tier),
                )
                new_total += cur.rowcount
            conn.commit()
        except Exception as ex:
            conn.rollback()
            logging.error(f"{e['puuid'][:10]} 실패 — {ex}")

        if i % 50 == 0:
            logging.info(f"{tier} {i}/{len(entries)} — 신규 {new_total}건")
        time.sleep(1.3)

    cur.close()
    return new_total


def main():
    conn = psycopg2.connect(
        dbname="lol_db", user="postgres", password=DB_PASSWORD,
        host="localhost", port=5432,
    )

    for tier, division in TARGETS:
        try:
            entries = fetch_entries(tier, division, TARGET_COUNT)
            logging.info(f"{tier} {division}: {len(entries)}명 확보")

            save_summoners(conn, entries)
            new_ids = collect_match_ids(conn, entries, tier)
            logging.info(f"{tier} 완료 — 신규 매치 {new_ids}건")

        except Exception as ex:
            conn.rollback()
            logging.error(f"{tier} 전체 실패 — {ex}")

    conn.close()


if __name__ == "__main__":
    main()