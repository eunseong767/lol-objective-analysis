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
BASE = "https://kr.api.riotgames.com/lol/league/v4"

TIERS = {
    "CHALLENGER": f"{BASE}/challengerleagues/by-queue/RANKED_SOLO_5x5",
    "GRANDMASTER": f"{BASE}/grandmasterleagues/by-queue/RANKED_SOLO_5x5",
}


def fetch_tier(tier_name, url):
    res = requests.get(url, headers=HEADERS, timeout=10)
    res.raise_for_status()
    return res.json()["entries"]


def save(conn, tier_name, entries):
    cur = conn.cursor()
    for e in entries:
        cur.execute(
            """
            INSERT INTO summoner (puuid, tier, league_points, wins, losses, inactive)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (puuid) DO UPDATE SET
                tier = EXCLUDED.tier,
                league_points = EXCLUDED.league_points,
                wins = EXCLUDED.wins,
                losses = EXCLUDED.losses,
                inactive = EXCLUDED.inactive,
                collected_at = NOW()
            """,
            (e["puuid"], tier_name, e["leaguePoints"],
             e["wins"], e["losses"], e["inactive"])
        )
    conn.commit()
    cur.close()


def main():
    conn = psycopg2.connect(
        dbname="lol_db", user="postgres", password=DB_PASSWORD,
        host="localhost", port=5432
    )

    for tier_name, url in TIERS.items():
        try:
            entries = fetch_tier(tier_name, url)
            save(conn, tier_name, entries)
            logging.info(f"{tier_name}: {len(entries)}명 저장")
        except Exception as ex:
            conn.rollback()
            logging.error(f"{tier_name} 실패 — {ex}")
        time.sleep(1.5)

    conn.close()


if __name__ == "__main__":
    main()