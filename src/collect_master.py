import os, time, logging, requests, psycopg2
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("RIOT_API_KEY")
DB_PASSWORD = os.getenv("DB_PASSWORD")
HEADERS = {"X-Riot-Token": API_KEY}
TARGET = 1000

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("collect.log", encoding="utf-8"), logging.StreamHandler()])

url = "https://kr.api.riotgames.com/lol/league/v4/masterleagues/by-queue/RANKED_SOLO_5x5"
res = requests.get(url, headers=HEADERS, timeout=15)
res.raise_for_status()
entries = res.json()["entries"]
logging.info(f"마스터 전체 {len(entries)}명 → {TARGET}명만 사용")

# LP 높은 순으로 정렬해서 상위 450명
#entries.sort(key=lambda e: e["leaguePoints"], reverse=True)
entries = entries[:TARGET]

conn = psycopg2.connect(dbname="lol_db", user="postgres", password=DB_PASSWORD,
                        host="localhost", port=5432)
cur = conn.cursor()

for e in entries:
    cur.execute("""
        INSERT INTO summoner (puuid, tier, league_points, wins, losses, inactive)
        VALUES (%s, 'MASTER', %s, %s, %s, %s)
        ON CONFLICT (puuid) DO UPDATE SET tier='MASTER', collected_at=NOW()
        """, (e["puuid"], e["leaguePoints"], e["wins"], e["losses"], e["inactive"]))
conn.commit()

new_total = 0
for i, e in enumerate(entries, 1):
    u = f"https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/{e['puuid']}/ids"
    try:
        r = requests.get(u, headers=HEADERS, params={"start": 0, "count": 20, "queue": 420}, timeout=15)
        r.raise_for_status()
        for mid in r.json():
            cur.execute("INSERT INTO match_queue (match_id, source_tier) VALUES (%s, 'MASTER') ON CONFLICT DO NOTHING", (mid,))
            new_total += cur.rowcount
        conn.commit()
    except Exception as ex:
        conn.rollback()
        logging.error(f"{e['puuid'][:10]} 실패 — {ex}")
    if i % 50 == 0:
        logging.info(f"MASTER {i}/{TARGET} — 신규 {new_total}건")
    time.sleep(1.3)

logging.info(f"MASTER 완료 — 신규 매치 {new_total}건")
cur.close()
conn.close()