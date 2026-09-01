import os
import time
import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("RIOT_API_KEY")
HEADERS = {"X-Riot-Token": API_KEY}

TIER = "DIAMOND"
DIVISION = "I"
SAMPLE = 100

# 1) 명단 한 페이지 받기
url = f"https://kr.api.riotgames.com/lol/league/v4/entries/RANKED_SOLO_5x5/{TIER}/{DIVISION}"
res = requests.get(url, headers=HEADERS, params={"page": 1}, timeout=15)
res.raise_for_status()
entries = res.json()

print(f"1페이지 인원: {len(entries)}명")
print("샘플 필드:", list(entries[0].keys()))

# 2) 100명의 매치 ID 수집해서 중복률 확인
puuids = [e["puuid"] for e in entries[:SAMPLE]]
all_ids = []

for i, puuid in enumerate(puuids, 1):
    u = f"https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids"
    try:
        r = requests.get(u, headers=HEADERS, params={"start": 0, "count": 20, "queue": 420}, timeout=15)
        r.raise_for_status()
        all_ids.extend(r.json())
    except Exception as ex:
        print(f"{i} 실패: {ex}")
    if i % 25 == 0:
        print(f"{i}/{SAMPLE} — 누적 {len(all_ids)}건, 고유 {len(set(all_ids))}건")
    time.sleep(1.3)

unique = len(set(all_ids))
print(f"\n총 {len(all_ids)}건 수집, 고유 {unique}건")
print(f"중복률 {100 - unique/len(all_ids)*100:.1f}%")
print(f"1인당 고유 경기 {unique/SAMPLE:.1f}건")