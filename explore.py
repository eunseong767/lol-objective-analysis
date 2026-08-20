import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("RIOT_API_KEY")
MATCH_ID = "KR_8335559651"

url = f"https://asia.api.riotgames.com/lol/match/v5/matches/{MATCH_ID}"
headers = {"X-Riot-Token": API_KEY}

res = requests.get(url, headers=headers)
res.raise_for_status()
data = res.json()

info = data["info"]

print("=== 경기 정보 ===")
print("queueId:", info["queueId"])
print("gameDuration:", info["gameDuration"])
print("gameVersion:", info["gameVersion"])
print("gameMode:", info.get("gameMode"))

print("\n=== 팀 정보 ===")
for team in info["teams"]:
    print(json.dumps(team, indent=2, ensure_ascii=False))