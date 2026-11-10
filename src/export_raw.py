import os, json, pathlib
import datetime as dt
import psycopg2
from dotenv import load_dotenv

load_dotenv()
KST = dt.timezone(dt.timedelta(hours=9))
RAW_ROOT = pathlib.Path(__file__).resolve().parent.parent / "data" / "raw"

conn = psycopg2.connect(
    dbname="lol_db", user="postgres", password=os.getenv("DB_PASSWORD"),
    host="localhost", port=5432,
)

with conn.cursor() as c:
    c.execute("SELECT COUNT(*) FROM match")
    total = c.fetchone()[0]
print(f"match 테이블: {total}건")

cur = conn.cursor(name="export")   # 서버 측 커서: 한 번에 메모리에 다 안 올림
cur.itersize = 1000
cur.execute("""
    SELECT m.match_id, q.fetched_at, m.raw_json
    FROM match m
    LEFT JOIN match_queue q USING (match_id)
""")

n, skipped = 0, 0
for match_id, fetched_at, raw in cur:
    if fetched_at is None:
        skipped += 1
        continue
    if fetched_at.tzinfo is None:              # 타임존 없는 컬럼이면 UTC로 간주
        fetched_at = fetched_at.replace(tzinfo=dt.timezone.utc)
    day = fetched_at.astimezone(KST).date().isoformat()

    folder = RAW_ROOT / day
    folder.mkdir(parents=True, exist_ok=True)
    data = raw if isinstance(raw, dict) else json.loads(raw)
    (folder / f"{match_id}.json").write_text(
        json.dumps(data, ensure_ascii=False), encoding="utf-8"
    )
    n += 1
    if n % 5000 == 0:
        print(f"{n}건...")

print(f"내보냄 {n}건 / 수집시각 없어서 건너뜀 {skipped}건")
conn.close()