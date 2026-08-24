import os
import time
import json
import logging
from datetime import datetime, timedelta

import requests
import psycopg2

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

API_KEY = os.getenv("RIOT_API_KEY")
DB_PASSWORD = os.getenv("DB_PASSWORD")
HEADERS = {"X-Riot-Token": API_KEY}

log = logging.getLogger(__name__)


def get_conn():
    return psycopg2.connect(
        dbname="lol_db",
        user="postgres",
        password=DB_PASSWORD,
        host="host.docker.internal",
        port=5432,
    )


def collect_summoners():
    base = "https://kr.api.riotgames.com/lol/league/v4"
    tiers = {
        "CHALLENGER": f"{base}/challengerleagues/by-queue/RANKED_SOLO_5x5",
        "GRANDMASTER": f"{base}/grandmasterleagues/by-queue/RANKED_SOLO_5x5",
    }

    conn = get_conn()
    cur = conn.cursor()
    total = 0

    for tier, url in tiers.items():
        res = requests.get(url, headers=HEADERS, timeout=15)
        res.raise_for_status()
        entries = res.json()["entries"]

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
                (e["puuid"], tier, e["leaguePoints"], e["wins"], e["losses"], e["inactive"]),
            )
        conn.commit()
        total += len(entries)
        log.info(f"{tier}: {len(entries)}명")
        time.sleep(1.5)

    cur.close()
    conn.close()
    return total


def collect_match_ids():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT puuid FROM summoner WHERE NOT inactive LIMIT 100")
    puuids = [r[0] for r in cur.fetchall()]

    new_count = 0
    for puuid in puuids:
        url = f"https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids"
        params = {"start": 0, "count": 20, "queue": 420}
        try:
            res = requests.get(url, headers=HEADERS, params=params, timeout=15)
            res.raise_for_status()
            for mid in res.json():
                cur.execute(
                    "INSERT INTO match_queue (match_id) VALUES (%s) ON CONFLICT DO NOTHING",
                    (mid,),
                )
                new_count += cur.rowcount
            conn.commit()
        except Exception as ex:
            conn.rollback()
            log.error(f"{puuid[:10]} 실패 — {ex}")
        time.sleep(1.3)

    cur.close()
    conn.close()
    log.info(f"신규 매치 {new_count}건")
    return new_count


def collect_matches():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT match_id FROM match_queue WHERE status='pending' LIMIT 300")
    rows = cur.fetchall()
    done = 0

    for (match_id,) in rows:
        url = f"https://asia.api.riotgames.com/lol/match/v5/matches/{match_id}"
        try:
            res = requests.get(url, headers=HEADERS, timeout=15)
            if res.status_code == 429:
                wait = int(res.headers.get("Retry-After", 10))
                time.sleep(wait)
                res = requests.get(url, headers=HEADERS, timeout=15)
            res.raise_for_status()
            data = res.json()
            info = data["info"]

            cur.execute(
                """
                INSERT INTO match (match_id, queue_id, game_duration, game_version, game_start, raw_json)
                VALUES (%s, %s, %s, %s, to_timestamp(%s), %s)
                ON CONFLICT (match_id) DO NOTHING
                """,
                (match_id, info["queueId"], info["gameDuration"], info["gameVersion"],
                 info["gameStartTimestamp"] / 1000, json.dumps(data)),
            )

            for team in info["teams"]:
                o = team["objectives"]
                cur.execute(
                    """
                    INSERT INTO match_team
                        (match_id, team_id, win, horde_first, horde_kills,
                         dragon_first, dragon_kills, baron_first, tower_first, champion_first)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (match_id, team_id) DO NOTHING
                    """,
                    (match_id, team["teamId"], team["win"],
                     o["horde"]["first"], o["horde"]["kills"],
                     o["dragon"]["first"], o["dragon"]["kills"],
                     o["baron"]["first"], o["tower"]["first"], o["champion"]["first"]),
                )

            cur.execute(
                "UPDATE match_queue SET status='done', fetched_at=NOW() WHERE match_id=%s",
                (match_id,),
            )
            conn.commit()
            done += 1

        except Exception as ex:
            conn.rollback()
            cur.execute(
                "UPDATE match_queue SET status='failed', retry_count=retry_count+1, last_error=%s WHERE match_id=%s",
                (str(ex)[:200], match_id),
            )
            conn.commit()
            log.error(f"{match_id} 실패 — {ex}")

        time.sleep(1.0)

    cur.close()
    conn.close()
    log.info(f"{done}건 수집")
    return done


def validate():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM match_team GROUP BY match_id HAVING COUNT(*) <> 2")
    bad = cur.fetchall()
    if bad:
        raise ValueError(f"팀 수가 2가 아닌 경기 {len(bad)}건")

    cur.execute("SELECT COUNT(*) FROM match")
    total = cur.fetchone()[0]
    log.info(f"검증 통과 — 총 {total}경기")

    cur.close()
    conn.close()


with DAG(
    dag_id="lol_daily_pipeline",
    start_date=datetime(2026, 8, 20),
    schedule="0 4 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["lol"],
) as dag:

    t1 = PythonOperator(task_id="collect_summoners", python_callable=collect_summoners)
    t2 = PythonOperator(task_id="collect_match_ids", python_callable=collect_match_ids)
    t3 = PythonOperator(task_id="collect_matches", python_callable=collect_matches)
    t4 = PythonOperator(task_id="validate", python_callable=validate)

    t1 >> t2 >> t3 >> t4