"""
유충 3마리 vs 첫 드래곤 — 승률 분석

sql/analysis/*.sql 을 순서대로 실행해 추정치 사다리(1~4단계)를 재현하고,
티어 간 차이에 대한 통계 검정까지 수행한다.

    python src/analyze.py
"""
import os
import pathlib
import logging

import numpy as np
import psycopg2
from dotenv import load_dotenv
from scipy import stats, optimize

load_dotenv()
DB_PASSWORD = os.getenv("DB_PASSWORD")

SQL_DIR = pathlib.Path(__file__).resolve().parent.parent / "sql" / "analysis"

# 티어를 실력 순으로 매긴 순위 (추세 검정용). 낮을수록 상위 티어.
TIER_RANK = {"CHALLENGER_GM": 0, "MASTER": 1, "DIAMOND": 2, "EMERALD": 3}
TIER_LABEL = {
    "CHALLENGER_GM": "챌린저+그마",
    "MASTER": "마스터",
    "DIAMOND": "다이아몬드 I",
    "EMERALD": "에메랄드 I",
}

logging.basicConfig(level=logging.INFO, format="%(message)s")


def run_sql(cur, filename):
    cur.execute((SQL_DIR / filename).read_text(encoding="utf-8"))


def main():
    conn = psycopg2.connect(
        dbname="lol_db", user="postgres", password=DB_PASSWORD,
        host="localhost", port=5432,
    )
    cur = conn.cursor()

    # 분석용 뷰 생성
    run_sql(cur, "00_create_view.sql")
    conn.commit()

    # ── 수집 규모 ─────────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM match")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM match_queue WHERE status='failed'")
    failed = cur.fetchone()[0]
    print(f"\n총 {total:,}경기 수집 (실패 {failed}건)\n")

    # ── 1단계: 단순 집계 ──────────────────────────────────────
    run_sql(cur, "01_naive.sql")
    games, wr, eff1 = cur.fetchone()
    print("=" * 58)
    print(f"1단계  단순 집계 (유충 1마리 이상)      {games:>6,}경기  {eff1:>6.2f}%p")
    print("       └ 유충을 1~2마리만 먹은 팀이 섞여 과대 추정")

    # ── 2단계: 유충 3마리 한정 ────────────────────────────────
    run_sql(cur, "02_horde3.sql")
    rows2 = cur.fetchall()
    tot2 = [r for r in rows2 if r[0] is None][0]
    per_tier2 = {r[0]: r for r in rows2 if r[0] is not None}
    print(f"2단계  유충 3마리 한정 (진영 미통제)    {tot2[1]:>6,}경기  {tot2[3]:>6.2f}%p")
    print("       └ 조건을 정확히 정의하자 효과가 내려감")

    # ── 3~4단계: 진영 통제 ────────────────────────────────────
    run_sql(cur, "03_side_controlled.sql")
    rows3 = cur.fetchall()

    n_all = sum(r[1] for r in rows3)
    blue_n = sum(r[2] for r in rows3)
    red_n = sum(r[3] for r in rows3)
    print(f"3단계  + 진영 통제                      {n_all:>6,}경기")
    print(f"       └ 첫 드래곤 획득: 블루 {blue_n:,}건 vs 레드 {red_n:,}건 "
          f"(레드가 {red_n / blue_n:.1f}배)")
    print("       └ 레드가 드래곤을 자주 가져가는데 레드는 불리한 진영 →")
    print("         드래곤 팀에 진영 페널티가 얹혀 효과가 가려져 있었음")
    print("=" * 58)

    # ── 티어별 결과 ───────────────────────────────────────────
    print(f"\n{'티어':<14}{'경기수':>8}{'드래곤 이득':>13}{'블루 이득':>12}")
    print("-" * 58)
    for tier, n, _, _, _, _, drag_eff, blue_eff in rows3:
        print(f"{TIER_LABEL.get(tier, tier):<14}{n:>8,}{drag_eff:>12.2f}%p{blue_eff:>11.2f}%p")

    # ── 통계 검정 ─────────────────────────────────────────────
    run_sql(cur, "04_contingency.sql")
    cont = cur.fetchall()

    tiers = [r[0] for r in cont]
    wins = np.array([r[1] + r[3] for r in cont], float)
    tot_n = np.array([r[1] + r[2] + r[3] + r[4] for r in cont], float)
    ranks = np.array([TIER_RANK[t] for t in tiers], float)

    # 1) 티어 간 이질성 (4x2 카이제곱)
    chi2, p_chi, dof, _ = stats.chi2_contingency(
        [[r[1] + r[3], r[2] + r[4]] for r in cont]
    )

    # 2) 선형 추세 검정 (Cochran-Armitage)
    p_bar = wins.sum() / tot_n.sum()
    x_bar = np.average(ranks, weights=tot_n)
    num = (tot_n * (wins / tot_n - p_bar) * (ranks - x_bar)).sum()
    den = np.sqrt(p_bar * (1 - p_bar) * (tot_n * (ranks - x_bar) ** 2).sum())
    z = num / den
    p_trend = 2 * (1 - stats.norm.cdf(abs(z)))

    # 3) 진영 통제 로지스틱 회귀: win ~ tier_rank + is_blue
    X, Y, N = [], [], []
    for tier, bw, bl, rw, rl in cont:
        k = TIER_RANK[tier]
        X.append([1, k, 1]); Y.append(bw); N.append(bw + bl)
        X.append([1, k, 0]); Y.append(rw); N.append(rw + rl)
    X, Y, N = np.array(X, float), np.array(Y, float), np.array(N, float)

    def nll(b):
        p = np.clip(1 / (1 + np.exp(-(X @ b))), 1e-12, 1 - 1e-12)
        return -(Y * np.log(p) + (N - Y) * np.log(1 - p)).sum()

    beta = optimize.minimize(nll, np.zeros(3), method="BFGS").x
    p_hat = 1 / (1 + np.exp(-(X @ beta)))
    W = N * p_hat * (1 - p_hat)
    se = np.sqrt(np.diag(np.linalg.inv((X * W[:, None]).T @ X)))
    z_logit = beta / se
    p_logit = 2 * (1 - stats.norm.cdf(np.abs(z_logit)))

    print(f"\n{'통계 검정':<34}{'통계량':>10}{'p':>10}")
    print("-" * 58)
    print(f"{'티어 간 이질성 (4x2 카이제곱)':<30}{chi2:>12.3f}{p_chi:>10.4f}")
    print(f"{'낮은 티어일수록 이득 큼 (선형 추세)':<28}{z:>12.3f}{p_trend:>10.4f}")
    print(f"{'  └ 진영 통제 로지스틱 (티어 계수)':<28}{z_logit[1]:>12.3f}{p_logit[1]:>10.4f}")
    print(f"{'블루 진영 효과 (로지스틱)':<31}{z_logit[2]:>12.3f}{p_logit[2]:>10.4f}")

    print("\n결론")
    print("  · 모든 티어에서 첫 드래곤 쪽이 유리하다.")
    print("  · 낮은 티어일수록 이득이 커 보이지만 통계적으로 확정적이지 않다"
          f" (p={p_trend:.2f}).")
    print("  · 블루 진영 효과는 뚜렷하다 → 3단계의 진영 통제는 필수였다"
          f" (p={p_logit[2]:.4f}).")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
