-- 한글 주석이 깨지지 않도록 (Windows psql 기본값은 UHC)
SET client_encoding TO 'UTF8';

-- 2단계: 유충 3마리 확보로 한정 (+ 15분 미만 경기 제외)
--
-- 1단계의 오염을 제거한다. horde_kills = 3 으로 조건을 정확히 정의하고,
-- 리메이크/조기 항복(15분 미만)을 빼서 초반 오브젝트가 의미를 갖는 경기만 남긴다.
-- 아직 진영은 통제하지 않았다 → 3단계에서 처리.
SELECT
    source_tier,
    COUNT(*)                                                  AS games,
    ROUND(100.0 * AVG((win_team = dragon_team)::int), 2)      AS dragon_winrate,
    ROUND(2 * (100.0 * AVG((win_team = dragon_team)::int) - 50), 2) AS effect_pp
FROM v_match_pivot
WHERE horde3_team IS NOT NULL
  AND dragon_team IS NOT NULL
  AND horde3_team <> dragon_team
  AND game_duration >= 900
GROUP BY ROLLUP (source_tier)
ORDER BY source_tier NULLS LAST;
