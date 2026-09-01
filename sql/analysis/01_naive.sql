-- 한글 주석이 깨지지 않도록 (Windows psql 기본값은 UHC)
SET client_encoding TO 'UTF8';

-- 1단계: 단순 집계 → 10.9%p (과대 추정)
--
-- "유충을 가져간 팀" vs "첫 드래곤을 가져간 팀"으로 단순 비교한다.
-- 문제: horde_kills > 0 이므로 유충을 1~2마리만 먹은 팀까지 '유충 팀'에 포함된다.
-- 유충의 가치는 3마리를 다 모을 때 나오므로, 덜 먹은 팀이 섞이면
-- 유충 쪽 승률이 낮게 깔리고 드래곤의 이득이 과대 추정된다.
--
-- 지표: 이득(%p) = 드래곤팀 승률 − 유충팀 승률 = 2 × (드래곤팀 승률 − 50)
SELECT
    COUNT(*)                                                  AS games,
    ROUND(100.0 * AVG((win_team = dragon_team)::int), 2)      AS dragon_winrate,
    ROUND(2 * (100.0 * AVG((win_team = dragon_team)::int) - 50), 2) AS effect_pp
FROM v_match_pivot
WHERE horde_any_team IS NOT NULL
  AND dragon_team    IS NOT NULL
  AND horde_any_team <> dragon_team;
