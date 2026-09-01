-- 한글 주석이 깨지지 않도록 (Windows psql 기본값은 UHC)
SET client_encoding TO 'UTF8';

-- 3~4단계: 진영(블루/레드) 통제
--
-- 핵심 교란 변수: 레드 진영이 첫 드래곤을 가져가는 비율이 훨씬 높은데
-- (아래 n_blue_dragon vs n_red_dragon 참고), 진영 효과가 드래곤 효과에 섞여 있었다.
-- 드래곤 팀에 진영 페널티가 얹혀 있어서 드래곤의 진짜 효과가 가려져 있던 것.
--
-- 통제 방법: 드래곤 팀이 블루인 경기와 레드인 경기로 층화한 뒤 두 층의 효과를 평균한다.
--   드래곤 이득 = (pB − 50) + (pR − 50) = pB + pR − 100
--   블루 이득   = (pB − 50) − (pR − 50) = pB − pR
-- 여기서 pB = 드래곤팀이 블루일 때의 드래곤팀 승률, pR = 레드일 때의 승률.
SELECT
    source_tier,
    COUNT(*)                                            AS games,
    COUNT(*) FILTER (WHERE dragon_team = 100)           AS n_blue_dragon,
    COUNT(*) FILTER (WHERE dragon_team = 200)           AS n_red_dragon,
    ROUND(100.0 * AVG((win_team = dragon_team)::int) FILTER (WHERE dragon_team = 100), 2) AS pB,
    ROUND(100.0 * AVG((win_team = dragon_team)::int) FILTER (WHERE dragon_team = 200), 2) AS pR,
    ROUND(  100.0 * AVG((win_team = dragon_team)::int) FILTER (WHERE dragon_team = 100)
          + 100.0 * AVG((win_team = dragon_team)::int) FILTER (WHERE dragon_team = 200) - 100, 2) AS dragon_effect_pp,
    ROUND(  100.0 * AVG((win_team = dragon_team)::int) FILTER (WHERE dragon_team = 100)
          - 100.0 * AVG((win_team = dragon_team)::int) FILTER (WHERE dragon_team = 200), 2) AS blue_effect_pp
FROM v_match_pivot
WHERE horde3_team IS NOT NULL
  AND dragon_team IS NOT NULL
  AND horde3_team <> dragon_team
  AND game_duration >= 900
GROUP BY source_tier
ORDER BY dragon_effect_pp;
