-- 한글 주석이 깨지지 않도록 (Windows psql 기본값은 UHC)
SET client_encoding TO 'UTF8';

-- 티어 간 차이 검정용 분할표 (src/analyze.py 가 이 결과를 읽어 검정한다)
-- 티어 × 진영 × 드래곤팀 승패 → 8개 셀
SELECT
    source_tier,
    COUNT(*) FILTER (WHERE dragon_team = 100 AND win_team =  dragon_team) AS blue_win,
    COUNT(*) FILTER (WHERE dragon_team = 100 AND win_team <> dragon_team) AS blue_loss,
    COUNT(*) FILTER (WHERE dragon_team = 200 AND win_team =  dragon_team) AS red_win,
    COUNT(*) FILTER (WHERE dragon_team = 200 AND win_team <> dragon_team) AS red_loss
FROM v_match_pivot
WHERE horde3_team IS NOT NULL
  AND dragon_team IS NOT NULL
  AND horde3_team <> dragon_team
  AND game_duration >= 900
GROUP BY source_tier
ORDER BY source_tier;
