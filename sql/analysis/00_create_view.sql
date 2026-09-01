-- 한글 주석이 깨지지 않도록 (Windows psql 기본값은 UHC)
SET client_encoding TO 'UTF8';

-- 분석용 뷰: 경기당 2행(match_team)을 1행으로 피벗한다.
-- 팀 행이 2개가 아닌 경기는 제외 (수집 중단 등으로 한쪽만 저장된 건)
CREATE OR REPLACE VIEW v_match_pivot AS
SELECT
    mt.match_id,
    q.source_tier,
    m.game_duration,
    MAX(CASE WHEN mt.horde_kills > 0 THEN mt.team_id END) AS horde_any_team,   -- 유충 1마리 이상
    MAX(CASE WHEN mt.horde_kills = 3 THEN mt.team_id END) AS horde3_team,      -- 유충 정확히 3마리
    MAX(CASE WHEN mt.dragon_first    THEN mt.team_id END) AS dragon_team,      -- 첫 드래곤
    MAX(CASE WHEN mt.win             THEN mt.team_id END) AS win_team          -- 승리 팀 (100=블루, 200=레드)
FROM match_team mt
JOIN match_queue q USING (match_id)
JOIN match       m USING (match_id)
GROUP BY mt.match_id, q.source_tier, m.game_duration
HAVING COUNT(*) = 2;
