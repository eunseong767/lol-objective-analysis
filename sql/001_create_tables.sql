-- 수집 대상 소환사
CREATE TABLE summoner (
    puuid         TEXT PRIMARY KEY,
    tier          TEXT NOT NULL,
    league_points INT,
    wins          INT,
    losses        INT,
    inactive      BOOLEAN DEFAULT FALSE,
    collected_at  TIMESTAMP DEFAULT NOW()
);

-- 수집해야 할 매치 목록 (작업 큐)
CREATE TABLE match_queue (
    match_id    TEXT PRIMARY KEY,
    status      TEXT NOT NULL DEFAULT 'pending',
    retry_count INT DEFAULT 0,
    last_error  TEXT,
    fetched_at  TIMESTAMP,
    created_at  TIMESTAMP DEFAULT NOW()
);

-- 경기 원본
CREATE TABLE match (
    match_id      TEXT PRIMARY KEY,
    queue_id      INT,
    game_duration INT,
    game_version  TEXT,
    game_start    TIMESTAMP,
    raw_json      JSONB,
    collected_at  TIMESTAMP DEFAULT NOW()
);

-- 경기별 팀 정보 (경기당 2행)
CREATE TABLE match_team (
    match_id     TEXT NOT NULL REFERENCES match(match_id),
    team_id      INT  NOT NULL,
    win          BOOLEAN NOT NULL,
    horde_first  BOOLEAN,
    horde_kills  INT,
    dragon_first BOOLEAN,
    dragon_kills INT,
    baron_first  BOOLEAN,
    tower_first  BOOLEAN,
    champion_first BOOLEAN,
    PRIMARY KEY (match_id, team_id)
);

-- 큐에서 미처리 건을 빠르게 찾기 위한 인덱스
CREATE INDEX idx_queue_status ON match_queue(status);