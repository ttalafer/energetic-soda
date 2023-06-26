-- DROP DATABASE energetic_soda;
-- create database energetic_soda;
-----------------------------------------------------------------
-- MODEL
-----------------------------------------------------------------
-- SCHEMAS
-----------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS doc;
CREATE SCHEMA IF NOT EXISTS cds;

-----------------------------------------------------------------
-- TABLES AND INDEXES
-----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS doc.lake(
	id						SERIAL PRIMARY KEY NOT NULL,
	id_file					TEXT NOT NULL,
	document				JSONB NOT NULL,
	timestamp_inserted		TIMESTAMPTZ NOT NULL DEFAULT(NOW() AT TIME ZONE 'UTC'),
	model					TEXT
);

CREATE INDEX IF NOT EXISTS doc_lake_document
ON doc.lake USING GIN(document);
CREATE INDEX IF NOT EXISTS doc_lake_timestamp_inserted
ON doc.lake USING BRIN(timestamp_inserted);
CREATE INDEX IF NOT EXISTS doc_lake_search_key
ON doc.lake(timestamp_inserted, model, document);

CREATE TABLE IF NOT EXISTS cds.league_model(
	id 					SERIAL PRIMARY KEY NOT NULL,
	season				TEXT NOT NULL,
	match_date			DATE,
	home_team			TEXT,
	away_team			TEXT,
	half_score_home		INT,
	half_score_away		INT,
	final_score_home	INT,
	final_score_away	INT,
	winner				TEXT GENERATED ALWAYS AS (CASE WHEN final_score_home > final_score_away THEN 'home'
								WHEN final_score_home = final_score_away THEN 'draw'
								WHEN final_score_home < final_score_away THEN 'away'
								END) STORED
);

CREATE UNIQUE INDEX IF NOT EXISTS cds_league_model_key 
ON cds.league_model(season,match_date,home_team,away_team);
CREATE INDEX IF NOT EXISTS cds_league_model_match_date
ON cds.league_model USING BRIN(match_date);

CREATE TABLE IF NOT EXISTS cds.invest_model(
	id 			SERIAL PRIMARY KEY NOT NULL,
	team		TEXT,
	profit		NUMERIC(18,2),
	investment	NUMERIC(18,2)
);

CREATE UNIQUE INDEX IF NOT EXISTS cds_invest_model_key 
ON cds.invest_model(team);

-----------------------------------------------------------------
-- PROCESSING
-----------------------------------------------------------------

CREATE OR REPLACE PROCEDURE process_data()
LANGUAGE plpgsql
AS $$
BEGIN
	--UPDATE LEAGUE MODEL
	INSERT INTO cds.league_model AS l(
		season,match_date,home_team,away_team,half_score_home,half_score_away,final_score_home,final_score_away
	)
	SELECT -- dissect the json object into the actual columns
		document ->> 'season' season,
		SUBSTRING(document ->> 'Date' FROM '\(\w+\)\s(.+)\s(\d?)')::DATE match_date,
		TRIM(SUBSTRING(document ->> 'Team 1' FROM '([\w\s]+)\s\(\d*\)')) home_team,
		TRIM(SUBSTRING(document ->> 'Team 2' FROM '([\w\s]+)\s\(\d*\)')) away_team,
		SPLIT_PART(document ->> 'HT','-',1)::INT half_score_home,
		SPLIT_PART(document ->> 'HT','-',2)::INT half_score_away,
		SPLIT_PART(document ->> 'FT','-',1)::INT final_score_home,
		SPLIT_PART(document ->> 'FT','-',2)::INT final_score_away
	FROM doc.lake WHERE model = 'LEAGUE' -- pick only league files
	AND id_file = ( -- always use latest file
		SELECT id_file 
		FROM doc.lake 
		WHERE model = 'LEAGUE' 
		ORDER BY timestamp_inserted 
		DESC limit 1
	)
	ON CONFLICT (season,match_date,home_team,away_team)
	DO UPDATE SET
		half_score_home = COALESCE(EXCLUDED.half_score_home,l.half_score_home),
		half_score_away = COALESCE(EXCLUDED.half_score_away,l.half_score_away),
		final_score_home = COALESCE(EXCLUDED.final_score_home,l.final_score_home),
		final_score_away = COALESCE(EXCLUDED.final_score_away,l.final_score_away);


	--UPDATE INVEST MODEL
	INSERT INTO cds.invest_model as i(
		team, profit, investment
	)
	SELECT -- dissect the json object into the actual columns
		TRIM(document ->> 'Team') as team,
		REPLACE(REPLACE(document ->> 'Profit per goal','$',''),',','')::NUMERIC as profit,
		REPLACE(REPLACE(document ->> 'Investment required','$',''),',','')::NUMERIC as investment
	FROM doc.lake 
	WHERE model = 'INVEST' -- pick only investment data
	AND id_file = ( -- always use latest file
		SELECT id_file 
		FROM doc.lake 
		WHERE model = 'INVEST' 
		ORDER BY timestamp_inserted 
		DESC limit 1
	)
	ON CONFLICT(team) DO UPDATE SET
		profit = COALESCE(excluded.profit,i.profit),
		investment = COALESCE(excluded.investment,i.investment);
END;
$$;

CREATE OR REPLACE FUNCTION get_season(_season TEXT)
RETURNS TABLE(
	season TEXT,
	team TEXT,
	total_points INT,
	games_played INT,
	goals_against INT,
	draws INT,
	wins INT,
	losses INT,
	goals_favor INT,
	win_ratio NUMERIC(18,2)
)
LANGUAGE plpgsql
AS $$
BEGIN
	DROP TABLE IF EXISTS _general_season;
	CREATE TEMP TABLE _general_season AS
	SELECT DISTINCT 
		l.season season,
		l.home_team team
	FROM cds.league_model l
	WHERE l.season like _season;

	DROP TABLE IF EXISTS total_score;
	CREATE TEMP TABLE total_score AS
	WITH home_score AS (
		SELECT  l.season,
				l.home_team team,
				SUM(l.final_score_home)goals_favor,
				SUM(l.final_score_away)goals_against,
				SUM(CASE WHEN l.winner = 'HOME' THEN 3 WHEN l.winner = 'DRAW' THEN 1 ELSE 0 END) points,
				SUM(CASE WHEN l.winner = 'HOME' THEN 1 ELSE 0 END) wins,
				SUM(CASE WHEN l.winner = 'DRAW' THEN 1 ELSE 0 END) draws,
				SUM(CASE WHEN l.winner = 'AWAY' THEN 1 ELSE 0 END) losses,
				COUNT(l.match_date) games_played
		FROM cds.league_model l
		GROUP BY l.season,l.home_team
	),
	away_score AS(
		SELECT  l.season,
				l.away_team team,
				SUM(l.final_score_away)goals_favor,
				SUM(l.final_score_home)goals_against,
				SUM(CASE WHEN l.winner = 'AWAY' THEN 3 WHEN l.winner = 'DRAW' THEN 1 ELSE 0 END) points,
				SUM(CASE WHEN l.winner = 'AWAY' THEN 1 ELSE 0 END) wins,
				SUM(CASE WHEN l.winner = 'DRAW' THEN 1 ELSE 0 END) draws,
				SUM(CASE WHEN l.winner = 'HOME' THEN 1 ELSE 0 END) losses,
				COUNT(l.match_date) games_played
		FROM cds.league_model l
		GROUP BY l.season,l.away_team
	)
	
	SELECT
		a.season, 
		a.team, 
		(a.goals_favor + h.goals_favor) goals_favor,
		(a.goals_against + h.goals_against) goals_against,
		(a.points + h.points) points,
		(a.wins + h.wins )wins,
		(a.draws + h.draws )draws,
		(a.losses + h.losses )losses,
		(a.games_played + h.games_played) games_played
	FROM away_score a
	JOIN home_score h ON a.season = h.season AND a.team = h.team
	ORDER BY a.season ASC, a.team;


	RETURN QUERY SELECT 
		s.season::TEXT,
		s.team::TEXT,
		t.points::INT total_points,
		t.games_played::INT,
		t.goals_against::INT,
		t.draws::INT,
		t.wins::INT,
		t.losses::INT,
		t.goals_favor::INT,
		((t.wins::FLOAT/t.games_played::FLOAT)*100.00)::NUMERIC(18,2) win_ratio
	FROM _general_season s
	JOIN total_score t ON s.team = t.team AND s.season = t.season;

END;
$$;