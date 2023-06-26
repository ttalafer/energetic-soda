------------------------------------------------------------------------
-- ANALYSIS
------------------------------------------------------------------------
/*
	AFTER REVIEWING THE SCORES AND CROSS REFERENCING IT WITH THE SPONSORING DATA,
	THE FOLLOWING WAS IDENTIFIED:
		- THE MOST PROFITABLE TEAM IS THE [REAL MADRID CF] WITH THE HIGHEST NET PROFIT OF 610K
		- BOTH TEAMS HAVE WON THE LEAGUE THE SAME NUMBER OF TIMES FOR THE LAST 10 SEASONS
		- THE MOST SUCCESSFUL TEAM IN THE LAST 5 SEASONS IS [FC BARCELONA]
		- THE HIGHEST NET PROFIT FOR [FC BARCELONA] IS 560K WHITHIN THE LAST 5 SEASONS

	IN CONCLUSION, THE BEST TEAM FOR US TO INVEST IN, IS [FC BARCELONA], BECAUSE OF IT'S LAST 
	CONSISTENT STREAK OF 3 SEASONS IN THE TOP 4 WHILE KEEPING AN AVG REVENUE OF ~$500K. 
	THE CONSISTENT RESULT OF TOP 4 GIVES IT A CHANCE TO PARTICIPATE IN THE CHAMPION'S LEAGUE.

	DECISION WAS MADE BY EVALUATING THE NET PROFIT ((PROFIT PER GOAL X THE NUMBER OF GOALS IN THE SEASON) - INVESTMENT) 
	FOR ALL THE TEAMS RANKED BY POINTS WITHIN THE TOP 7 IN EACH SEASON. THIS LATTER POINT WAS DUE TO THE FACT THAT ONLY THE TOP 7 
	AT THE END OF EACH SEASON ARE ELEGIBLE TO PLAY DURING CHAMPIONS LEAGUE AND EUROCOPA LEAGUE IN THE SAME YEAR, THUS HAVING 
	A HIGHER POTENTIAL PROFIT AT THE END OF EACH YEAR, WHILE POSITIONS BETWEEN 8 AND BEYOND DON'T AND EVEN GET PUT IN A LOWER 
	CATEGORY, WHICH IMPACTS THE TOTAL YEARLY PROFIT THAT TEAM CAN EARN NEGATIVELY.

	BELOW ARE SNAPSHOTS OF THE DATA USED FOR THIS ANALYIS AND FURTHER BELOW IS THE REQUIRED CODE NEEDED TO REACH THE RESULTS
	
	team			season		season_rank	investment	total_profit	net_profit
	FC Barcelona	2008-09		1			600000.00	1050000.00		450000.00
	Real Madrid CF	2009-10		2			600000.00	1020000.00		420000.00
	Real Madrid CF	2010-11		2			600000.00	1020000.00		420000.00
	Real Madrid CF	2011-12		1			600000.00	1210000.00		610000.00
	FC Barcelona	2012-13		1			600000.00	1150000.00		550000.00
	Real Madrid CF	2013-14		2			600000.00	1040000.00		440000.00
	Real Madrid CF	2014-15		2			600000.00	1180000.00		580000.00
	FC Barcelona	2015-16		1			600000.00	1120000.00		520000.00
	FC Barcelona	2016-17		2			600000.00	1160000.00		560000.00
	FC Barcelona	2017-18		1			600000.00	990000.00		390000.00
*/

DROP TABLE IF EXISTS results;
CREATE TEMP TABLE results AS
WITH home_score AS (
	SELECT  season,
			home_team team,
			SUM(final_score_home)final_score,
			SUM(CASE WHEN winner = 'HOME' THEN 3 WHEN winner = 'DRAW' THEN 1 ELSE 0 END) points
	FROM cds.league_model
	GROUP BY season,home_team
),
away_score AS(
	SELECT  season,
			away_team team,
			SUM(final_score_away)final_score,
			SUM(CASE WHEN winner = 'AWAY' THEN 3 WHEN winner = 'DRAW' THEN 1 ELSE 0 END) points
	FROM cds.league_model
	GROUP BY season,away_team
), 
total_score AS(
	SELECT 
		row_number() 
			OVER(PARTITION BY a.season 
			ORDER BY (a.points + h.points) DESC  ) season_rank,
		a.season, 
		a.team, 
		(a.final_score + h.final_score) total_score,
		(a.points + h.points) total_points
	FROM away_score a
	JOIN home_score h ON a.season = h.season AND a.team = h.team 
	ORDER BY a.season ASC, total_score DESC
)
SELECT
	row_number() 
		OVER(PARTITION BY t.season 
		ORDER BY ((t.total_score*i.profit) - i.investment) DESC ) rn,
	i.team, 
	t.season,
	t.season_rank,
	t.total_score, 
	i.investment,
	i.profit,
	(t.total_score*i.profit) total_profit, 
	((t.total_score*i.profit) - i.investment) net_profit
FROM cds.invest_model i
JOIN total_score t ON i.team = t.team;

SELECT 
	team, 
	season, 
	season_rank,
	investment,
	total_profit, 
	net_profit
FROM (
	SELECT 
		ROW_NUMBER() 
			OVER(PARTITION BY season 
			ORDER BY INVESTMENT ASC, net_profit DESC) best_profit,
		team, 
		season, 
		season_rank,
		investment,
		total_profit, 
		net_profit
	FROM results 
	WHERE season_rank < 8
		AND rn =1
	ORDER BY INVESTMENT ASC, net_profit DESC
)x
WHERE x.best_profit = 1
ORDER BY x.season,season_rank;