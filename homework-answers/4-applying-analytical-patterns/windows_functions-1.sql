WITH all_games AS (
	SELECT 
		game_date_est,
		game_id,
		home_team_id AS team_id,
		home_team_wins AS win
	FROM games
	UNION ALL
	SELECT 
		game_date_est,
		game_id,
		visitor_team_id AS team_id,
		1  - home_team_wins AS win
		FROM games
),
rolling_wins AS (
	SELECT 
		team_id,
		game_date_est,
		SUM(win) OVER (PARTITION BY team_id ORDER BY game_date_est ROWS 89 PRECEDING) AS wins_in_90_games
	FROM all_games
)
SELECT
	team_id,
	MAX(wins_in_90_games)
FROM rolling_wins
GROUP BY team_id
ORDER BY max(wins_in_90_games) DESC
LIMIT 1
		
	