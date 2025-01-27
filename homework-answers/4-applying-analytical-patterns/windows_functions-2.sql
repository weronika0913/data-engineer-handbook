with lebron_games AS(
select 
	player_name,
	g.game_date_est,
	pts,
	case when pts > 10 then 1 else 0 end as scored_over_10
from game_details gd
left join games g
	on gd.game_id = g.game_id
where player_name = 'LeBron James'
),
streaks as(
	select 
	game_date_est,
	row_number() over (order by game_date_est) - 
	row_number() over (partition by scored_over_10 order by game_date_est) as streak_id,
	scored_over_10
	from lebron_games
),
streak_lengths as (
	select 
	streak_id,
	count(*) as streak_length
	from streaks
	where scored_over_10 = 1
	group by streak_id
)
select 
	max(streak_length) as longest_streak
from streak_lengths
