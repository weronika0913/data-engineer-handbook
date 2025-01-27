select 
	case
		when grouping(player_name) = 0 
			and grouping(t.nickname) = 0
			then 'player_name__team_name'
		when grouping(player_name) = 0
			and grouping(season) = 0
			then 'player_name__season'
		when grouping(t.nickname) = 0
			then 'team_name'
	end as aggregation_level,		
	coalesce(player_name,  'overall') as player_name,
	coalesce(t.nickname, 'overall') as team_name,
	g.season,
	sum(coalesce(pts,0)) as total_points,
	count(g.home_team_wins) as total_wins
		
from game_details gd
join teams t
	on t.team_id = gd.team_id
join games g
	on gd.game_id = g.game_id
group by grouping sets (
	(player_name, t.nickname),
	(player_name, g.season),
	(t.nickname)
	)

--who scored the most points playing for one team?
with grouping_set as (
select 
	case
		when grouping(player_name) = 0 
			and grouping(t.nickname) = 0
			then 'player_name__team_name'
		when grouping(player_name) = 0
			and grouping(season) = 0
			then 'player_name__season'
		when grouping(t.nickname) = 0
			then 'team_name'
	end as aggregation_level,		
	coalesce(player_name,  'overall') as player_name,
	coalesce(t.nickname, 'overall') as team_name,
	g.season,
	sum(coalesce(pts,0)) as total_points,
	count(g.home_team_wins) as total_wins
from game_details gd
join teams t
	on t.team_id = gd.team_id
join games g
	on gd.game_id = g.game_id
group by grouping sets (
	(player_name, t.nickname),
	(player_name, g.season),
	(t.nickname)
	)
)
select *
from grouping_set
where aggregation_level = 'player_name__team_name'
order by total_points desc
limit 1


-- who scored the most points in one season?
with grouping_set as (
select 
	case
		when grouping(player_name) = 0 
			and grouping(t.nickname) = 0
			then 'player_name__team_name'
		when grouping(player_name) = 0
			and grouping(season) = 0
			then 'player_name__season'
		when grouping(t.nickname) = 0
			then 'team_name'
	end as aggregation_level,		
	coalesce(player_name,  'overall') as player_name,
	coalesce(t.nickname, 'overall') as team_name,
	g.season,
	sum(coalesce(pts,0)) as total_points,
	count(g.home_team_wins) as total_wins
from game_details gd
join teams t
	on t.team_id = gd.team_id
join games g
	on gd.game_id = g.game_id
group by grouping sets (
	(player_name, t.nickname),
	(player_name, g.season),
	(t.nickname)
	)
)
select *
from grouping_set
where aggregation_level = 'player_name__season'
order by total_points desc
limit 1

--which team has won the most games?
with grouping_set as (
select 
	case
		when grouping(player_name) = 0 
			and grouping(t.nickname) = 0
			then 'player_name__team_name'
		when grouping(player_name) = 0
			and grouping(season) = 0
			then 'player_name__season'
		when grouping(t.nickname) = 0
			then 'team_name'
	end as aggregation_level,		
	coalesce(player_name,  'overall') as player_name,
	coalesce(t.nickname, 'overall') as team_name,
	g.season,
	sum(coalesce(pts,0)) as total_points,
	count(g.home_team_wins) as total_wins
from game_details gd
join teams t
	on t.team_id = gd.team_id
join games g
	on gd.game_id = g.game_id
group by grouping sets (
	(player_name, t.nickname),
	(player_name, g.season),
	(t.nickname)
	)
)
select *
from grouping_set
where aggregation_level = 'team_name'
order by total_points desc
limit 1