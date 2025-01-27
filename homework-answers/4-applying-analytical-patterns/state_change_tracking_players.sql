with yesterday as ( 
	select * from players_growth_accouting
	where date = 1999
	
), today as (
	select 
		player_name,
		current_season as today_date,
		is_active,
		count(1)
	
	from players
	where current_season = 2000
	group by player_name, current_season, is_active
)
insert into players_growth_accouting
select 
	coalesce(t.player_name,y.player_name) as player_name,
	coalesce(y.first_active_date, t.today_date) as first_active_date,
	coalesce(t.today_date, y.first_active_date) as last_active_date,
	case 
	    when y.player_name is null then 'new'
	    when t.is_active = true and y.player_name is not null then 'continue playing'
	    when y.year_active_state = 'retired' and t.is_active = false then 'stayed retired'
	    when y.year_active_state = 'retired' and t.is_active = true then 'returned from retirement'
	    when t.is_active = false then 'retired'



	end as year_active_state,
	coalesce(y.years_active, array[]::INT[])
	|| case when 
		t.is_active is true
		then array [t.today_date]
		else array[]::INT[]
		end as years_active,
	coalesce(t.today_date, y.date + 1) as date

from today t
full outer join yesterday y
on t.player_name = y.player_name
