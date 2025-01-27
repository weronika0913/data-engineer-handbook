create table players_growth_accouting (
	player_name text,
	first_active_date int,
	last_active_date int,
	year_active_state text,
	years_active int[],
	date integer,
	primary key (player_name, date)
);