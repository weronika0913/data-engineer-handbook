create table actor_history_scd (
    actorid text,
    quality_class quality_class,
    is_active boolean,
    start_date integer,
    end_date integer,
    current_year integer,
    primary key(actorid, start_date)
)

