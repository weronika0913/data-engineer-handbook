create type scd_type as (
    quality_class quality_class,
    is_active boolean,
    start_date integer,
    end_date integer
)


with last_year_scd as(
    select * from actor_history_scd
    where current_year = 2021
    and end_date = 2021
),
historical_scd as(
    select 
        actorid,
        quality_class,
        is_active,
        start_date,
        end_date
    from actor_history_scd
    where current_year = 2021
    and end_date < 2021
),
this_year_data as (
    select * from actors
    where current_year = 2022
),
unchanged_records as(
select ty.actorid,
        ty.quality_class, 
        ty.is_active,
        ly.start_date,
        ty.current_year as end_date
from this_year_data as ty
    join last_year_scd as ly
    on ty.actorid = ly.actorid
    where ty.quality_class = ly.quality_class
    and ty.is_active = ly.is_active
),
changed_records as(
    select 
        ty.actorid,
        UNNEST(array[
            row(
                ly.quality_class,
                ly.is_active,
                ly.start_date,
                ly.end_date
                )::scd_type,
            row(
                ty.quality_class,
                ty.is_active,
                ty.current_year,
                ty.current_year
                )::scd_type
            ]) as records
    from this_year_data as ty
        left join last_year_scd as ly
        on ty.actorid = ly.actorid
        where (ty.quality_class <> ly.quality_class
        or ty.is_active <> ly.is_active)
),
unnested_changed_records as(
    select 
        actorid,
        (records::scd_type).quality_class,
        (records::scd_type).is_active,
        (records::scd_type).start_date,
        (records::scd_type).end_date
    from changed_records
),

new_records as (
    select 
        ty.actorid,
        ty.quality_class,
        ty.is_active,
        ty.current_year as start_date,
        ty.current_year as end_date
    from this_year_data as ty
    left join last_year_scd as ly
     on ty.actorid = ly.actorid
     where ly.actorid is null


)
Select *, 2022 as current_season from (
select * from historical_scd

union all

select * from unchanged_records

union all

select * from unnested_changed_records

union all

select * from new_records
) a