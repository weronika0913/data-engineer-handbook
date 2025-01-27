insert into actor_history_scd
with with_previous as (
select 
    actorid,
    current_year,
    quality_class,
    is_active,
    LAG(quality_class,1) over (partition by actorid order by current_year) as previous_quality_class_scd,
    LAG(is_active,1) over (partition by actorid order by current_year) as previous_is_active_scd
from actors
 where current_year <= 2021
),
    with_indicators as(
select *, 
    case 
        when quality_class <> previous_quality_class_scd then 1
        when is_active <> previous_is_active_scd then 1
        else 0 
        end as change_indicator
    from with_previous
),
    with_streaks as(

select *,
    sum(change_indicator) over (partition by actorid order by current_year) as streak_identifier
from with_indicators
),

aggregated as (
    select  
    actorid,
    streak_identifier,
    is_active,
    quality_class,
    min(current_year) as start_year,
    max(current_year) as end_year,
    2021 as current_year
from with_streaks
group by actorid,streak_identifier,is_active,quality_class
order by actorid
)

select actorid, quality_class, is_active, start_year, end_year, current_year
from aggregated

