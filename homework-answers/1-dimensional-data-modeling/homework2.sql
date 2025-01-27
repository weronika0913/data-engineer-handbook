WITH yesterday AS (
    SELECT * FROM actors
    WHERE current_year = 1972
    ),
    today AS (SELECT
        actor,
        actorid,
        ARRAY_AGG(ARRAY[ROW(
            film,
            year,
            votes,
            rating,
            filmid)::films]) AS films,
        AVG(rating) AS avg_rating,
        MAX(year) AS current_year

        FROM actor_films
        WHERE year = 1973
        GROUP BY actor, actorid
    )
INSERT INTO actors
SELECT
    COALESCE(t.actor, y.actor) AS actor,
    COALESCE(t.actorid, y.actorid) AS actorid,

    CASE 
    WHEN y.films IS NULL
        THEN t.films
    WHEN y.films IS NOT NULL
    THEN y.films || t.films
    END AS films,

    CASE WHEN t.avg_rating IS NULL THEN y.quality_class
    WHEN t.avg_rating > 8 THEN 'star'
    WHEN t.avg_rating > 7 THEN 'good'
    WHEN t.avg_rating > 6 THEN 'average'
    ELSE 'bad'
    END AS quality_class,
    COALESCE(t.current_year, y.current_year + 1) AS current_year,
    CASE 
        WHEN t.current_year IS NULL 
        THEN FALSE
        ELSE TRUE
    END AS is_active

FROM today as t FULL OUTER JOIN yesterday as y
ON t.actorid = y.actorid;