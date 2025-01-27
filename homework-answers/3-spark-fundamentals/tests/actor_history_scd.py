from pyspark.sql import SparkSession





def do_actors_history_scd(spark, actors_df, actor_films_df, actor_history_scd_df):
    query = f"""
WITH yesterday AS (
    SELECT * FROM actors
    WHERE current_year = 2000
), today AS (
    SELECT * FROM actor_films
    WHERE year = 2001
), ratings AS (
    SELECT actorid, AVG(rating)
    FROM today
    GROUP BY actorid
)
SELECT 
    COALESCE(y.actorid, t.actorid) AS actorid,
    COALESCE(y.actor, t.actor) AS actor,
    COALESCE(y.current_year + 1, t.year) AS current_year,
    CASE
        WHEN t.film IS NULL THEN y.films
        WHEN y.films IS NULL THEN array(struct(t.film, t.votes, t.rating, t.filmid))
        ELSE concat(y.films, array(struct(t.film, t.votes, t.rating, t.filmid)))
    END AS films
    FROM today as t
    FULL OUTER JOIN yesterday as y
    ON y.actorid = t.actorid
    """
    actors_df.createOrReplaceTempView("actors")
    actor_films_df.createOrReplaceTempView("actor_films")
    actor_history_scd_df.createOrReplaceTempView("actor_history_scd")
    return spark.sql(query)


def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("actors_history_scd") \
      .getOrCreate()
    output_df = do_actors_history_scd(spark, spark.table("actors"), spark.table("actor_films"), spark.table("actors_history_scd"))
    output_df.write.mode("overwrite").insertInto("actors_history_scd")

if __name__ == "__main__":
    main()