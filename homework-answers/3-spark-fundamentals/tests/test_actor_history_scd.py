from chispa.dataframe_comparer import *
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

from ..jobs.actor_history_scd import do_actors_history_scd
from collections import namedtuple

Actors = namedtuple("actors", "actorid actor current_year films")
ActorFilms = namedtuple("actor_films", "actorid actor film votes rating filmid year")
Actorhistoryscd = namedtuple("actors_history_scd", "actorid actor current_year films")
Films = namedtuple("films", "film votes rating filmid")


def test_when_only_new_actors(spark):
    actor_films_data = [
        ActorFilms(actorid=1, actor="actor1", film="film1", votes=100, rating=9.0, filmid=1, year=2001),
        ActorFilms(actorid=2, actor="actor2", film="film2", votes=100, rating=9.0, filmid=2, year=2001)
    ]

    expected_values = [
        Actorhistoryscd(actorid=1, actor="actor1", current_year=2001, films=[Films(film="film1", votes=100, rating=9.0, filmid=1)]),
        Actorhistoryscd(actorid=2, actor="actor2", current_year=2001, films=[Films(film="film2", votes=100, rating=9.0, filmid=2)]) ]

    actors_df = spark.createDataFrame([], "actorid INT, actor STRING, current_year INT, films ARRAY<STRUCT<film: STRING, votes: INT, rating: DOUBLE, filmid: INT>>")

    actor_films_df = spark.createDataFrame(actor_films_data, "actorid INT, actor STRING, film STRING, votes INT, rating DOUBLE, filmid INT, year INT")
    actor_history_scd_df = spark.createDataFrame([], " actorid INT, actor STRING, current_year INT, films ARRAY<STRUCT<film: STRING, votes: INT, rating: DOUBLE, filmid: INT>>")
    expected_df = spark.createDataFrame(expected_values, "actorid INT, actor STRING, current_year INT, films ARRAY<STRUCT<film: STRING, votes: INT, rating: DOUBLE, filmid: INT>>")

    actual_df = do_actors_history_scd(spark, actor_films_df, actor_history_scd_df, actors_df)

    assert_df_equality(actual_df, expected_df)