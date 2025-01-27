from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
import os
import shutil
from pyspark.sql import functions as F

def broadcast_join_maps(spark):
    #create maps and matches dataframes
    matches_df = spark.read.csv("../../data/matches.csv", header=True, inferSchema=True)
    maps_df = spark.read.csv("../../data/maps.csv", header=True, inferSchema=True)
    
    #broadcast join maps_df with matches_df
    matches_maps_df = matches_df.join(broadcast(maps_df), "mapid")

    return matches_maps_df

def broadcast_join_medals(spark):
    #create medals and medal matches players dataframes
    medals_df = spark.read.csv("../../data/medals.csv", header=True, inferSchema=True)
    medals_matches_players_df = spark.read.csv("../../data/medals_matches_players.csv", header=True, inferSchema=True)

    #broadacast join medals_df with medal_matches_players_df
    medals_players_df = medals_matches_players_df.join(broadcast(medals_df), "medal_id")

    return medals_players_df

def bucket_join_matches(spark):

    num_buckets = 16
    table_path = "./spark-warehouse/matches_bucketed"

    #delete tables if it exists
    if os.path.exists(table_path):
        shutil.rmtree(table_path)

    #create matches dataframe
    matches_df = spark.read.csv("../../data/matches.csv", header=True, inferSchema=True)

    #repartition and sort within partitions
    matches_df = matches_df.repartitionByRange(num_buckets, "match_id").sortWithinPartitions("match_id")
    matches_df.write.format("parquet").bucketBy(num_buckets, "match_id").mode("overwrite").saveAsTable("matches_bucketed")

    #read bucketed table
    matches_bucketed_df = spark.read.table("matches_bucketed")
    return matches_bucketed_df


def bucket_join_match_details(spark):
    num_buckets = 16
    table_path = "./spark-warehouse/match_details_bucketed"

    #create match_details dataframe
    match_details_df = spark.read.csv("../../data/match_details.csv", header=True, inferSchema=True)
    
    #delete tables if it exist:
    if os.path.exists(table_path):
        shutil.rmtree(table_path)

    #repartition and sort within partitions
    match_details_df.repartitionByRange(num_buckets,"match_id").sortWithinPartitions("match_id")
    match_details_df.write.format("parquet").bucketBy(num_buckets,"match_id").mode("overwrite").saveAsTable("match_details_bucketed")

    #read bucketed table
    matches_details_df = spark.read.table("match_details_bucketed")

    return matches_details_df


def bucket_join_medals_matches_players(spark):
    num_buckets = 16
    table_path = "./spark-warehouse/medals_matches_players_bucketed"

    #create medal_matches_players dataframe
    medals_matches_players_df = spark.read.csv("../../data/medals_matches_players.csv", header=True, inferSchema=True)
    
    #delete tables if it exist:
    if os.path.exists(table_path):
        shutil.rmtree(table_path)

    #repartition and sort within partitions
    medals_matches_players_df.repartitionByRange(num_buckets,"match_id").sortWithinPartitions("match_id")
    medals_matches_players_df.write.format("parquet").bucketBy(num_buckets,"match_id").mode("overwrite").saveAsTable("medals_matches_players_bucketed")

    #read bucketed table
    matches_bucketed_df = spark.read.table("medals_matches_players_bucketed")
    return matches_bucketed_df


def bukcet_join_tables(matches_df, match_details_df, medals_matches_players_df):
    #join tables
    joined_df = matches_df.join(match_details_df, "match_id").join(medals_matches_players_df, "match_id")

    return joined_df

def get_aggregated_stats(spark, df):
    
    # Which player averages the most kills per game?
    df.groupBy("match_id", "player_gamertag").avg("player_total_kills").alias("average_kills_per_game").show()

    #Which playlist gets played the most?
    most_common_playlist = df.groupBy("playlist_id").count().orderBy(F.desc("count")).take(1)
    print(f"Most common playlist is {most_common_playlist}")

    #Which map gets played the most?
    most_common_map = df.groupBy("mapid").count().orderBy("count", ascending=False).take(1)
    print(f"Most common map is {most_common_map}")

    #Which map do players get the most Killing Spree medals on?
    medals_df = spark.read.csv("../../data/medals.csv", header=True, inferSchema=True)
    df = df.join(broadcast(medals_df), "medal_id")
    killing_spree_df = df.filter(df["classification"] == "KillingSpree").groupBy("mapid").count().orderBy("count", ascending=False).take(1)
    print(f"Map with most Killing Spree medals is {killing_spree_df}")

def bucket_join_matches_v2(spark):
    num_buckets = 16

   #create matches dataframe
    matches_df = spark.read.csv("../../data/matches.csv", header=True, inferSchema=True)

    #repartition and sort within partitions
    matches_df = matches_df.repartitionByRange(num_buckets, "match_id").sortWithinPartitions("match_id")
    matches_df.write.format("parquet").bucketBy(num_buckets, "match_id").mode("overwrite").saveAsTable("matches_bucketed_v2")

    #read bucketed table
    matches_bucketed_df = spark.read.table("matches_bucketed_v2")
    return matches_bucketed_df

def bucket_join_matches_v3(spark):
    num_buckets = 16

   #create matches dataframe
    matches_df = spark.read.csv("../../data/matches.csv", header=True, inferSchema=True)

    #repartition and sort within partitions
    matches_df = matches_df.repartitionByRange(num_buckets, "match_id").sortWithinPartitions("match_id", "playlist_id")
    matches_df.write.format("parquet").bucketBy(num_buckets, "match_id").mode("overwrite").saveAsTable("matches_bucketed_v3")

    #read bucketed table
    matches_bucketed_df = spark.read.table("matches_bucketed_v3")
    return matches_bucketed_df

def bucket_join_matches_v4(spark):
    num_buckets = 16

   #create matches dataframe
    matches_df = spark.read.csv("../../data/matches.csv", header=True, inferSchema=True)

    #repartition and sort within partitions
    matches_df = matches_df.repartitionByRange(num_buckets, "match_id").sortWithinPartitions("match_id", "playlist_id", "mapid")
    matches_df.write.format("parquet").bucketBy(num_buckets, "match_id").mode("overwrite").saveAsTable("matches_bucketed_v4")

    #read bucketed table
    matches_bucketed_df = spark.read.table("matches_bucketed_v4")
    return matches_bucketed_df

def compare_file_sizes():
    #compare the files sizes of the three bucketed dataframes
    tables = ["matches_bucketed", "matches_bucketed_v2", "matches_bucketed_v3", "matches_bucketed_v4"]

    for table in tables:
        file_path = 'spark-warehouse/' + table
        file_sizes = []
        if os.path.exists(file_path):
            for file in os.listdir(file_path):
                file_sizes.append(os.path.getsize(file_path + "/" + file))
                print(f"Table: {table}, Files sizes: {sorted(file_sizes)}")

def verify_table_correctness(spark):
    tables = ["matches_bucketed", "match_details_bucketed", "medals_matches_players_bucketed"]
    for table in tables:
        df = spark.table(table)
        print(f"Partitions for table {table}: {df.rdd.getNumPartitions()}")
        print(f"Table: {table}, Number of rows: {df.count()}")


def main():
    #create a sparksession
    spark = SparkSession.builder.appName("Homework").getOrCreate()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    #broadcast join maps and matches datraframes
    broadcast_join_maps(spark)
    broadcast_join_medals(spark)

    #bucket join matches, match_details and medals_matches_players dataframes
    bucketed_matches = bucket_join_matches(spark)
    bucketed_match_details= bucket_join_match_details(spark)
    bucketed_medals_matches_players = bucket_join_medals_matches_players(spark)

    #aggregated tables to answer questions
    aggregated_tables = bukcet_join_tables(bucketed_matches, bucketed_match_details, bucketed_medals_matches_players)
    get_aggregated_stats(spark, aggregated_tables)

    # bucket join matches dataframe v2
    bucketed_matches_v2 = bucket_join_matches_v2(spark)
    bucketed_df_v2 = bukcet_join_tables(bucketed_matches_v2, bucketed_match_details, bucketed_medals_matches_players)
    get_aggregated_stats(spark, bucketed_df_v2)

    # bucket join matches dataframe v3
    bucketed_matches_v3 = bucket_join_matches_v3(spark)
    bucketed_df_v3 = bukcet_join_tables(bucketed_matches_v3, bucketed_match_details, bucketed_medals_matches_players)
    get_aggregated_stats(spark, bucketed_df_v3)

    # bucket join matches dataframe v4
    bucketed_matches_v4 = bucket_join_matches_v4(spark)
    bucketed_df_v4 = bukcet_join_tables(bucketed_matches_v4, bucketed_match_details, bucketed_medals_matches_players)
    get_aggregated_stats(spark, bucketed_df_v4)

    #compare file sizes of the bucketed dataframes
    compare_file_sizes()

    #verify table correctness
    verify_table_correctness(spark)
    

if __name__ == "__main__":
    main()


