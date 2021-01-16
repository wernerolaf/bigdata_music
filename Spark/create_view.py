import pyspark
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.sql import functions as F

if __name__ == "__main__":
    conf = SparkConf().setAppName("DV")
    sc = SparkContext(conf=conf)
    sqlsc = HiveContext(sc)

    df_w_a = sqlsc.read.format('json').load('/user/bigdata_music/wikidata/')
    df_w_a = df_w_a.dropDuplicates()

    df_s_a = sqlsc.read.format('json').load('/user/bigdata_music/spotify/*')
    df_s_a = df_s_a.dropDuplicates()

    df_s_a_subset = df_s_a.drop('external_urls').drop('href').drop('images').drop('type').drop('uri')
    df_s_a_subset = df_s_a_subset.dropDuplicates()

    df_s_a_subset = df_s_a_subset.withColumn("followers", df_s_a_subset["followers"].getItem('total'))
    df_s_a_subset = df_s_a_subset.withColumnRenamed("genres", "spotify_genres")

    df_w_a_flat = df_w_a.groupBy('id').agg(F.collect_set(df_w_a.genre).alias('wikidata_genres'))
    df_w_a_instruments = df_w_a.groupBy('id').agg(F.collect_set(df_w_a.instrument).alias('instruments'))

    df_w_a_gr = df_w_a.drop('instrument').drop('genre').drop('pseudonym').distinct()
    df_w_a_flat = df_w_a_flat.join(df_w_a_gr, 'id', 'left')
    df_w_a_flat = df_w_a_flat.join(df_w_a_instruments, 'id', 'left')

    df_f = df_w_a_flat.join(df_s_a_subset, 'id', 'right')

    df_f.write.mode('overwrite').parquet('/user/bigdata_music/spark_etl_view/view.parquet')