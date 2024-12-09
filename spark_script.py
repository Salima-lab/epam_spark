from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf, when
from pyspark.sql.types import StringType
import geohash2
import requests

spark = SparkSession.builder \
    .appName("Restaurant Weather ETL") \
    .getOrCreate()


restaurant_file_path = "/Users/salima/Desktop/restaurant_csv/part-00000-c8acc470-919e-4ea9-b274-11488238c85e-c000.csv"
restaurant_df = spark.read.csv(restaurant_file_path, header=True, inferSchema=True)

weather_file_path = "/Users/salima/Desktop/weather 2/year=2016/month=10/day=01/part-00140-44bd3411-fbe4-4e16-b667-7ec0fc3ad489.c000.snappy.parquet"
weather_df = spark.read.csv(weather_file_path, header=True, inferSchema=True)

def get_lat_lng_from_opencage(city, country):
    """Fetch latitude and longitude for a city using OpenCage API."""
    api_key = "your_opencage_api_key"
    url = f"https://api.opencagedata.com/geocode/v1/json?q={city},{country}&key={api_key}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data['results']:
            return data['results'][0]['geometry']['lat'], data['results'][0]['geometry']['lng']
    return None, None

def populate_lat_lng(city, country):
    lat, lng = get_lat_lng_from_opencage(city, country)
    return f"{lat},{lng}" if lat and lng else None

populate_lat_lng_udf = udf(populate_lat_lng, StringType())

restaurant_df = restaurant_df.withColumn(
    "lat_lng",
    when(
        (col("lat").isNull()) | (col("lng").isNull()),
        populate_lat_lng_udf(col("city"), col("country"))
    ).otherwise(None)
)

def split_lat_lng(lat_lng):
    if lat_lng:
        return lat_lng.split(",")[0], lat_lng.split(",")[1]
    return None, None

split_lat_lng_udf = udf(split_lat_lng, StringType())

restaurant_df = restaurant_df.withColumn("lat", when(col("lat").isNull(), split_lat_lng_udf(col("lat_lng"))[0]).otherwise(col("lat")))
restaurant_df = restaurant_df.withColumn("lng", when(col("lng").isNull(), split_lat_lng_udf(col("lat_lng"))[1]).otherwise(col("lng")))


def generate_geohash(lat, lng):
    return geohash2.encode(float(lat), float(lng), precision=4)

generate_geohash_udf = udf(generate_geohash, StringType())

restaurant_df = restaurant_df.withColumn("geohash", generate_geohash_udf(col("lat"), col("lng")))


weather_df = weather_df.withColumn("geohash", generate_geohash_udf(col("lat"), col("lng")))


enriched_df = restaurant_df.join(weather_df, on="geohash", how="left")


output_path = "/Users/salima/Desktop/output/output.csv"
enriched_df.write.mode("overwrite").partitionBy("country").parquet(output_path)


spark.stop()

print("completed successfully!")
