import dlt
from pyspark.sql import functions as F, types as T, Window

BOOTSTRAP = spark.conf.get("kafka.bootstrap")
SASL_USER = spark.conf.get("kafka.username")
SASL_PASS = spark.conf.get("kafka.password")
TOPIC     = spark.conf.get("kafka.topic", "carpark.raw")

@dlt.table(name="carpark_bronze", table_properties={"quality":"bronze"})
def bronze():
    return (spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config",
                f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{SASL_USER}' password='{SASL_PASS}';")
        .load()
        .select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("json_str"),
            F.col("timestamp").alias("ingest_ts"),
            "topic","partition","offset"))

schema = T.StructType([
    T.StructField("timestamp", T.StringType()),
    T.StructField("carpark_number", T.StringType()),
    T.StructField("lot_type", T.StringType()),
    T.StructField("total_lots", T.StringType()),
    T.StructField("lots_available", T.StringType()),
])

@dlt.view
def silver_base():
    b = dlt.read_stream("carpark_bronze")
    return (b
      .withColumn("rec", F.from_json("json_str", schema))
      .select("ingest_ts","topic","partition","offset","rec.*")
      .withColumn("api_ts", F.to_timestamp("timestamp"))
      .withColumn("total_lots", F.col("total_lots").cast("int"))
      .withColumn("lots_available", F.col("lots_available").cast("int"))
      .withColumn("carpark_id",
                  F.coalesce(F.col("carpark_number"),
                             F.sha2(F.concat_ws("|", F.col("carpark_number"),
                                                F.coalesce(F.col("lot_type"), F.lit("N/A"))),256)))
      .withColumn("event_id",
                  F.sha2(F.concat_ws("|",
                      F.coalesce(F.col("carpark_number"),F.lit("")),
                      F.coalesce(F.col("lot_type"),F.lit("")),
                      F.date_format("api_ts","yyyy-MM-dd HH:mm:ss")
                  ),256)))

@dlt.table(name="carpark_silver", table_properties={"quality":"silver"})
@dlt.expect_or_drop("non_negative_avail","lots_available >= 0")
def silver():
    base = dlt.read_stream("silver_base").withWatermark("api_ts","10 minutes")
    return base.dropDuplicates(["event_id"])

@dlt.table(name="carpark_gold_latest", table_properties={"quality":"gold"})
def gold_latest():
    s = dlt.read_stream("carpark_silver")
    latest = (s.withWatermark("api_ts","10 minutes")
                .groupBy("carpark_id","lot_type")
                .agg(F.max("api_ts").alias("max_api_ts")))
    return (s.join(latest,
                   on=[s.carpark_id==latest.carpark_id,
                       s.lot_type==latest.lot_type,
                       s.api_ts==latest.max_api_ts],
                   how="inner")
             .select(s["*"]))
