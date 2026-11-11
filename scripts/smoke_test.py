"""Step 0 smoke test.

Goal: prove that the three pieces agree (Java 17, Spark 3.5, Delta 3.3)
by starting a SparkSession with Delta enabled, writing a tiny Delta table
and reading it back. If this runs, every later step has a working engine.
"""
import logging

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("smoke_test")

# 1. Describe the session we want. Nothing starts yet: a builder is just a config object.
builder = (
    SparkSession.builder
    .appName("fuel-lakehouse-smoke-test")
    # local[*]: driver and executors live in ONE JVM on this machine, using all CPU cores.
    .master("local[*]")
    # Teach Spark SQL the Delta syntax (MERGE INTO, time travel, VACUUM...).
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    # Make the built-in catalog understand Delta tables.
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

# 2. configure_spark_with_delta_pip adds the Delta JAR (a Java library) to the JVM classpath.
#    The first run downloads it from Maven, so it needs internet and takes about a minute.
#    getOrCreate() actually launches the JVM.
spark = configure_spark_with_delta_pip(builder).getOrCreate()
log.info("Spark %s started with Delta extensions", spark.version)

# 3. A DataFrame is a plan, not data. createDataFrame builds the plan; nothing runs yet.
rows = [("1000001", "Gazole", 2.401), ("1000001", "SP98", 2.265), ("1000002", "E10", 2.178)]
df = spark.createDataFrame(rows, ["station_id", "fuel", "price"])

# 4. write is an ACTION: the plan executes and files land on disk.
path = "data/smoke_test/prices"
df.write.format("delta").mode("overwrite").save(path)
log.info("Delta table written to %s", path)

# 5. Read it back from disk. count() and show() are actions too.
back = spark.read.format("delta").load(path)
back.show()
log.info("%d rows read back", back.count())

spark.stop()
