from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, DoubleType, TimestampType, StringType
from pyspark.sql import DataFrame


spark = SparkSession.builder.appName("NYC Taxi Corp Analyzer").getOrCreate()
spark = SparkSession.builder \
    .appName("NYC_TaxiCorpAnalyzer") \
    .config("spark.driver.memory", "12g") \
    .config("spark.executor.memory", "12g") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    .config("spark.jars.packages", "mysql:mysql-connector-java:9.0.0") \
    .config("spark.executor.heartbeatInterval", "100000") \
    .config("spark.network.timeout", "500000") \
    .master("local[*]") \
    .getOrCreate()

final_df : DataFrame = None
#.schema(schema)
for i in range(1, 13):
    if i < 10:
        data_YT = spark.read.parquet(f"file:///E:/Programming/PySpark/NewYorkTaxiProject/Data/yellow_tripdata_2023-0{i}.parquet")
    else:
        data_YT = spark.read.parquet(f"file:///E:/Programming/PySpark/NewYorkTaxiProject/Data/yellow_tripdata_2023-{i}.parquet")

    print(f"Columns and data types for yellow_tripdata_2023-0{i}:")
    for col, dtype in data_YT.dtypes:
        print(f"{col}: {dtype}")
    print("\n")  # Print a new line for readability

    if final_df is None:
        final_df = data_YT
    else:
        # Union the current DataFrame with the final_df
        final_df = final_df.union(data_YT)


final_df.show()


# Cast columns to match the expected schema
final_df_schema = final_df.withColumn("VendorID", col("VendorID").cast(IntegerType())) \
                          .withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast(TimestampType())) \
                          .withColumn("tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast(TimestampType())) \
                          .withColumn("passenger_count", col("passenger_count").cast(IntegerType())) \
                          .withColumn("trip_distance", col("trip_distance").cast(DoubleType())) \
                          .withColumn("RatecodeID", col("RatecodeID").cast(IntegerType())) \
                          .withColumn("store_and_fwd_flag", col("store_and_fwd_flag").cast(StringType())) \
                          .withColumn("PULocationID", col("PULocationID").cast(IntegerType())) \
                          .withColumn("DOLocationID", col("DOLocationID").cast(IntegerType())) \
                          .withColumn("payment_type", col("payment_type").cast(IntegerType())) \
                          .withColumn("fare_amount", col("fare_amount").cast(DoubleType())) \
                          .withColumn("extra", col("extra").cast(DoubleType())) \
                          .withColumn("mta_tax", col("mta_tax").cast(DoubleType())) \
                          .withColumn("tip_amount", col("tip_amount").cast(DoubleType())) \
                          .withColumn("tolls_amount", col("tolls_amount").cast(DoubleType())) \
                          .withColumn("improvement_surcharge", col("improvement_surcharge").cast(DoubleType())) \
                          .withColumn("total_amount", col("total_amount").cast(DoubleType())) \
                          .withColumn("congestion_surcharge", col("congestion_surcharge").cast(DoubleType())) \
                          .withColumn("Airport_fee", col("Airport_fee").cast(DoubleType()))  # Ensure correct field name

# Display the dataframe
final_df_schema.show()


#Write DataFrame to HDFS
final_df_schema.write.mode("overwrite").orc("hdfs://localhost:9000/output/NY_YellowTaxiData")


# Next Step - Transformations


# While loading from saved file
df = spark.read.format("orc").load("hdfs://localhost:9000/output/NY_YellowTaxiData")
df.show()


# Analyzing Data
from pyspark.sql.functions import weekofyear, sum, lit, month, concat, col
from pyspark.sql.functions import *


# Week level aggregation of Sales of each vendor
df_weekly = final_df.groupBy("VendorID", month("tpep_pickup_datetime").alias("month"),  weekofyear("tpep_pickup_datetime").alias("week"))\
                    .agg(sum("total_amount").alias("total_sales"))
df_weekly_sorted = df_weekly.sort(col("VendorID").asc(), col("month").asc(), col("week").asc())
df_weekly_sorted.show()
# Concating with "month-week" column (upper one is better)
# df_weekly = final_df.withColumn("month-week", concat(month("tpep_pickup_datetime").cast("string"), lit("-"), weekofyear("tpep_pickup_datetime").cast("string")))\
#         .groupBy("VendorID", "month-week").agg(sum("total_amount").alias("total_sales"))



# Month level aggregation of Sales of each vendor
df_monthly = final_df.groupBy("VendorID", month("tpep_pickup_datetime").alias("month")).agg(sum("total_amount").alias("amount_collected"))
df_monthly_rounded =  df_monthly.withColumn("Amount", round(df_monthly.amount_collected, 2))
df_monthly_org = df_monthly_rounded.withColumn("Amount", format_number(df_monthly_rounded.Amount, 2)).drop(df_monthly_rounded.amount_collected)
df_monthly_sorted = df_monthly_org.sort(df_monthly_org.Amount.desc(), df_monthly_org.VendorID.asc())
df_monthly_sorted.show()


# Average amount of congestion surcharge each vendor charged in each month
df_Avg_CS = final_df.groupBy("VendorID", month("tpep_pickup_datetime").alias("month")).agg(avg("congestion_surcharge").alias("Avg_CongestionSurcharge"))
df_avg_congestion = df_Avg_CS.sort(df_Avg_CS.VendorID.asc(), df_Avg_CS.month.asc())
df_avg_congestion.show()


# Distribution of percentage of trips at each hour of day
df_hourly_distribution = final_df.groupBy(hour("tpep_pickup_datetime").alias("Hour")).agg(count("*").alias("Count by *"), (col("Count by *")/final_df.count()).alias("%age"))
df_hourly_distribution_alt = final_df.groupBy(hour("tpep_pickup_datetime").alias("Hour")).agg(count("*").alias("Count by *"))\
                                                                            .withColumn("%age", round(col("Count by *")/final_df.count() * 100,2))
df_hourly_distribution_alt.sort(df_hourly_distribution_alt.Hour).show(30)


# Top 3 payment types users used in each month (Getting payment_name information for payments table in MySQL)
connection_prop = {
    "user":"root",
    "password":"root",
    "driver":"com.mysql.cj.jdbc.Driver"
}

table = "payment_otc"

url = "jdbc:mysql://localhost:3306/source"


# spark = SparkSession.builder\
#     .appName("WithPaymentTypeData")\
#     .getOrCreate()

# Now read from MySQL database
payment_otc_df = spark.read.jdbc(url = url, table = table, properties = connection_prop)

payment_otc_df.show()


from pyspark.sql.window import Window

df_joined = final_df.join(payment_otc_df, final_df["payment_type"] == payment_otc_df["payment_type"])

df_joined_with_month = df_joined.withColumn("month", month(col("tpep_pickup_datetime")))

# Define the window specification
window_spec = Window.partitionBy("month").orderBy(col("count").desc())

# Get the top 3 payment types per month
df_top_payments = df_joined_with_month.groupBy("month", "payment_name") \
    .count() \
    .withColumn("rank", row_number().over(window_spec)) \
    .filter(col("rank") <= 3)

df_top_payments.show()

# Another manual method instead of window function
# df_grouped = df_joined_with_month.groupBy("month", "payment_name").count()

# # Sort by month and count descending
# df_sorted = df_grouped.orderBy(col("month"), col("count").desc())

# # Now you can filter the top 3 per month using a manual approach by selecting each month
# # This requires multiple steps for each month (assuming there are only a few months).

# # Example for filtering top 3 for month 1
# df_top_month_1 = df_sorted.filter(col("month") == 1).limit(3)

# # Example for filtering top 3 for month 2
# df_top_month_2 = df_sorted.filter(col("month") == 2).limit(3)

# # Union all the results (assuming multiple months)
# df_top_payments = df_top_month_1.union(df_top_month_2)

# # You would need to repeat this process for each month and combine them together
# df_top_payments.show()




# Distribution of payment type in each month
df_monthly_paymentType = df_joined.groupBy(month("tpep_pickup_datetime").alias("month"), "payment_name").count()
window_spec = Window.partitionBy("month")
df_with_total_count = df_monthly_paymentType.withColumn("monthly_total", sum("count").over(window_spec))
df_with_total_count.show()

df_monthly_payment_dist = df_with_total_count.withColumn("Dist %age", round(col("count")/col("monthly_total") * 100, 2))

df_monthly_payment_dist.show()


# Total passengers each Vendor served in each month
df_total_passengers = final_df.groupBy("VendorID", month("tpep_pickup_datetime").alias("month")) \
                            .agg(sum("passenger_count").alias("total_passengers"))\
                            .orderBy(col("VendorID"), col("month"))

df_total_passengers.show()


# storing the final output dataframes back to HDFS
df_weekly_sorted.write.mode("overwrite").orc("hdfs://localhost:9000/output/weekly_sales")
df_monthly_sorted.write.mode("overwrite").orc("hdfs://localhost:9000/output/monthly_sales")
df_avg_congestion.write.mode("overwrite").orc("hdfs://localhost:9000/output/avg_congestion_surcharge")
df_hourly_distribution_alt.write.mode("overwrite").orc("hdfs://localhost:9000/output/hourly_trip_distribution")
df_top_payments.write.mode("overwrite").orc("hdfs://localhost:9000/output/top_payment_types")
df_monthly_payment_dist.write.mode("overwrite").orc("hdfs://localhost:9000/output/payment_distribution")
df_total_passengers.write.mode("overwrite").orc("hdfs://localhost:9000/output/total_passengers")


#spark.stop




