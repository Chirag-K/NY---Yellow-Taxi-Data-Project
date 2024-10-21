# Transfering Analyzed transformed data to MySQL

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType, StructType, StructField
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


# While loading from saved file
df_weekly = spark.read.orc("hdfs://localhost:9000/output/weekly_sales")
df_monthly = spark.read.orc("hdfs://localhost:9000/output/monthly_sales")
df_avg_congestion = spark.read.orc("hdfs://localhost:9000/output/avg_congestion_surcharge")
df_hourly_distribution = spark.read.orc("hdfs://localhost:9000/output/hourly_trip_distribution")
df_top_payments = spark.read.orc("hdfs://localhost:9000/output/top_payment_types")
df_monthly_payment_dist = spark.read.orc("hdfs://localhost:9000/output/payment_distribution")
df_total_passengers = spark.read.orc("hdfs://localhost:9000/output/total_passengers")

# To Connect with MySQL
connection_prop = {
    "user":"root",
    "password":"root",
    "driver":"com.mysql.cj.jdbc.Driver"
}

jdbcUrl = "jdbc:mysql://localhost:3306/source"

df_weekly.write.mode("overwrite").jdbc(url=jdbcUrl, table="weekly_sales", properties=connection_prop)
df_monthly.write.mode("overwrite").jdbc(url=jdbcUrl, table="monthly_sales", properties=connection_prop)
df_avg_congestion.write.mode("overwrite").jdbc(url=jdbcUrl, table="avg_congestion_surcharge", properties=connection_prop)
df_hourly_distribution.write.mode("overwrite").jdbc(url=jdbcUrl, table="hourly_trip_distribution", properties=connection_prop)
df_top_payments.write.mode("overwrite").jdbc(url=jdbcUrl, table="top_payment_types", properties=connection_prop)
df_monthly_payment_dist.write.mode("overwrite").jdbc(url=jdbcUrl, table="payment_distribution", properties=connection_prop)
df_total_passengers.write.mode("overwrite").jdbc(url=jdbcUrl, table="total_passengers", properties=connection_prop)



# Deleting a file which got saved with wrong name

import mysql.connector 

# Define the MySQL connection properties
jdbcUrl = "jdbc:mysql://localhost:3306/source"
db_user = "root"
db_password = "root"

# Create a connection to the MySQL database using mysql-connector
conn = mysql.connector.connect(
    host="localhost",
    user=db_user,
    password=db_password,
    database="source"
)
# Create a cursor object using the connection
cursor = conn.cursor()

# SQL query to drop the table (change "table_name" to the table you want to delete)
sql_drop_table = "DROP TABLE IF EXISTS weeky_sales"

# Execute the SQL query to drop the table
cursor.execute(sql_drop_table)

# Commit the transaction (if necessary)
conn.commit()

# Close the connection
cursor.close()
conn.close()



