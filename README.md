# NY---Yellow-Taxi-Data-Project

Description:
Reading data from CSV files (New York Yellow taxi trip dataset) and transforming data to generate final output tables to be stored in traditional DBMS.

We have New York Yellow taxi trip dataset available and let us assume that the client wants to see the analysis of the overall data. Now, the size of the dataset is very huge (might go up to billions of rows) and using traditional DBMS is not feasible. So, we will use Big Data tool like Apache Spark to transform the data and generate the necessary aggregated output tables and store it in MySQL database. With this architecture the UI will be able to fetch reports and charts at much faster speed from MySQL than querying on the actual raw data.

Finally, the batch we use to analyze the data can be automated to run on daily basis within a fixed period of time.


Steps to perform: 
Read data from CSV file and store the data into HDFS (Hadoop File System) in compressed format.
Transform the raw data and build multiple table by performing the required aggregations.
Load the tables to MySQL.


Dataset:
The dataset for this project can be found here(https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). 
I have taken only "2023" year of "yellow taxi trip record" data.
