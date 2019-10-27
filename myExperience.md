1. Setting Up Howrtonworks Sandbox
  - Download and install VirtualBox
  - Download Howrtonworks HDP Sandbox
  - Add the Sandbox to VirtualBox and Deploy

2. Configure Hortonworks Sandbox
  - Login on SSH http://localhost:4200 using root/hadoop
  - Update the password for root user
  - In order to login to Ambari server:
    - update the admin password using: ambari-admin-password-reset command in shell web client also known as Shell In a Box
  - Open Ambari Dashboard by going to http://localhost:1080

Problems Encountered:
 - PC only has 8GB of RAM - for all services to work on the Sandbox I needed to assign at least 10GB RAM
 - Assigned 5GB to start the sandbox but PC performance degraded and was unable to test properly
 - YARN, Hive, HBase and several other services were starting



========= Tasks 1 and 2 ===============
1. Few things that need to be set up before we can start with the tasks
  - Download Scala IDE
  - Install Java Development Toolkt
  - instal Spark (spark.apache.org)
  - Set up SPARK_HOME, JAVA_HOME and PATH environment variables
  - Install winutils.exe and set HADOOP_Home
2. Set Up Project
  - Create folder on local drive as our work enviorenment 
  - Open our folder with Eclipse and create a new package, and inside the package add Scala Application
  - Download and add External Jars


3. Task 1 - Read CSV Files, Upload to HDFS and create Hive Tables
 * Read and Upload CSV to HDFS
  - Import any extrnal dependencies in our application such as org.apache.spark, org.apache.spark.sql etc.
  - Create our SparkSession object (note for Windows and I had to set the .config() property)
  - Using the new SparkSession object import .implicits and .sql, this will give us access to thigs such as the SqlContext
  - In order to do whatever with the CSV files first we need to Read the CSV files from our project files
  - We need to define the headers in a StructType so we can later use it to create DataFrame (we will need this when we are saving the csv on HDFS)
  - Create RDD's (since the data we got each row is now just a string) and map them using .map(), also note the data is comma delimited so we want to use .split(",") in order to retrieve each field
  - Create DataFrame using sqlContext() -> pass params RDDRows and Headers
  - Save the DataFrame object to a file with overwrite mode, header, single partition (driversDF.repartition(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").option(key = "encoding", value = "UTF-8").csv(path = "file:///home/maria_dev/drivers.csv"))
 * Create HIVE TABLE
  - Using DataFrame object we create tempView (which can act as a temporary table in our application) --> driversDF.createOrReplaceTempView("drivers");
  - sqlContext.sql() lets us write query like language commands so we create the table --> sqlContext.sql("create table my_table as select * from drivers");


4. Task 2 - HBase
 - Read CSV file and create Data Frame object
 - Create catalog that represents the schema of the table in Hbase
 - import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog --> will allow us to map our Data Frame object to a new HBase table
 - Read in the data from the newly created Table, create a new RDD and map it to the record in the DB where driverId eq 4 and update the record
 - use println to print the information about the driver

