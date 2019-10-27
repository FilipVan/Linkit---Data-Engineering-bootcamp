   package com.linkit.spark

   import org.apache.spark._
   import org.apache.spark.sql._
   import org.apache.log4j._
   import org.apache.spark.sql.types.{StructType, StructField, StringType}
   import org.apache.spark.sql.Row
   //import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog
   import org.apache.hadoop.hbase

   object UploadCsvAndCreateTable extends App {
     
     
  
   // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)     

    

  // Main function where all operations will occur
  override def main (args:Array[String]) = {
  
  val spark = SparkSession
      .builder
      .appName("UploadCsvAndCreateTable")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
      import spark.implicits._
      import spark.sql
  
     // Defining SQL context to run Spark SQL
     // It uses library org.apache.spark.sql._
    val sqlContext = new SQLContext(spark.sparkContext)
    
    // Reading the text file
    val drivers = spark.sparkContext.textFile("../drivers.csv")

    // Defining the data-frame header structure
    val driversHeader = "driverId,name,ssn,location,certified,wage-plan"

    // Defining schema from header which we defined above
    // It uses library org.apache.spark.sql.types.{StructType, StructField, StringType}
    val schema = StructType(driversHeader.split(",").map(fieldName => StructField(fieldName,StringType, true)))

    // Converting String RDD to Row RDD for 5 attributes
    val rowRDD = drivers.map(_.split(",")).map(x => Row(x(0), x(1), x(2), x(3), x(4), x(5)))

    // Creating dataframe based on Row RDD and schema
    val driversDF = sqlContext.createDataFrame(rowRDD, schema)

    // Writing dataframe to a file with overwrite mode, header and single partition.
    //NOT TESTED ON HDFS could not get Hortonworks to have enough RAM
    //driversDF
    //.repartition(1)
    //.write
    //.mode("overwrite")
    //.format("com.databricks.spark.csv")
    //.option("header", "true")
    //.option(key = "encoding", value = "UTF-8")
    //.csv(path = "file:///home/maria_dev/drivers.csv") //Note there are 3 /, the first two are for the file protocol, the third one is for the root folder.
    
    //saves on local drive
    driversDF
    .repartition(1)
    .write
    .mode("overwrite")
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option(key = "encoding", value = "UTF-8")
    .save("drivers.csv")
     
    //we need an input for the hdfs location we will use a param from the console to get it
    //val hdfs_master = args(0)
    // ======= Writing files
    // Writing Dataframe as csv file
    
    //driversDF.write.mode(SaveMode.Overwrite).csv(hdfs_master + "admin/hdfs/wiki/testwiki.csv")
    
    
    //======== CREATE HIVE TABLE ========
    driversDF.createOrReplaceTempView("drivers")
    sqlContext.sql("drop table if exists drivers")
    sqlContext.sql("create table my_table as select * from drivers")
    
    //=========== CREATE HBase TABLE ================
    
    //define catalogue
    
    
    //we need to load the data from the csv file
    val dangerousDrivers = spark.sparkContext.textFile("../dangerous-driver.csv")
    
    //define the headers
    val dangerousDriversHeaders = "eventId,driverId,driverName,eventTime,eventType,latitudeColumn,longitudeColumn,routeId,routeName,truckId"
    
    val dangerousDriversSchema = StructType(dangerousDriversHeaders.split(",").map(fieldName => StructField(fieldName,StringType, true)))
    val dangerousRowRDD = dangerousDrivers.map(_.split(",")).map(x => Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9)))
    val dangerousDriversDF = sqlContext.createDataFrame(dangerousRowRDD, dangerousDriversSchema)
    
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"dangerous_driving"},
         |"rowkey":"key",
         |"columns":{
         |"eventId":{"cf":"rowkey", "col":"eventId", "type":"string"},
         |"driverId":{"cf":"person", "col":"driverId", "type":"string"},
         |"driverName":{"cf":"person", "col":"driverName", "type":"string"},
         |"eventTime":{"cf":"event", "col":"eventTime", "type":"string"},
         |"eventType":{"cf":"event", "col":"eventType", "type":"string"},
         |"latitudeColumn":{"cf":"address", "col":"latitudeColumn", "type":"string"},
         |"longitudeColumn":{"cf":"address", "col":"longitudeColumn", "type":"string"},
         |"routeId":{"cf":"address", "col":"routeId", "type":"string"},
         |"routeName":{"cf":"address", "col":"routeName", "type":"string"},
         |"truckId":{"cf":"address", "col":"truckId", "type":"string"}
         |}
         |}""".stripMargin
    
    //dangerousDriversDF.write.options(
      //Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "4"))
      //.format("org.apache.spark.sql.execution.datasources.hbase")
      //.save()
      
      val extraDriver = spark.sparkContext.textFile("../extra-driver.csv")
      val extraDriverRowRDD = extraDriver.map(_.split(",")).map(x => Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9)))
      val extraDriversDF = sqlContext.createDataFrame(extraDriverRowRDD, dangerousDriversSchema)
      dangerousDriversDF.unionAll(extraDriversDF)
      
      //update HBase table
      //dangerousDriversDF.write.options(
      //Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "4"))
      //.format("org.apache.spark.sql.execution.datasources.hbase")
      //.save()
      
    spark.sparkContext.stop()
   }   
    
 }