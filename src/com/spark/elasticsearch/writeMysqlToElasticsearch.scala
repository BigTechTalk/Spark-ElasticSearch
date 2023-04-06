package com.spark.elasticsearch

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import java.util.Date
import org.apache.spark.sql.functions._
import java.util.Properties
import org.apache.spark.sql.SaveMode

object writeMysqlToElasticsearch {
  
  def main(args:Array[String]){
    
    val spark = SparkSession.builder()
    .appName("WriteMysqlToElastic")
    .master("local[*]")
    .getOrCreate();
    

    val ssc = new StreamingContext(spark.sparkContext, Seconds(2));
    val driver = "com.mysql.jdbc.Driver"
    
    val url = "jdbc:mysql://localhost:3306/iot"
    val user = "root"
    val pass = "root"
    val tableName = "iot_temp"
    
    val connectionProperties = new Properties()
    connectionProperties.put("user", user)
    connectionProperties.put("password", pass)
   
    connectionProperties.setProperty("driver", driver)
    
    
   /*val data = spark.read.format("jdbc")
    .option("driver", driver)
        .option("url", url)
        .option("dbtable", tableName)
        .option("user", user)
        .option("password", pass)
        .option("numPartitions", 1)
        .load().select("id", "room_id", "noted_date", "temp", "installed_loc").where("installed_loc='In'").limit(20);*/
    
    
    val data = spark.read.option("numPartitions", 1)
      .jdbc(url, tableName, connectionProperties)
      .select("id", "room_id", "noted_date", "temp", "installed_loc","seq_id");
    
        val dateformat = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        val currentdate = dateformat.format(new Date)
        
        val modifiedDF = data.withColumn("Date", lit(currentdate))
         
        // Code to write to elastic search
        modifiedDF.write
        .format("org.elasticsearch.spark.sql")
        .option("es.port", "9200") // ElasticSearch port
        .option("es.nodes", "localhost") // ElasticSearch host
        .mode("append")
        .save("iotdata/doc") // indexname/document type   
    
        // Select max from the mqsql and write to a diffrent table for refrence
       data.agg(max("seq_id")).write.mode(SaveMode.Overwrite)
      .option("numPartitions", 1)
      .jdbc(url, "iot.latestrecord", connectionProperties) 
       
        data.show()
        modifiedDF.show()
  }
  
}