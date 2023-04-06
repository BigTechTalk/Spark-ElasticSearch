package com.spark.elasticsearch

import org.apache.spark.sql.SparkSession
import java.util.Date
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level

object writeToElastic {

  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR);
    val spark = SparkSession.builder().appName("writeToElastic").master("local[*]").getOrCreate();

    val dateformat = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    val currentdate = dateformat.format(new Date)
    
    val lines = spark.read.option("header", "true").csv("D:\\SampleData\\used-cars-database\\usedcar.csv")
    
    lines.show()
    // Add a column in existing dataframe
    val modifiedDF = lines.withColumn("Date", lit(currentdate))
    println("With new fields")
    modifiedDF.show()

    //Write DF to elastic search
    modifiedDF.write
      .format("org.elasticsearch.spark.sql")
      .option("es.port", "9200") // ElasticSearch port
      .option("es.nodes", "localhost") // ElasticSearch host
      .mode("append") 
      .save("usedcarspark/doc") // indexname/document type     

     spark.stop();
  }
 

}





