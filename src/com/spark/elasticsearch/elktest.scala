package com.spark.elasticsearch

import org.apache.spark.sql.SparkSession
import java.util.Date
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level

object elktest {
  
 
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR);
    val sc = SparkSession.builder().appName("writeToElastic").master("local[*]").getOrCreate();

    val host: String = "localhost"
    val port: String = "9200"
    sc.conf.set("es.index.auto.create", "true")
    sc.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sc.conf.set("es.node", host)
    sc.conf.set("es.port", port)
    // Use username and password when cluster is secured
    sc.conf.set("es.net.http.auth.user", "")
    sc.conf.set("es.net.http.auth.pass", "")

    val dateformat = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    val date = dateformat.format(new Date)
    
    val lines = sc.read.option("header", "true").csv("D:\\SampleData\\used-cars-database\\cardata_sample_data.csv")
    lines.show()
    // Add a column in existing dataframe
    val modifiedDF = lines.withColumn("Date", lit(date))
    println("With new fields")
    modifiedDF.show()

    //Write DF to elastic search
    modifiedDF.write
      .format("org.elasticsearch.spark.sql")
      .option("es.nodes.wan.only", "true")
      .option("es.port", "9200")
      .option("es.nodes", "localhost")
      .mode("append")
      .save("usedcarspark/doc")
  }

}



