package com.spark.elasticsearch

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SaveMode

object pullDataFromElasticSearch {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR);
    val spark = SparkSession.builder.appName("ELK_Data")
      .master("local[*]").getOrCreate();

    val reader = spark.read
      .format("org.elasticsearch.spark.sql")
      .option("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .option("es.port", "9200")
      .option("es.nodes", "localhost")

    val df = reader.load("movieratingspark/doc")

    println("No of records in movies rating topic:" + df.count());

    df.show(); // Sample of the data. It will show only 20 rows.
    // Writing data to local file system
    // "file///" represents local file system
    df.write.format("csv")
    .option("header", true)
    .mode(SaveMode.Overwrite)
    .save("file:///D:\\SampleData\\Output\\elastic_data_download");

    println("Job completed");
  }

}