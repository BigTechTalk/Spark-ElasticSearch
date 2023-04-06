package com.spark.elasticsearch

import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import kafka.utils.Logging
import java.util.Properties

object customReceiverMysqlToElastic {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR);
    val driver = "com.mysql.jdbc.Driver"

    val url = "jdbc:mysql://localhost:3306/iot"
    val user = "root"
    val pass = "root"
    val tableName = "iot_temp"

    val spark = SparkSession.builder().appName("CustomerReceiver")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate();

    // You can configure Spark’s runtime config properties
    // ss.conf.set("spark.sql.shuffle.partitions", 6)

    val connectionProperties = new Properties()
    connectionProperties.put("user", user)
    connectionProperties.put("password", pass)

    connectionProperties.setProperty("driver", driver)

    val ssc = new StreamingContext(spark.sparkContext, Seconds(10));
    val customReceiverStream = ssc.receiverStream(new receiver(spark,url,tableName,connectionProperties))

  }

  class receiver(spark: SparkSession,url: String,tableName:String ,connectionProperties: Properties) 
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {

    // Start the thread that receives the data untill receiver is stopped
    def onStart() {

      
      new Thread("Socket Receiver") {
        override def run() { receive(spark,url,tableName,connectionProperties) }
      }.start()

    }
    //Stops the thread
    def onStop() {

    }

    def receive(spark: SparkSession,url: String,tableName:String ,connectionProperties: Properties) {

      try {

        val data = spark.read.option("numPartitions", 1)
          .jdbc(url, tableName, connectionProperties)
          .select("id", "room_id", "noted_date", "temp", "installed_loc", "seq_id");
        
        
        data.show()

      } catch {
        case conect: java.net.ConnectException => {
          restart("Error connecting to " + url, conect)
        }
        case throwexc: Throwable => {
          // restart if there is any other error
          restart("Error receiving data", throwexc)
        }
      }

    }

  }

}


