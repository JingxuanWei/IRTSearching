package IRTSearching

import java.io.File
import java.util
import java.util.Scanner

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import org.joda.time.DateTime
import org.json.JSONObject
import com.mongodb.spark._
import com.mongodb.spark.sql._
import org.apache.spark.sql.SQLContext


import scala.collection.JavaConversions._

/**
  * Created by Tom on 8/09/2016.
  */
object SparkQL {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Applications\\")
    val conf = new SparkConf().setAppName("TreeMerge").setMaster("local[*]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = SQLContext.getOrCreate(sc)
    val readData = sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://127.0.0.1:27017/TestDB.Test1")))
    // read data from MongoDB, and use filter to find data in certain range.
    //    val startTime = System.currentTimeMillis()
    //    val result = readData.filter(readData("acc.r3") >= 4 && readData("acc.r3") <= 5).show()
    //    val endTime = System.currentTimeMillis()
    //    println("Time Taken " + (endTime - startTime) + " milli seconds ")
    ///////////////////////////////////////////////////////////////////////////////
    // read data from MongoDB, and use Spark SQL to insert data
    readData.registerTempTable("railwayData")
    val startTime = System.currentTimeMillis()
    val SparkQLResult = sqlContext.sql("SELECT count(*) FROM railwayData Where acc.r3 > 4 and acc.r3 < 6 ")
    SparkQLResult.show()
    val endTime = System.currentTimeMillis()
    println("Time Taken " + (endTime - startTime) + " milli seconds ")
  }
}
