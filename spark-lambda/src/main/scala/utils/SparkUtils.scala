package utils

/**
  * Created by kalit_000 on 12/03/2017.
  */

import java.lang.management.ManagementFactory

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{StreamingContext, Duration}
import org.apache.spark.{SparkContext, SparkConf}

object SparkUtils {

  val isIDE={
    ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")
  }

 def getSparkContext(appName:String): Unit ={
   var checkpointDirecotry=""

   //get spark config

   Logger.getLogger("org").setLevel(Level.WARN)
   Logger.getLogger("akka").setLevel(Level.WARN)

   val conf = new SparkConf().setAppName(appName).set("spark.hadoop.validateOutputSpecs", "false")

   if (isIDE){
     conf.setMaster("local[*]")
     checkpointDirecotry="file:///c:/temp"
   } else {
     checkpointDirecotry="hdfs://lambda-pluralsight:9000/spark/checkpoint"
   }


   val sc = SparkContext.getOrCreate(conf)
   sc.setCheckpointDir(checkpointDirecotry)
   sc
 }

  def getSQLContext(sc: SparkContext) = {
    val sqlContext = SQLContext.getOrCreate(sc)
    sqlContext
  }

  def getStreamingContext(streamingApp:(SparkContext,Duration) => StreamingContext, sc:SparkContext, batchDuration:Duration)={
    val creatingFunc = () => streamingApp(sc,batchDuration)
   val ssc=sc.getCheckpointDir  match {
     case Some(checkpointDir) => StreamingContext.getActiveOrCreate(checkpointDir,creatingFunc,sc.hadoopConfiguration,createOnError = true)
     case None => StreamingContext.getActiveOrCreate(creatingFunc)
   }
    sc.getCheckpointDir.foreach(cp => ssc.checkpoint(cp))
    ssc
  }


}
