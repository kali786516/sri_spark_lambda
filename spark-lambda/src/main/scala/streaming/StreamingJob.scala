package streaming

/**
  * Created by kalit_000 on 12/03/2017.
  */

import java.lang.management.ManagementFactory

import config.Settings
import domain.{ActivityByProduct, Activity}
import io.netty.handler.codec.string.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.{SparkContext, SparkConf}
import utils.SparkUtils._
import _root_.kafka.serializer.{DefaultDecoder, StringDecoder}

object StreamingJob {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("Lambda with Spark").set("spark.hadoop.validateOutputSpecs", "false")

    val isIDE={
      ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")
    }

    var checkpointDirecotry=""

    if (isIDE){
      conf.setMaster("local[*]")
      checkpointDirecotry="file:///c:/temp"
    } else {
      checkpointDirecotry="hdfs://lambda-pluralsight:9000/spark/checkpoint"
    }

    if (ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")){
      conf.setMaster("local[*]")
    }

    val sc = new SparkContext(conf)
    implicit val sqlContext=new SQLContext(sc)


    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    val batchDuration=Seconds(4)
    def streamingApp(sc:SparkContext,batchDuration:Duration) = {
      val ssc = new StreamingContext(sc, batchDuration)
      val wlc=Settings.WebLogGen
      val topic=wlc.kafkatopic

     /*val kafkaParams=Map(
        "zookeeper.connect" -> "localhost:2181",
        "group.id" -> "lambda",
        "auto.offset.reset" -> "largest"
      )*/

      val kafkaDirectParams = Map(
        "metadata.broker.list" -> "localhost:9092",
        "group.id" -> "lambda",
        "auto.offset.reset" -> "largest"
      )

      val kafkaDirectStream = KafkaUtils.createDirectStream[String,String,
        String,String](
        ssc,kafkaDirectParams,Set(topic)
      ).map(_._2)

      /*val kafkaStream=KafkaUtils.createStream[String,String,StringDecoder,StringDecoder](
        ssc,kafkaParams,Map(topic -> 1),StorageLevel.MEMORY_AND_DISK
      ).map(_._2)*/

      ssc.checkpoint(checkpointDirecotry)

      val inputpath = isIDE match {
        case true => "file:///c:/spark_lambda_vagrant_file/spark-kafka-cassandra-applying-lambda-architecture/vagrant/input"
        case false => "file:///vagrant/input"
      }

      val textDStream = ssc.textFileStream(inputpath)

      val activityStream=kafkaDirectStream.transform(input => {
        val offsetRanges=input.asInstanceOf[HasOffsetRanges].offsetRanges
        input.flatMap{ line =>
          val record=line.split("\\t")
          val MS_IN_HOUR = 1000 * 60 * 60
          if (record.length == 7)
            Some(Activity(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR,record(1),record(2),record(3),record(4),record(5),record(6)))
          else
            None
        }
      }).cache()

      activityStream.foreachRDD{ rdd =>
        val activityDF=rdd.toDF()

      }

     val statefuleactivitybyproduct=activityStream.transform(rdd => {
       val df=rdd.toDF()
       df.registerTempTable("activity")
       val activityByProductsql = sqlContext.sql("""SELECT
                                            product,
                                            timestamp_hour,
                                            sum(case when action = 'purchase' then 1 else 0 end) as purchase_count,
                                            sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
                                            sum(case when action = 'page_view' then 1 else 0 end) as page_view_count
                                            from activity
                                            group by product, timestamp_hour """)
       activityByProductsql.map( r => ((r.getString(0),r.getLong(1)),
         ActivityByProduct(r.getString(0),r.getLong(1),r.getLong(2),r.getLong(3),r.getLong(4))
         ))
     }).updateStateByKey((newItemsPerKey:Seq[ActivityByProduct],currentState:Option[(Long,Long,Long)]) => {
       var (purchase_count,add_to_cart_count,page_view_count) =currentState.getOrElse((0L,0L,0L))
       newItemsPerKey.foreach( a => {
         purchase_count += a.purchase_count
         add_to_cart_count += a.add_to_cart_count
         page_view_count += a.page_view_count
       })
       Some((purchase_count,add_to_cart_count,page_view_count))
     })


      statefuleactivitybyproduct.print(10)
      ssc
    }

    val ssc=getStreamingContext(streamingApp,sc,batchDuration)
    ssc.start()
    ssc.awaitTermination()

  }


}
