package batch

/**
  * Created by kalit_000 on 08/03/2017.
  */

import java.lang.management.ManagementFactory

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SaveMode, SQLContext}
import domain._
import utils.SparkUtils._

object BatchJob {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("Lambda with Spark").set("spark.hadoop.validateOutputSpecs", "false")

    if (ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")){
      conf.setMaster("local[*]")
    }

    val sc = new SparkContext(conf)
    implicit val sqlContext=new SQLContext(sc)



    import org.apache.spark.sql.functions._
    import sqlContext.implicits._


    //initialise input rdd
    val sourceFile="file:///vagrant/data.tsv"
    //val sourceFile="file:///c:/spark_lambda_vagrant_file/spark-kafka-cassandra-applying-lambda-architecture/vagrant/data.tsv"
    val input=sc.textFile(sourceFile)

    //spark action

   // input.foreach(println)

    val inputRDD=input.flatMap{ line =>
      val record=line.split("\\t")
      val MS_IN_HOUR = 1000 * 60 * 60
      if (record.length == 7)
      Some(Activity(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR,record(1),record(2),record(3),record(4),record(5),record(6)))
      else
        None
    }

    //key is product and timestamp_hour
    val keyedByProduct = inputRDD.keyBy(a => (a.product,a.timestamp_hour)).cache()

    val visitorsByProduct = keyedByProduct
      .mapValues( a => a.visitor)
      .distinct()
      .countByKey()

    val activityByProduct=keyedByProduct
        .mapValues{ a =>
        a.action match {
          case "purchase" => (1,0,0)
          case "add_to_cart" => (0,1,0)
          case "page_view" => (0,0,1)
          }
        }
       .reduceByKey( (a,b) => (a._1+b._1,a._2+b._2,a._3+b._3))

    //visitorsByProduct.foreach(println)
    //activityByProduct.foreach(println)


    val inputRDDsql=input.flatMap{ line =>
      val record=line.split("\\t")
      val MS_IN_HOUR = 1000 * 60 * 60
      if (record.length == 7)
        Some(Activity(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR,record(1),record(2),record(3),record(4),record(5),record(6)))
      else
        None
    }.toDF()

    val df=inputRDDsql.select(
      add_months(from_unixtime(inputRDDsql("timestamp_hour")/1000),1).as("timestamp_hour"),
      inputRDDsql("referrer"),inputRDDsql("action"),inputRDDsql("prevPage"),inputRDDsql("page"),inputRDDsql("visitor"),inputRDDsql("product")
    ).cache()

    df.registerTempTable("activity")

    sqlContext.udf.register("UnderExposed",(pageViewcount:Long,purchaseCount:Long) => if (purchaseCount == 0) 0 else pageViewcount / purchaseCount)


    val visitorsByProductsql = sqlContext.sql(
      """SELECT product, timestamp_hour, COUNT(DISTINCT visitor) as unique_visitors
        |FROM activity GROUP BY product, timestamp_hour
      """.stripMargin)



    val activityByProductsql = sqlContext.sql("""SELECT
                                            product,
                                            timestamp_hour,
                                            sum(case when action = 'purchase' then 1 else 0 end) as purchase_count,
                                            sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
                                            sum(case when action = 'page_view' then 1 else 0 end) as page_view_count
                                            from activity
                                            group by product, timestamp_hour """).cache()

    activityByProductsql.registerTempTable("activityByProdcut")

    /*val underExposedProdcut = sqlContext.sql(
      """select product,timestamp_hour,UnderExposed(page_view_count,purchase_count) as negative_exposure
        |from activityByProdcut
        |order by negative_exposure DESC
        |limit 5
      """.stripMargin)
      */

   activityByProductsql.coalesce(5).write.partitionBy("timestamp_hour").mode(SaveMode.Overwrite).parquet("hdfs://lambda-pluralsight:9000/lambda/batch1")

    activityByProductsql.foreach(println)

    visitorsByProductsql.foreach(println)



  }
}
