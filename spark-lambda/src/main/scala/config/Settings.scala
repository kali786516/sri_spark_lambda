package config

import com.typesafe.config.ConfigFactory

/**
  * Created by kalit_000 on 08/03/2017.
  */

//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.{SparkContext, SparkConf}

object Settings {
private val config=ConfigFactory.load()

  object WebLogGen {
    private val weblogGen=config.getConfig("clickstream")

    lazy val records=weblogGen.getInt("records")
    lazy val timeMultiplier=weblogGen.getInt("time_multiplier")
    lazy val pages=weblogGen.getInt("pages")
    lazy val visitors=weblogGen.getInt("visitors")
    lazy val filepath=weblogGen.getString("file_path")
    lazy val destpath=weblogGen.getString("dest_path")
    lazy val numberOfFiles=weblogGen.getInt("number_of_files")
    lazy val kafkatopic=weblogGen.getString("kafka_topic")
  }
}
