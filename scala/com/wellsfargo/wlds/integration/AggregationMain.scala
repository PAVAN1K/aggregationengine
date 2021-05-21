package com.wellsfargo.wlds.integration


import java.text.SimpleDateFormat
import java.util.Calendar

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{col, lit, lower, max, min, trim}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import java.io.File

import com.wellsfargo.wlds.integration.utils.CommonUtils

import scala.reflect.runtime.universe._

/**
 * Spark Aggregation or Analytics Engine App
 */
object AggregationMain {

  lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]) {

    val confFile: String = args(0)
    val appName: String = args(1)

    val spark = CommonUtils.getSparkHandle(appName, "yarn")
    val conf: Config = CommonUtils.loadConfig(spark, confFile)
    val steps = conf.getConfigList("steps")

    logger.info("Aggregation Main Started: " + CommonUtils.getCurrentTime("yyyy-MM-dd'T'HH:mm:ss"))
    println("Aggregation Main Started: " + CommonUtils.getCurrentTime("yyyy-MM-dd'T'HH:mm:ss"))
    logger.info("Iterating over steps")

    /*Evaluating over Steps Starts from here...*/
    steps.map(config => {

      println(config.getInt(CommonUtils.SEQ) + ": " + config.entrySet())

      config.getString(CommonUtils.TYPE) match {
        case "stats" => CommonUtils.collectStats(spark, config)
        case "execsql" => CommonUtils.execSql(spark, config.getString(CommonUtils.SQL_FILE))
        case "aggregator" => {

//          val dfRules = CommonUtils.queryMaprDBTable(spark, config.getString(CommonUtils.MAPRDB_TABLE_PATH), config)
          val dfRules = spark.sql("select * from db.tbname")
          val dfResult = CommonUtils.aggregator(spark, config, dfRules)


          CommonUtils.ParquetOutput(dfResult, config)
          CommonUtils.dropHiveTable(spark, config)
          CommonUtils.createHiveTable(spark, config, dfResult)
          val df = spark.read.parquet(config.getString(CommonUtils.LOCATION))

          println("DF Count:" + df.count())
        }

        case _ => logger.info("invalid type: " + config.getString(CommonUtils.TYPE))
      }

    })
    logger.info("Aggregation Main Completed: " + CommonUtils.getCurrentTime("yyyy-MM-dd'T'HH:mm:ss"))
  }

}

