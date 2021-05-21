package com.wellsfargo.wlds.integration.utils

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
import scala.reflect.runtime.universe._

object CommonUtils {
  lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)

  case class rules_maprdb_schema(_id: String, seq_no: Integer, insert_time: String, Name: String,
                                 TableName: String, ColumnName: String, DataType: String, Level: Integer, Expression: String
                                )

  /**
   * App Conf Steps Tag variables
   */
  val SEQ = "seq"
  val SQL_FILE = "sqlfile"
  val LOCATION = "location"
  val TYPE = "type"
  val NUM_PARTITION = "numpartition"
  val VIEW_NAME = "viewname"
  val DB_NAME = "dbname"
  val TABLE_NAME = "tablename"
  val COLUMNS = "columns"
  val PARTITION_KEYS = "partitionkeys"
  val RULES_FILE = "rulesfile"
  val LOG_FILE = "logfile"
  val MAPRDB_TABLE_PATH = "maprdbtablepath"
  val MAPRTDB_TABLE_FILTER = "maprdbtablefilter"
  val LEVELS = "levels"

  /**
   * Template header names
   */
  val LEVEL: String = "Level"
  val SEQ_NO: String = "seq_no"
  val FILTER_COLUMN = "TableName"

  /**
   * Validation Map for Rules File Columns. DataType and Nulls Check.
   * DataType check is a regex expression matching the input data.
   * IF false, nulls are not allowed for Nulls check and vice versa.
   */
  val validationMap: Map[String, (String, Boolean)] = Map(
    "Name" -> (".*", false),
    "TableName"-> ("[a-zA-Z0-9_#]", false),
    "ColumnName"-> ("[a-zA-Z0-9_#]", false),
    "DataType"-> ("(TINYINT|SMALLINT|LONG|INT|BIGINT|DOUBLE|FLOAT|STRING|BOOLEAN|BINARY|CHAR\\(\\d*\\)|VARCHAR\\(\\d*\\)|TIMESTAMP|DATE|DECIMAL\\(\\d*,?\\d*?\\))", false),
    "Level"-> ("[\\d+]", false),
    "Expression"-> ("[\\x20-\\x7E]", false)
)

  /**
   * Get Spark Session and setting log level as ERROR
   *
   * @param name
   * @param master
   * @return
   */
  def getSparkHandle(name: String, master: String): SparkSession = {

    val spark = SparkSession.builder.appName(name)
      .master(master)
      .enableHiveSupport()
      .config("spark.sql.parquet.writeLegacyFormat", "true")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    /** *
     * UDAF Registering for calculating Mode or MostCommon. It is Typed version. We have kept String.
     * UDF Registering for CollectWs. Converting Array[String] => Make String with Camma Delimited
     * UDAF Registering for weighted Average. Input is Double, typed Version. Its output is Double. Usage: (weight, value)
     */
    spark.udf.register("MostCommon", MostCommonUDAF[String])
    spark.udf.register("CollectWs", CommonUtils.collectws)
    spark.udf.register("WeightedAverage", WeightedAverageUDAF[Double]())
    spark
  }


def loadConfig(spark: SparkSession, confFileLocation: String): Config = {
  val appConf: Array[(String, String)] = spark.sparkContext.wholeTextFiles(confFileLocation).collect()
  val appConfStringContent = appConf(0)._2
  ConfigFactory.parseString(appConfStringContent)
}


  /**
   * Writing Parquet file as output with Overwrite option in the given Location
   *
   * @param df
   * @param config
   */
  def ParquetOutput(df: DataFrame, config: Config) {
    println("Started Writing to a Parquet file")
    df.write.mode(SaveMode.Overwrite).parquet(config.getString(LOCATION))
    logger.info("Completed Writing to a Parquet file")
    logger.info("Generated Parquet File Location: " + config.getString(LOCATION))
  }

  def collectws[T: TypeTag]: Seq[T] => String = x => x.map(y => y.toString).reduceOption(_ ++","++ _).mkString

  /**
   * Creating a Hive Table.
   *
   * @param spark
   * @param config
   * @param dfRules
   */
  def createHiveTable2(spark: SparkSession, config: Config, dfRules: DataFrame): Unit = {

    val columnsList: String = dfRules.select("ColumnName", "DataType").collect()
      .map(dt => dt(0) + " " + dt(1)).mkString(",")
    val sb = StringBuilder.newBuilder

    sb.append("CREATE EXTERNAL TABLE " + config.getString(TABLE_NAME))
    sb.append(" ( ")
    sb.append(columnsList)
    sb.append(" ) ")
    sb.append(" ROW FORMAT SERDE    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'")
    sb.append(" STORED AS INPUTFORMAT     'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'")
    sb.append(" OUTPUTFORMAT    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'")
    sb.append(" LOCATION '" + config.getString(LOCATION) + "'")

    spark.sql(sb.toString)
    logger.info("Table " + config.getString(TABLE_NAME) + " created at location : " + config.getString(LOCATION))
  }

  /** Create Hive Table from DataFrame
   *
   * @param spark
   * @param config
   * @param dfResult
   */
  def createHiveTable(spark: SparkSession, config: Config, dfResult: DataFrame): Unit = {

    dfResult.createOrReplaceTempView("dfResult")
    val columnsList: String = spark.sql("DESCRIBE dfResult").collect().map(dt => dt(0) + " " + dt(1)).mkString(",")
    val sb = StringBuilder.newBuilder

    sb.append("CREATE EXTERNAL TABLE " + config.getString(TABLE_NAME))
    sb.append(" ( ")
    sb.append(columnsList)
    sb.append(" ) ")
    sb.append(" ROW FORMAT SERDE    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'")
    sb.append(" STORED AS INPUTFORMAT     'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'")
    sb.append(" OUTPUTFORMAT    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'")
    sb.append(" LOCATION '" + config.getString(LOCATION) + "'")

    spark.sql(sb.toString)
    println("Table " + config.getString(TABLE_NAME) + " created at location : " + config.getString(LOCATION))
  }

  /**
   * Dropping Hive Table
   *
   * @param spark
   * @param config
   */
  def dropHiveTable(spark: SparkSession, config: Config): Unit = {
    spark.sql("DROP TABLE IF EXISTS " + config.getString(TABLE_NAME))
    logger.info("Table Dropped : " + config.getString(TABLE_NAME))
  }

//  def queryMaprDBTable(spark: SparkSession, maprDBTablePath: String, config: Config): DataFrame = {
//    val (minLevel, maxLevel): (Int, Int) = (config.getString(LEVELS).split(",")(0).toInt,
//      config.getString(LEVELS).split(",")(1).toInt)
//
//    val rules: DataFrame = spark.loadFromMapRDB[rules_maprdb_schema](maprDBTablePath)
//      .filter(lower(col(FILTER_COLUMN)) === config.getString(MAPRTDB_TABLE_FILTER).toLowerCase &&
//        trim(col(LEVEL)).between(lit(minLevel), lit(maxLevel)))
//      .sort(col(LEVEL), col(SEQ_NO))
//
//    rules
//  }

  /**
   * Executing Sql from a query taken from sql_File_Path and running the query in spark.
   *
   * @param spark
   * @param sqlFilePath
   * @return
   */
  def execSql(spark: SparkSession, sqlFilePath: String): DataFrame = {
    val hql = spark.read.textFile(sqlFilePath).collect().mkString(" ")
    spark.sql(hql.stripMargin)
  }

  /**
   * Aggregator is the main logic which runs in different levels calculating expression for each row in rules file
   * stored in MapRDB.
   *
   * @param spark
   * @param config
   * @param dfRules
   * @return
   */
  def aggregator(spark: SparkSession, config: Config, dfRules: DataFrame): DataFrame = {
    import spark.implicits._
    val hql = spark.read.textFile(config.getString(SQL_FILE)).collect().mkString(" ")
    if (config.getInt(NUM_PARTITION) > 0)
      spark.conf.set("spark.sql.shuffle.partitions", config.getInt(NUM_PARTITION))

    /* Reading HQL file */
    spark.sql(hql.stripMargin).createOrReplaceTempView(config.getString(VIEW_NAME))
    val dsRulesIn = dfRules.as[rules_maprdb_schema]

    /* Total Number of Level present in the metadata file for the table filter greater than zero */
    val levelsCount: Long = dsRulesIn.filter(col(LEVEL).cast("string").cast("int") > 0)
      .groupBy(LEVEL).count().count()
    var aggLevels:Row=null
    var minLevel:Int=0
    var maxLevel:Int=0
    if (levelsCount>0){
      aggLevels = dsRulesIn.agg(min(col(LEVEL)), max(col(LEVEL))).first
      minLevel  = aggLevels.getInt(0)
      maxLevel  = aggLevels.getInt(1)
    }else{
      minLevel  = 0
      maxLevel  = 0
    }

    logger.info("Number of Levels:" + levelsCount.toInt)

    if (levelsCount.toInt == 0)
      logger.info("There are not levels defined in the metadata rules in MapR-db for the selected MapR-db filter")

    val tmpView: String = config.getString(VIEW_NAME)
    var dfResult: DataFrame = null
    var df: DataFrame = null

    var selectString: String = "select * "
    val fromTableString: String = " from " + tmpView
    var columnsString: String = " "
    df = spark.sql(selectString + fromTableString)
    dfResult = df
    val dsRules = dsRulesIn.collect.distinct
    for (n <- Range(minLevel, maxLevel + 1)) {

      logger.info("Current Level: " + n)
      val dfRulesLevel = dsRules.filter(_.Level.equals(n))
      var dfRulesTx: Array[String] = null
      if (n == 0) {
        selectString = "select  "
        dfRulesTx = dfRulesLevel.map(r => " cast(" + r.Expression + " as " + r.DataType + ") " + " as " + r.ColumnName)
      }
      else {
        selectString = "select * "
        dfRulesTx = dfRulesLevel.map(r => " , " + " cast(" + r.Expression + " as " + r.DataType + ") " + " as " + r.ColumnName)
      }
      val countRules = dfRulesTx.length
      columnsString = dfRulesTx.take(countRules.toInt).mkString(",")
        .replaceAll("\\[|\\]", "").replaceAll(", *,", ",").replaceAll("\"", " ")

      val result = selectString + columnsString + fromTableString
      df = spark.sql(result)
      dfResult = df
      spark.sql("drop table if exists " + tmpView)
      dfResult.createOrReplaceTempView(tmpView)
    }
    dfResult
  }

  /*Get the current Time based on the Format given */
  def getCurrentTime(format: String): String = {
    val cal = Calendar.getInstance
    val sdf = new SimpleDateFormat(format)
    sdf.format(cal.getTime)
  }

//  def dataFrameToMapRDB(df: DataFrame, maprdbTablePath: String): Unit = {
//    df.write.option("Operation", "Overwrite").saveToMapRDB(maprdbTablePath, idFieldPath = idColumn)
//  }

  /**
   * Analyze Table with Table Name and Column Name
   *
   * @param spark
   * @param config
   */
  def collectStats(spark: SparkSession, config: Config): Unit = {

    logger.info("Collecting Stats for : " + config.getString(TABLE_NAME))

    if (config.hasPath(PARTITION_KEYS)) {
      logger.info("Collecting Stats partition key : " + config.getStringList(PARTITION_KEYS))
      spark.sql(s"ANALYZE TABLE " + config.getString(TABLE_NAME) +
        " PARTITION (" + config.getStringList(PARTITION_KEYS).mkString(",") + ") COMPUTE STATISTICS")
    } else spark.sql(s"ANALYZE TABLE " + config.getString(TABLE_NAME) + " COMPUTE STATISTICS")
    if (config.hasPath(COLUMNS)) {
      logger.info("Collecting Stats columns : " + config.getStringList(COLUMNS))
      spark.sql(s"ANALYZE TABLE " + config.getString(TABLE_NAME) + " COMPUTE STATISTICS FOR COLUMNS " +
        config.getStringList(COLUMNS).mkString(","))
    }
  }

  /**
   * Replace All String Function extracts the Columns names in the Expression.
   * This is used for syntax Check and Wrong reference of Columns.
   *
   * @param
   * @return
   */
  def replaceAllString(st: String): Array[String] = {

    val keywordList = Array("CASE", "WHEN", "THEN", "END", "PARTITION", "IS", "NULL", "NOT", "OVER",
      "ELSE", "ASC", "DESC", "ASCENDING", "DESCENDING", "AS", "CAST", "STRING", "INT", "DOUBLE", "BOOLEAN"
      , "LONG", "BYTE", "SHORT", "DECIMAL", "DATE", "TIMESTAMP")

    val pattern = """([\w#]*)""".r
    val st1 = " " + st.replaceAll("[A-Za-z]+(\\')[A-Za-z]+", " ") + " "

    var stringReplace = st1.toString().toUpperCase
      .replaceAll("\\) *OVER *\\(", " ")
      .replaceAll(" *(\\w+\\()+", " ")
      .replaceAll(" +(OR|AND|IS|NOT) +", " ")
      .replaceAll("PARTITION +BY", " ")
      .replaceAll("ORDER +BY", " ")
      .replaceAll(" *CASE +WHEN +", "  ")
      .replaceAll("IS +NULL", "  ")
      .replaceAll("IS +NOT +NULL", "  ")
      .replaceAll("(\'\')", " ")
      .replaceAll("(\'[\\x20-\\x26\\x28-\\x7e]+\')", " ")
      .replaceAll("\\,", " \\, ")
      .replaceAll("[A-Za-z]+(\\')[A-Za-z]+", " ")
      .replaceAll("([\\x20\\x21\\x24-\\x26\\x28-\\x2d\\x2f\\x3c-\\x3f\\x5b-\\x5e\\x7b-\\x7e]+)", " ")
      .replaceAll("[^\\x20-\\x94]+", " ")
      .replaceAll(" +", " ")

    var digitsflag = true
    var stringreplace = stringReplace
    val digitspattern = " +(\\d*\\.?\\d+)+ +".r
    while (digitsflag) {
      stringreplace = stringReplace.replaceAll(" +(\\d*\\.?\\d+)+ +", " ")
      stringReplace = stringreplace
      if (digitspattern.findAllIn(stringReplace).size == 0) {
        digitsflag = false
      }
    }

    stringReplace = stringReplace.replaceAll(" ", ",").split(",").map(x => x.trim).mkString(",")
    val arrayString = pattern.findAllIn(stringReplace).toArray.distinct

    val resString = arrayString.map(x => x.trim).map(x => {
      if (x.length > 0) {
        if (keywordList.contains(x)) {
          ""
        } else {
          x.toUpperCase.trim
        }
      } else {
        ""
      }
    })
    resString.filter(x => x != "").filter(x => x != " ").sortBy(x => x.length).reverse
  }

  /**
   * Validation Function for Rules File. Individual columns will be validated here for DataType and Nulls Check
   *
   * @param columnName
   * @param value
   * @return
   */
  def validation(columnName: String, value: String): (String, Int, String) = {

    val vm: (String, Boolean) = validationMap(columnName)
    val regexPattern = vm._1.r
    var dtypes, nulls = 0

    if (!vm._2 && (StringUtils.isEmpty(value) || StringUtils.isEmpty(value.trim))) nulls = 2 else nulls = 0

    if (nulls == 0) {
      if (regexPattern.replaceAllIn(value.toUpperCase.replaceAll("^\"|\"$", " "), " ").trim.length == 0)
        dtypes = 0
      else
        dtypes = 1
    }
    (columnName, dtypes + nulls, value)
  }

}