package com.wellsfargo.wlds.integration
import java.io.{File, FileOutputStream, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Calendar

import com.wellsfargo.wlds.integration.utils.CommonUtils

//import com.mapr.db.spark.sql._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{col, lit, lower, max, min, trim,concat}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._


object FileToMapRDB {
  lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)

  case class rules_template_schema(seq_no: Int, Name: String, TableName: String, ColumnName: String, DataType: String,
                                   Level: String, Expression: String)

  case class schema_MapRDB_Load(seq_no: Integer, Name: String,
                                TableName: String, ColumnName: String, DataType: String, Level: String,
                                Expression: String, OracleDataType: String, SourceTable: String, Version: String
                               )

  case class schema_DerivedCol_Ref(seq_no: Integer, Name: String,
                                   TableName: String, ColumnName: String, DataType: String, Level: String,
                                   Expression: String, ExtractedColsFromExpression: Array[String])

  case class schema_DerivedCol_Ref_Join(seq_no: Integer, Name: String,
                                        TableName: String, ColumnName: String, DataType: String, Level: String,
                                        Expression: String, ExtractedColsFromExpression: Array[String],
                                        LevelListCols: List[String])

  val DELIMITER = "\t"

  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      logger.info("ERROR IN INPUT")
      return
    }

    val confFile: String = args(0)
    val appName: String = args(1)

    val spark = CommonUtils.getSparkHandle(appName, "local[*]")
    val conf: Config = CommonUtils.loadConfig(spark, confFile)
    import spark.implicits._

    logger.info("Reading conf file:" + confFile)

    val applicationConf = conf.getConfig("application")
    val stepsConf = conf.getConfigList("steps")
    val ruleFilePath = applicationConf.getString(CommonUtils.RULES_FILE)
    val logFilePath = applicationConf.getString(CommonUtils.LOG_FILE)
    val maprdbTablePath = applicationConf.getString(CommonUtils.MAPRDB_TABLE_PATH)
    val insertTime = CommonUtils.getCurrentTime("yyyy-MM-dd'T'HH:mm:ss")

    val file = new File(logFilePath)
    if (file.exists()) file.delete()
    file.createNewFile()

    val pWriter = new PrintWriter(new FileOutputStream(file))

    var duplicatesFlag: Boolean = false
    var dataMismatchFlag: Boolean = false
    var wrongColumnRefFlag: Boolean = false
    var syntaxErrorFlag: Boolean = false
    logger.info(insertTime + " PROCESSING RULES TEMPLATE FILE >>>>> \" + ruleFilePath")

    val inputRulesDF: DataFrame = spark.read.option("header", "true").option("delimiter", DELIMITER)
      .csv(ruleFilePath)

    val inputRDD = inputRulesDF.rdd

    var metadataZipIndexRDD = inputRDD.zipWithIndex.map(cols =>
      (cols._2.toInt + 1, cols._1.getString(0),
        cols._1.getString(1), cols._1.getString(2), cols._1.getString(3),
        cols._1.getString(4), cols._1.getString(5), cols._1.getString(6),
        cols._1.getString(7), cols._1.getString(8)
      ))

    var metadataZipIndexRDDReq = metadataZipIndexRDD.map(cols => (
      cols._1, cols._2, cols._3,
      cols._4, cols._5, cols._6, cols._7
    ))

    /** *
     * Validation check: Check for the Data Type and Nulls for the Rules File.
     * If any thing is not matching. The process will be stopped with error message 100
     * The errors will be printed in the log file. No flag will be set.
     */
    val resultDataTypeMatch = metadataZipIndexRDDReq.map(cols => {
      (
        cols._1,
        List(CommonUtils.validation("Name", cols._2), CommonUtils.validation("TableName", cols._3),
          CommonUtils.validation("ColumnName", cols._4), CommonUtils.validation("DataType", cols._5),
          CommonUtils.validation("Level", cols._6), CommonUtils.validation("Expression", cols._7)
        ))
    }).collect.toList

    var overallFlag = false
    for (cols <- resultDataTypeMatch) {
      var reason = ""
      var caseString = ""

      var flagDuplicates = false
      for (vals <- cols._2) {
        caseString = vals._2 match {
          case 3 => flagDuplicates = true
            overallFlag = true
            vals._1 + ", Issue: Is empty and dataType mismatch"
          case 2 => flagDuplicates = true
            overallFlag = true
            vals._1 + ", Issue: Is empty"
          case 1 => flagDuplicates = true
            overallFlag = true
            vals._1 + ", Issue: DataType mismatch"
          case 0 => vals._1 + " None"
        }
        if (flagDuplicates) {
          reason =
            s"""
               |At SeqNo: ${(cols._1.toInt + 1)}, the result of validation of column: ${caseString}. The value is: ${vals._3}
               |""".stripMargin

          if (!caseString.contains(" None")) {
            printResult(pWriter, insertTime, "Validation Check", reason)
          }
        }
      }
      dataMismatchFlag = overallFlag
    }

    if (dataMismatchFlag) {
      logger.error("Processing stopped due to Validation errors. Please check the Logs")
      pWriter.flush()
      pWriter.close()
      spark.stop()
      System.exit(100)
    }

    metadataZipIndexRDD = metadataZipIndexRDD.map(cols =>
      (cols._1, cols._2,
        cols._3.replaceAll("^\"|\"$", " "),
        cols._4.replaceAll("^\"|\"$", " "),
        cols._5.replaceAll("^\"|\"$", " "),
        cols._6,
        cols._7.replaceAll("^\"|\"$", " "),
        cols._8, cols._9, cols._10)
    )

    var metadataZipIndex = metadataZipIndexRDD.map(cols => (
      cols._1, cols._2, cols._3,
      cols._4, cols._5, cols._6, cols._7))

    val metadataRDD = metadataZipIndex.map(cols =>
      rules_template_schema(
        cols._1, cols._2, cols._3, cols._4, cols._5, cols._6, cols._7
      ))

    /**
     * Duplicate Check Module will check for the duplciates for the combination of TableName and ColumnName
     * It will print in the log file for the duplicates and will set the flag
     */
    val duplicatesList = metadataRDD.map(cols => ((cols.TableName, cols.ColumnName), cols.seq_no, cols.Level))
      .groupBy(cols => cols._1)
      .map(cols => (cols._1, cols._2.toList.length, cols._2.map(x => x._2.toString.toInt + 1 + "-->" + x._3)))

    if (duplicatesList.count() > 0) {
      duplicatesList.filter(cols => cols._2 > 1).collect.toList.foreach(x => {

        val reason = s"""${x._1} Column is Duplicate with Count:  ${x._2}  At position at Lines (SeqNo --> Level No):  ${x._3}"""
        duplicatesFlag = true
        printResult(pWriter, insertTime, "Duplicate Check", reason)
      })
    }


    /**
     * Wrong Column Reference Section.
     * If any Column is not present or referenced in the previous Levels, this block of code will print in log file
     * and set the Flag.
     */
    val listDistinctTables = metadataRDD.groupBy(tbl => tbl.TableName)
      .map(cols => (cols._1, cols._2.toList.length)).map(x => x._1)

    listDistinctTables.collect.toList.map(validation => {
      val filterData = metadataRDD.filter(listcols => listcols.TableName == validation.toString)
      val listNewCol = filterData.map(listCols => (
        listCols.seq_no, listCols.Name, listCols.TableName,
        listCols.ColumnName, listCols.DataType, listCols.Level,
        listCols.Expression,
        CommonUtils.replaceAllString(listCols.Expression.toString)))
        .map(x => schema_DerivedCol_Ref(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8))

      val distinctLevels = filterData.groupBy(cols => cols.Level)

      val colsList = filterData.map(cols => (cols.ColumnName, cols.Level))
        .groupBy(x => x._2).map(x => (x._1, x._2.map(x => x._1))).collect.toList

      val distinctLevelList = distinctLevels.map(x => x._1).collect

      val listcols = distinctLevelList.map(levels => {
        (levels, colsList.filter(cols => cols._1.toInt < levels.toInt).map(x => x._2).flatten.distinct)
      }).toList

      val listcolsrdd = spark.sparkContext.parallelize(listcols)

      val newListColsrdd = listNewCol.map(x => (x.Level, (x.seq_no, x.Name, x.TableName, x.ColumnName, x.DataType,
        x.Expression, x.ExtractedColsFromExpression))).join(listcolsrdd)
        .map(x => schema_DerivedCol_Ref_Join(x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._1._5, x._1,
          x._2._1._6, x._2._1._7, x._2._2))
        .map(y => (
          y.seq_no, y.Name, y.TableName, y.ColumnName, y.DataType, y.Level, y.Expression,
          y.ExtractedColsFromExpression.filter(x => x != " ").toList.map(_.toString.toUpperCase).distinct,
          y.LevelListCols.distinct.map(_.toString.toUpperCase))
        )

      val resultWrongColumnRef = newListColsrdd.filter(x => x._6.toString.toInt > 0)
        .map(x => {
          (
            if ((x._8 diff x._9).length > 0) {
              (x._1.toString.toInt + 1, x._8 diff x._9)
            } else {
              "true"
            }, x._6, x._8, x._9)
        })
        .filter(x => x._1 != "true")

      resultWrongColumnRef.collect().foreach(result => {
        val reason =
          "Following Columns are not referenced or used in the previous Levels: " + result._1 +
            ".And Level Number is:" + result._2 + ".And the list of Columns in Expression: " + result._3

        wrongColumnRefFlag = true
        printResult(pWriter, insertTime, "Level Check For Columns", reason)
      })
    }
    )

    /**
     * Syntax Error Check for the Expression in the Rules file.
     * It will check for the syntax, if it is not correct will be printed in the log file. The flag will be set.
     */
    val expressionList = metadataRDD.map(x => (x.seq_no, x.Level, x.Expression)).collect.toList
    var reasonnullString = " "
    var arrayList: List[String] = List()
    for (i <- expressionList) {
      try {
        val i3 = CommonUtils.replaceAllString(i._3).toList.distinct
        arrayList = i3
        var res = i._3.toUpperCase()
        for (j <- arrayList) {
          if (j != " ") {
            res = res.replaceAll(j, "null")
            reasonnullString = res
          }
        }
        spark.sql("select " + res)
      }
      catch {
        case e: Exception =>
          syntaxErrorFlag = true
          val reason =
            s"""At Seq No: + (i._1.toInt + 1) + and Level Number is: + i._2 + .The Expression is failing with syntax.+
    The Failed Expression is: + i._3 + .And the reason is: + reasonnullString + .And the List is: " + arrayList"""
          printResult(pWriter, insertTime, "Expression Syntax Check", reason)
      }
    }

    if (syntaxErrorFlag || duplicatesFlag || wrongColumnRefFlag || dataMismatchFlag) {
      logger.error("There are problems with validation in file. Please check the log for more details")
    }
    else {
      val metadataDF = metadataZipIndexRDD
        .map(selcols => schema_MapRDB_Load(selcols._1, selcols._2,
          selcols._3, selcols._4,
          selcols._5, selcols._6,
          selcols._7, selcols._8,
          selcols._9, selcols._10
        )).toDF
        .withColumn("idColumn", concat(col("TableName"), col("ColumnName"), col("seq_no")))
        .withColumn("insert_time", lit(insertTime))

      println(metadataDF.count())
    }
    pWriter.flush()
    pWriter.close()

  }
  def printResult(printWriter: PrintWriter, insertTime: String, typeOfError: String, reason: String):Unit={
    if(!reason.isEmpty){
      val line=typeOfError + ": Error at " + insertTime + "=>" + reason
      printWriter.write(line)
      printWriter.write("\n")
      logger.error(line)

    }
  }
  }
