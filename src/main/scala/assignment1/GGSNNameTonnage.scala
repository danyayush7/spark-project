package assignment1

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import util._
import org.apache.spark.sql.functions._

import scala.collection.mutable.Buffer
import org.apache.spark.sql.functions.broadcast

object GGSNNameTonnage  extends Serializable {

  private val logger = LoggerFactory.getLogger(getClass)
  private val appName = "GGSNNameTonnage"

  private val ggsnName = "ggsn_name"
  private val totalBytes = "total_bytes"

  private val hour = "hour"
  private val ggsnIP = "ggsn_ip"
  private val transactionUplinkBytes = "transaction_uplink_bytes"
  private val transactionDownlinkBytes = "transaction_downlink_bytes"

  private val selectCols = Buffer(hour, ggsnIP, transactionUplinkBytes, transactionDownlinkBytes)
  private val partitionCols = Buffer(hour)
  //private val inputPathEdrLogs = "/data/ayush/assignment/edr/"
  private val ggsnXmlPath = "/data/ayush/ggsn.xml"


  def main(args: Array[String]): Unit = {

    val config = JobUtils.readConfigFile(args(0))
    val (appName, dbName, tableName) = JobUtils.getContentsFromConfig(config)

    val spark = SparkUtils.createSparkSession(appName)

    var edrDf = SparkUtils.readHiveTableAsDataframe(spark, selectCols)

    val ggsnDf = readXMLData(spark, ggsnXmlPath)

    var selectDf = process(ggsnDf, edrDf)

    SparkUtils.writeDataframetoHiveTable(selectDf, "orc", partitionCols, "append",dbName, tableName)

    spark.close()
  }

  def process(ggsnDF: DataFrame, edrDf: DataFrame): DataFrame = {
    try{
      val ggsn = broadcast(ggsnDF)

      val joinDf = edrDf.join(broadcast(ggsn), edrDf.col(ggsnIP) === ggsn.col(ggsnIP)).drop(ggsn.col(ggsnIP))

      val aggDf = joinDf.groupBy(hour, ggsnName, ggsnIP)
        .agg(sum(transactionUplinkBytes).alias(transactionUplinkBytes),
          sum(transactionDownlinkBytes).alias(transactionDownlinkBytes))
        .withColumn(totalBytes, col(transactionUplinkBytes)+col(transactionDownlinkBytes))

      val selectDf = aggDf.select(hour, ggsnName, ggsnIP, totalBytes)

      selectDf
    }
    catch{
      case e: Exception =>
        logger.error("Exception in process method ")
        throw e
    }
  }

  def readXMLData(spark: SparkSession, path: String): DataFrame = {
    try{
      var ggsnDf = spark.read.option("rowTag","ggsn").option("valueTag","xyz").format("xml").load(path)

      ggsnDf = ggsnDf.select(col("_name").alias(ggsnName),explode(col("rule")))

      ggsnDf = ggsnDf.select(col(ggsnName),col("col.condition._value").alias(ggsnIP))

      ggsnDf
    }
    catch{
      case e: Exception =>
        logger.error("Exception in readXMLData method ")
        throw e
    }
  }

}

