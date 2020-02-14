package assignment1

import assignment1.ContentTypeContribution.logger
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory
import util._
import org.apache.spark.sql.functions._

import scala.collection.mutable.Buffer

object DomainHitsTonnage  extends Serializable {

  private val logger = LoggerFactory.getLogger(getClass)
  //private val appName = "DomainHitsTonnage"

  private val domain = "domain_name"
  private val hits = "hits"
  private val totalBytes = "total_bytes"

  private val hour = "hour"
  private val minute = "minute"
  private val httpUrl = "http_url"
  private val httpReplyCode = "http_reply_code"
  private val transactionUplinkBytes = "transaction_uplink_bytes"
  private val transactionDownlinkBytes = "transaction_downlink_bytes"

  private val selectCols = Buffer(hour, minute, httpUrl, httpReplyCode, transactionUplinkBytes, transactionDownlinkBytes)
  private val partitionCols = Buffer(hour, minute)
  //private val inputPath = "/data/ayush/assignment/edr/"


  def main(args: Array[String]): Unit = {

    val config = JobUtils.readConfigFile("DomainHitsTonnage.json")
    val (appName, dbName, tableName) = JobUtils.getContentsFromConfig(config)

    val spark = SparkUtils.createSparkSession(appName)

    var edrDf = SparkUtils.readHiveTableAsDataframe(spark, selectCols)

    val selectDf = process(edrDf)

    SparkUtils.writeDataframetoHiveTable(selectDf, "orc", partitionCols, "append", dbName, tableName)

    spark.close()
  }

  def process(edrDF: DataFrame): DataFrame = {
    try{
      var hitsDf = edrDF.filter(col(httpReplyCode) >= 200 and col(httpReplyCode) < 300)

      val getDomainNameUDF = udf[String,String](UDFUtils.getDomainName)

      val domainDf = hitsDf.withColumn(domain, getDomainNameUDF(col(httpUrl))).filter(col(domain) =!= "")

      val aggDf = domainDf.groupBy(hour, minute, domain).
        agg(sum(transactionUplinkBytes).alias(transactionUplinkBytes),sum(transactionDownlinkBytes).alias(transactionDownlinkBytes),
          count(lit(1)).alias(hits)).withColumn(totalBytes, col(transactionDownlinkBytes)+col(transactionDownlinkBytes))

      val selectDf = aggDf.select(hour, minute, domain, hits, totalBytes)

      selectDf
    }
    catch{
      case e: Exception =>
        logger.error("Exception in process method ")
        throw e
    }
  }

}

