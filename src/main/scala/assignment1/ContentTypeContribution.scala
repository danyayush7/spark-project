package assignment1

import org.slf4j.LoggerFactory
import util._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.collection.mutable.Buffer


object ContentTypeContribution extends Serializable {

  private val logger = LoggerFactory.getLogger(getClass)
  val contentTypeMap = JobUtils.contentMap()
  //private val appName = "ContentTypeContribution"

  private val contentCategory = "content_category"
  private val domain = "domain_name"
  private val totalBytesPerDomain = "tonnage_per_domain"
  private val totalBytesPerDomainCategory = "tonnage_per_domain_category"
  private val contributionPerCategory = "contribution_per_domain"

  private val hour = "hour"
  private val minute = "minute"
  private val httpContentType = "http_content_type"
  private val httpUrl = "http_url"
  private val httpReplyCode = "http_reply_code"
  private val transactionUplinkBytes = "transaction_uplink_bytes"
  private val transactionDownlinkBytes = "transaction_downlink_bytes"

  private val selectCols = Buffer(hour, minute, httpContentType, httpUrl, httpReplyCode, transactionUplinkBytes, transactionDownlinkBytes)
  private val partitionCols = Buffer(hour, minute)

  def main(args: Array[String]): Unit = {

    val config = JobUtils.readConfigFile(args(0))
    val (appName, dbName, tableName) = JobUtils.getContentsFromConfig(config)

    val spark = SparkUtils.createSparkSession(appName)

    val edrDf = SparkUtils.readHiveTableAsDataframe(spark, selectCols)

    val selectDf = process(edrDf)

    SparkUtils.writeDataframetoHiveTable(selectDf, "orc", partitionCols, "append", dbName, tableName)

    spark.close()

  }

  def process(edrDF: DataFrame): DataFrame = {

    try {

      val edrDf = edrDF.filter(httpContentType + " is not null").filter(httpUrl + " is not null")

      val hitsDf = edrDf.filter(col(httpReplyCode) >= 200 and col(httpReplyCode) < 300)

      val getDomainNameUDF = udf[String, String](UDFUtils.getDomainName)
      val domainDf = hitsDf.withColumn(domain, getDomainNameUDF(col(httpUrl))).filter(col(domain) =!= "")

      val getContentCategoryUDF = udf[String, String](getContentCategory)
      val contentDf = domainDf.withColumn(contentCategory, getContentCategoryUDF(col(httpReplyCode))).filter(col(contentCategory) =!= "")

      val windowPartitionCols = Buffer(hour, minute, domain)
      val windowSpec = SparkUtils.windowSpecs(windowPartitionCols)

      val aggDf = contentDf.groupBy(hour, minute, domain, contentCategory)
        .agg(sum(transactionUplinkBytes).alias(transactionUplinkBytes), sum(transactionDownlinkBytes)
          .alias(transactionDownlinkBytes)).withColumn(totalBytesPerDomainCategory,
        col(transactionUplinkBytes) + col(transactionDownlinkBytes))
        .withColumn(totalBytesPerDomain, sum(totalBytesPerDomainCategory).over(windowSpec))

      val contributionDf = aggDf.withColumn(contributionPerCategory, bround((col(totalBytesPerDomainCategory) / col(totalBytesPerDomain)) * 100, 2))

      val selectDf = contributionDf.select(hour, minute, domain, contentCategory, contributionPerCategory, totalBytesPerDomain)

      selectDf
    }
    catch{
      case e: Exception =>
        logger.error("Exception in process method ")
        throw e
    }

  }

  def getContentCategory(content: String): String = {
    try {
      if (JobUtils.checkContentTypeApplication(content) == 1) {
        //var content_map = JobUtils.contentMap()
        if (contentTypeMap.contains(content)) contentTypeMap(content)
        else ""
      }

      else {
        if (content.split("/").length > 0)
          content.split("/")(0)
        else
          ""
      }
    }
    catch {
      case e: Exception =>
        logger.error("Exception in getContentCategory method ")
        throw e
    }
  }

}

