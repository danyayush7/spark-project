package assignment1

import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory
import util._
import org.apache.spark.sql.functions._

import scala.collection.mutable.Buffer

object Top5ContentTypes extends Serializable {

  private val logger = LoggerFactory.getLogger(getClass)
  val contentTypeMap = JobUtils.contentMap()
  //private val appName = "Top5ContentTypes"

  private val hits = "hits"
  private val contentCategory = "content_category"

  private val hour = "hour"
  private val radiusUserName = "radius_user_name"
  private val httpContentType = "http_content_type"
  private val httpReplyCode = "http_reply_code"
  private val transactionUplinkBytes = "transaction_uplink_bytes"
  private val transactionDownlinkBytes = "transaction_downlink_bytes"

  private val selectCols = Buffer(hour, httpContentType, httpReplyCode, radiusUserName, transactionUplinkBytes, transactionDownlinkBytes)
  private val partitionCols = Buffer(hour)
  //private val inputPath = "/data/ayush/assignment/edr/"


  def main(args: Array[String]): Unit = {

    val config = JobUtils.readConfigFile(args(0))
    val (appName, dbName, tableName) = JobUtils.getContentsFromConfig(config)

    val spark = SparkUtils.createSparkSession(appName)

    var edrDf = SparkUtils.readHiveTableAsDataframe(spark, selectCols)

    val (finalDf, aggDf) = process(edrDf)

    SparkUtils.writeDataframetoHiveTable(finalDf, "orc", partitionCols, "append", dbName, tableName)

    aggDf.unpersist()

    spark.close()
  }

  def process(edrDf: DataFrame): (DataFrame, DataFrame) = {
    try{
      var hitsDf = edrDf.filter(col(httpReplyCode) >= 200 and col(httpReplyCode) < 300).filter(httpContentType + " is not null")

      var getContentCategoryUDF = udf[String,String](UDFUtils.getContentCategory)

      var contentDf = hitsDf.withColumn(contentCategory, getContentCategoryUDF(col(httpContentType))).filter(col(contentCategory) =!= "" )

      var aggDf = contentDf.groupBy(hour, radiusUserName, contentCategory).agg(sum(transactionUplinkBytes)
        .alias(transactionUplinkBytes),sum(transactionDownlinkBytes).alias(transactionDownlinkBytes),
        count(lit(1)).alias(hits)).persist()

      val uploadWindowCols = Buffer(hour, contentCategory)
      var uploadWindowSpec = SparkUtils.windowSpecs(uploadWindowCols).orderBy(desc(transactionUplinkBytes))
      var upBytesDf = aggDf.withColumn("rank", rank().over(uploadWindowSpec))
        .filter(col("rank") <= 5)
        .groupBy(hour, contentCategory)
        .agg(collect_list("radius_user_name").alias("top10Subscriber_uploadBytes"))

      val downloadWindowCols = Buffer(hour, contentCategory)
      var downloadWindowSpec = SparkUtils.windowSpecs(downloadWindowCols).orderBy(desc(transactionDownlinkBytes))
      var downBytesDf = aggDf.withColumn("rank", rank().over(downloadWindowSpec))
        .filter(col("rank") <= 5)
        .groupBy(hour, contentCategory)
        .agg(collect_list("radius_user_name").alias("top10Subscriber_downloadBytes"))


      val hitsWindowCols = Buffer(hour, contentCategory)
      var hitsWindowSpec = SparkUtils.windowSpecs(hitsWindowCols).orderBy(desc(hits))
      var numHitsDf = aggDf.withColumn("rank", rank().over(hitsWindowSpec))
        .filter(col("rank") <= 5)
        .groupBy(hour, contentCategory)
        .agg(collect_list("radius_user_name").alias("top10Subscriber_hits"))

      var joinedDf = upBytesDf.join(downBytesDf,Seq(hour, contentCategory)).join(numHitsDf, Seq(hour, contentCategory))

      (joinedDf, aggDf)

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

