package assignment1

import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory
import util._
import org.apache.spark.sql.functions._

import scala.collection.mutable.Buffer


object Top10Subscribers extends Serializable {

  private val logger = LoggerFactory.getLogger(getClass)
  //private val appName = "Top10Subscribers"

  private val totalBytes = "total_bytes"

  private val hour = "hour"
  private val radiusUserName = "radius_user_name"
  private val transactionUplinkBytes = "transaction_uplink_bytes"
  private val transactionDownlinkBytes = "transaction_downlink_bytes"

  private val selectCols = Buffer(hour, radiusUserName, transactionUplinkBytes, transactionDownlinkBytes)
  private val partitionCols = Buffer(hour)
  //private val inputPath = "/data/ayush/assignment/edr/"

  /**
    *
    * @param args inputPath , dbName, tableName
    */
  def main(args: Array[String]): Unit = {

    val config = JobUtils.readConfigFile(args(0))
    val (appName, dbName, tableName) = JobUtils.getContentsFromConfig(config)

    val spark = SparkUtils.createSparkSession(appName)

    val edrDf = SparkUtils.readHiveTableAsDataframe(spark, selectCols)

    val (finalDf, aggDf) = process(edrDf)

    SparkUtils.writeDataframetoHiveTable(finalDf, "orc", partitionCols, "append", dbName, tableName)

    aggDf.unpersist()

    spark.close()
  }

  def process(edrDf: DataFrame): (DataFrame, DataFrame) = {
    try{
      val aggDf = edrDf.groupBy(hour,radiusUserName).agg(sum(transactionUplinkBytes).alias(transactionUplinkBytes),
        sum(transactionDownlinkBytes).alias(transactionDownlinkBytes)).withColumn(totalBytes,
        col(transactionUplinkBytes)+col(transactionDownlinkBytes)).persist()

      val uploadWindowCols = Buffer(hour)
      val uploadWindowSpec = SparkUtils.windowSpecs(uploadWindowCols).orderBy(desc(transactionUplinkBytes))
      val upBytesDf = aggDf.withColumn("rank", rank().over(uploadWindowSpec))
        .filter(col("rank") <= 10)
        .groupBy(hour)
        .agg(collect_list("radius_user_name").alias("top10Subscribers_uploadBytes"))

      val downloadWindowCols = Buffer(hour)
      val downloadWindowSpec = SparkUtils.windowSpecs(downloadWindowCols).orderBy(desc(transactionDownlinkBytes))
      val downBytesDf = aggDf.withColumn("rank", rank().over(downloadWindowSpec))
        .filter(col("rank") <= 10)
        .groupBy(hour)
        .agg(collect_list("radius_user_name").alias("top10Subscribers_downloadBytes"))

      val totalBytesWindowCols = Buffer(hour)
      val totalBytesWindowSpec = SparkUtils.windowSpecs(totalBytesWindowCols).orderBy(desc(totalBytes))
      val totalBytesDf = aggDf.withColumn("rank", rank().over(totalBytesWindowSpec))
        .filter(col("rank") <= 10)
        .groupBy(hour)
        .agg(collect_list("radius_user_name").alias("top10Subscribers_totalBytes"))

      val finalDf = upBytesDf.join(downBytesDf, Seq(hour)).join(totalBytesDf, Seq(hour))

      (finalDf, aggDf)
    }
    catch{
      case e: Exception =>
        logger.error("Exception in process method ")
        throw e
    }
  }


}

