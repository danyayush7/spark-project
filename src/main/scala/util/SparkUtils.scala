package util

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import org.slf4j.LoggerFactory
import org.apache.spark.broadcast
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{asc, desc}
import util.JobUtils.logger


object SparkUtils {

  private val logger = LoggerFactory.getLogger(getClass)

  def createSparkSession(appName: String): SparkSession = {
    try {
      val spark = SparkSession.builder()
        .appName(appName)
        .config("hive.metastore.uris", "thrift://jiouapp001-mst-01.gvs.ggn:9083")
        .config("spark.sql.warehouse.dir", "/apps/hive/warehouse")
        .enableHiveSupport()
        .getOrCreate()

      spark
    }
    catch {
      case ex: Exception =>
        logger.error("Exception in Creating Spark Session")
        throw ex
    }
  }

  def getSparkContext(spark: SparkSession): SparkContext = {
    spark.sparkContext
  }


  def writeDataframetoHiveTable(writeDf: DataFrame, format: String, partitionCols: mutable.Buffer[String] = null, saveMode: String,
                                db_name: String, table_name: String): Unit = {
    try{
      writeDf.write.format(format).partitionBy(partitionCols: _*).mode(saveMode).saveAsTable(db_name + "." + table_name)
    }
    catch {
      case ex: Exception =>
        logger.error("Exception in Writing Dataframe to Hive Tables")
        throw ex
    }
  }

  def readHiveTableAsDataframe(spark: SparkSession, selectCols: mutable.Buffer[String]): DataFrame = {
    try{
      var readDf = spark.sql("select * from ayush_test.edr_logs").selectExpr(selectCols: _*)
      readDf
    }
    catch {
      case ex: Exception =>
        logger.error("Exception in Reading Hive Table as Dataframe")
        throw ex
    }
  }

  def broadcastHashMap(spark: SparkSession): broadcast.Broadcast[Map[String,String]] = {
    var content_map = JobUtils.contentMap()
    var broadcastMap = getSparkContext(spark).broadcast(content_map)

    broadcastMap
  }

  def windowSpecs(partitionCols: mutable.Buffer[String]): WindowSpec = {
    try{
      Window.partitionBy(partitionCols.head, partitionCols.tail:_*)
    }
    catch {
      case e: Exception =>
        logger.error("Exception in windowSpecs method")
        throw e
    }
  }

}