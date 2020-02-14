package util

import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions.broadcast


object UDFUtils {

  private val logger = LoggerFactory.getLogger(getClass)

  def getDomainName(url: String): String = {
    try{
      if (url == null)
        ""
      else {
        if (url.split("/").length>2)
          url.split("/")(2)
        else
          ""
      }
    }
    catch {
      case ex: Exception =>
        logger.error("Exception in getDomainName method ")
        throw ex
    }
  }

  def getContentCategory(content: String): String = {
    try{
      if (JobUtils.checkContentTypeApplication(content) == 1){
        var content_map = JobUtils.contentMap()
        if (content_map.contains(content)) content_map(content)
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
        logger.error("Exception in getDomainName method ")
        throw e
    }
  }

}
