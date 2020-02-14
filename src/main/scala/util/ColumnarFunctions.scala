package util

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column


object ColumnarFunctions {

  def column_split(url: Column,regex: String): Column = {
    split(url,regex)
  }

  def getDomain(url_split: Column): Column = {
    try{
      when(size(url_split)>2, url_split.getItem(2))
        .otherwise(url_split.getItem(0))
    }
    catch {
      case ex: Exception =>
        throw ex
    }
  }

}


