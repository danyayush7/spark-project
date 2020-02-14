package util

import org.slf4j.LoggerFactory
import java.io.FileReader
import java.security.InvalidParameterException

import scala.util.matching.Regex
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.lang3.StringUtils
import scala.collection.mutable

object JobUtils {

  private val logger = LoggerFactory.getLogger(getClass)

  def readConfigFile(fileName: String): Config = {
    if (StringUtils.isNoneBlank(fileName)) {
      ConfigFactory.parseReader(new FileReader(fileName.trim))
    } else {
      throw new InvalidParameterException("File Name cannot be null")
    }
  }

  def getContentsFromConfig(config: Config): (String, String, String) = {
    try {
      var appName = config.getString("appName")
      var dbName = config.getString("dbName")
      var tableName = config.getString("tableName")

      (appName, dbName, tableName)
    }
    catch {
      case e: Exception =>
        logger.error("Key-Value pair missing or wrongly specified")
        throw e
    }
  }

  def contentMap(): Map[String, String] ={
    var content_map = Map[String, String]()

    var strencodedform = "x-www-form-urlencoded|x-www-form-urlencoded; charset=utf-8|x-www-form-urlencoded; boundary=aab03x|x-www-form-urlencoded;charset=utf-8|x-www-form-urlencoded;charset=iso-8859-1|x-www-form-urlencoded|x-www-form-urlencoded|x-www-form-urlencoded;charset=utf-8|x-www-form-urlencoded; charset=utf-8|qfn-encoded"
    var encodedform = strencodedform.split('|')
    for(i <- 0 to encodedform.length-1){
      content_map += ("application/"+encodedform(i) -> "encoded url")
    }

    var strzip = "x-gzip|x-bzip2|zip;charset=utf-8|epub+zip|gzip|x-zip-compressed|x-gzip;charset=utf-8|x-bzip|vnd.adobe.article+zip|zip"
    var zip = strzip.split('|')
    for(i <- 0 to zip.length-1){
      content_map += ("application/"+zip(i) -> "zip")
    }

    var stroctet = "octet-streamapplication/octet-stream|octet-stream; charset=iso-8859-1|octet-stream|octet-stream; name=volleyball_girls_sophomore_schedule.ics|octet-stream charset=utf-8|octet-stream;charset=iso-8859-1|octet;charset=utf-8|octetstream|octet-stream; name=volleyball_girls_varsity_schedule.ics  |zip, application/octet-stream|octet-streamtext/plain|octet|octet-streamtext/plain;charset=utf-8|octet-stream;charset=utf-8|octet-stream; charset=binary|octet-streamtext/plain charset=utf-8|octet-stream; charset=utf-8|,applation/octet-stream; charset=utf-8|octet-stream; name=baseball_multiple_levels_schedule.ics  |vnd.google.octet-stream-compressible|octet-stream;charset=gbk|x-octet-stream|octet-stream;|octet-streamtext/html; charset=utf-8|octet-streamtext/html; charset=iso-8859-1|octet-stream; charset=x-user-defined|octet-streamoctet-stream"
    var octet = stroctet.split('|')
    for(i <- 0 to octet.length-1){
      content_map += ("application/"+octet(i) -> "binary stream")
    }

    var strproto = "x-protobuf|x-protobuffer|x-protobuf;charset=utf-8|x-protobuffer; charset=utf-8|protobuf|x-protobuf; charset=utf-8|x-protobuf;charset=iso-8859-1"
    var proto = strproto.split('|')
    for(i <- 0 to proto.length-1){
      content_map += ("application/"+proto(i) -> "protobuffer")
    }

    var strwoff = "font-woff; charset=utf-8|font-woff;charset=iso-8859-1|x-font-woff;charset=utf-8|font-woff;charset=utf-8|x-font-woff2|woff2|font-woff2|font-woff|x-font-woff|fontwoff2|x-font-woff;|x-woff2|font-woff2; charset=utf-8|x-font-woff|x-font-woff; charset=utf-8  |x-woff|font-woff2;charset=utf-8"
    var woff = strwoff.split('|')
    for(i <- 0 to woff.length-1){
      content_map += ("application/"+woff(i) -> "woff font")
    }

    var strxml = "rss+xml;charset=iso-8859-1|rss+xml|atom+xml; charset=utf-8|vnd.ms-sync.wbxml|x-rss+xml|ttaf+xml|vnd.openxmlformats-officedocument.spreadsheetml.sheet|xml;charset=iso-8859-1|vnd.sun.wadl+xml|rss+xml; qs=0.8|atom+xml|vnd.openxmlformats-officedocument.presentationml.presentation|text/xml|xml;|dash+xml|soap+xml|xml+phl|soap+xml; charset=utf-8|atom+xml; charset=utf-8; type=feed|xhtml+xml;charset=utf-8|xml;charset=us-ascii|xml|atom+xml;charset=utf-8|rdf+xml|atom+xml;charset=utf-8;type=feed|rss+xml; charset=utf-8; filename=content_rss20en_1.xml|rss+xml;|vnd.openxmlformats-officedocument.spreadsheetml.sheet;charset=utf-8|xml;charset=utf-8|v1.3+xml;charset=utf-8|xhtml+xml; charset=utf-8|rss+xml;charset=utf-8|xml; charset=utf-8|rss+xml;; charset=utf-8|bellshare-xmlrpc-binary|vnd.openxmlformats|vnd.adobe.adept+xml|opensearchdescription+xml; charset=utf-8|rss+xml; charset=utf-8|x-suggestions+xml; charset=utf-8|vnd.openxmlformats-officedocument.wordprocessingml.document|atom+xml; charset=utf-8; type=entry|vnd.microsoft.rtc.autodiscover+xml; v=1|vnd.ogc.se_xml;charset=utf-8|pls+xml|vnd.syncml+wbxml|xml; charset=iso-8859-1|vnd.ogc.se_xml; charset=utf-8|xhtml+xml|vnd.apple.installer+xml|octet-streamtext/xml|vnd.google.gdata.error+xml|f4m+xml|vnd.hp.cloud.eprint.papi.services.chat.1.1.shortreport+xml|ttml+xml|xml; utf-8|xml; charset=utf-8;|vnd.google-earth.kml+xml"
    var xml = strxml.split('|')
    for(i <- 0 to xml.length-1){
      content_map += ("application/"+xml(i) -> "xml")
    }

    var strjson = "json; version=0.1|json, charset=utf-8|jsonapplication/json|vnd.api.v3+json; charset=utf-8|problem+json|json; charset=gbk|json; charset=utf-8;|x-amz-json-1.0|vnd.api+json; charset=utf-8|json;charset:utf-8|json; charset=|json;charset=utf-8;charset=utf-8|json; charset: utf-8|json+oembed|vnd.steelhouse-v1.0+json|vnd.ammp.egress.1+json|jsonp; charset=utf-8|jsonapplication/x-www-form-urlencoded; charset=utf-8|json;charset=iso-8859-1|vnd.kafka.v1+json|x-json;charset=utf-8|vnd.api+json;com.liftopia.services.version=1.0; charset=utf-8|jsonjson;charset=utf-8|json; odata=verbose;charset=utf-8|,application-json|json; charset=utf-8|json; charset=utf-8;|x-msl+json;charset=utf-8|json; charset=utf-8application/json|json; charser=utf-8|json; charset=utf-8application/json; charset=utf-8|json; encoding=utf-8|octet-streamapplication/json|octet-streamapplication/json;charset=utf-8|json;  charset=utf-8|json;;charset=utf-8|json; charset=iso-8859-1|json;charset=utf-8;|x-suggestions+json;charset=utf-8|,application:json|json;charset=utf8|json;charsert=utf-8|json;odata=verbose;charset=utf-8|jsonp;charset=utf-8|encoded-json;charset=utf-8|json;; charset=utf-8|vnd.api+json|json;charset=gbk|json;charset=utf-8|json; charset=utf-8; encoding=utf8|hal+json;charset=utf-8|json+oembed; charset=utf-8|x-www-form-urlencodedapplication/json; charset=utf-8|vnd.geo+json; charset=utf-8|json; charset:utf-8|json charset=utf-8|json|json; charset=utf-8; charset=utf-8|json; encoding=utf8;charset=utf-8|json;|vnd.sonymobile.select+app+game+json;charset=utf-8|x-msl+json|x-json|vnd.sequoia+json;charset=utf-8|json; charset=utf8|json; charset=utf-8application/x-www-form-urlencoded; charset=utf-8|json; charset=windows-1251|json; encoding=utf-8; charset=utf-8|json.|manifest+json; charset=utf-8|x-json; charset=utf-8|manifest+json|json; version=1.0.0|json; charset=utf-8; =utf-8|hal+json; charset=utf-8|json; ;charset=utf-8|json; charset=us-ascii"
    var json = strjson.split('|')
    for(i <- 0 to json.length-1){
      content_map += ("application/"+json(i) -> "json")
    }

    var strjs = "x-javascript|javascript; charset=gb2312|x-javascriptapplication/javascript; charset=utf-8|javascripttext/javascript; charset=utf-8|x-javascript; charset=utf-8;|x-javascript; charset=utf-8; charset=utf-8|x-javascript; charset=us-ascii|x-javascript; charset=utf-8|octet-streamapplication/javascript|octet-streamapplication/x-javascript|javascript;charset=utf-8|javascript; charset=iso-8859-1;|x-javascript; charset=shift_jis|x-javascript; charset=euc-kr|x-javascript; charset=windows-1251|javascript;charset=iso-8859-1|x-javascript; charset=iso-8859-1|javascript; charset: utf-8|x-javascript; charset=utf-16|javascript|x-javascript; charset=windows-1252|x-javascript;charset=iso-8859-1|javascript; charset=gbk|x-javascript|javascript; charset=windows-1252|x-javascript; charset: utf-8|javascript; charset=utf-8|x+javascript|x-javascript; charset=utf8|javascriptapplication/x-javascript|javascript;encoding=utf-8|x-javascript; charset=gbk|javascript; charset=utf-8; valid=no;|x-javascript;charset=utf-8|javascript; charset=utf8|x-javascriptapplication/x-javascript|javascript; utf-8|x-javascript;charset=gb2312|x-javascript|javascriptapplication/javascript|x-javascript;|x-javascript-config|javascriptx-javascript,text/javascript |javascript; charset=utf-16|javascript; charset=iso-8859-1|javascript, */*;q=0.8|javascript; charset=utf-8application/javascript|javascript;|x-javascript; charset=gb2312"
    var js = strjs.split('|')
    for(i <- 0 to js.length-1){
      content_map += ("application/"+js(i) -> "javascript")
    }

    content_map
  }

  def checkContentTypeApplication(content: String): Int = {
    val pattern = new Regex("^application/")
    val result = pattern.findFirstIn(content).getOrElse("No")
    if (result == "application/")
      1
    else
      0
  }


}
