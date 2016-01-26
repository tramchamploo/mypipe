package mypipe.util

/** Created by guohang.bao on 14-8-27.
 */

import java.net.URL

import org.apache.xmlrpc._
import org.apache.xmlrpc.client._
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

class PaperboyLogger(host: String, port: Int) {

  private val selfLogger = LoggerFactory.getLogger(this.getClass)

  lazy val rpcClient = {
    val config = new XmlRpcClientConfigImpl
    config.setServerURL(new URL(s"http://$host:$port"))
    new XmlRpcClient
  }

  val LEVEL_DEBUG = 1
  val LEVEL_INFO = 2
  val LEVEL_WARN = 3
  val LEVEL_ERROR = 4
  val LEVEL_CRITICAL = 5

  def withLevel(level: Symbol)(msg: ⇒ String, category: ⇒ String) {
    try {
      rpcClient.execute("log", List(Map("t" → (new DateTime().getMillis / 1000.0), "c" → category, "l" → level.name, "m" → msg)))
    } catch {
      case e: XmlRpcException ⇒ selfLogger.error(e.getMessage, e);
    }
  }

  def debug = withLevel('debug) _

  def info = withLevel('info) _

  def warn = withLevel('warn) _

  def error = withLevel('error) _

  def critical = withLevel('critical) _
}