package mypipe.util

import com.typesafe.config.ConfigFactory

/** Created by guohang.bao on 14-8-27.
 */
trait Paperboy {

  private final lazy val conf = ConfigFactory.load().getConfig("paperboy")

  lazy val paperboy = new PaperboyLogger(conf.getString("host"), conf.getInt("port"))

}
