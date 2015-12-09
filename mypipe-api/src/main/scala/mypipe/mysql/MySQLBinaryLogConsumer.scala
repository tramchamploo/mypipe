package mypipe.mysql

import com.github.shyiko.mysql.binlog.event.{ Event ⇒ MEvent, _ }
import com.typesafe.config.Config

case class MySQLBinaryLogConsumer(override val config: Config)
  extends AbstractMySQLBinaryLogConsumer
  with HeartBeatClientPool
  with ConfigBasedErrorHandlingBehaviour[MEvent, BinaryLogFilePosition]
  with ConfigBasedEventSkippingBehaviour
  with CacheableTableMapBehaviour

