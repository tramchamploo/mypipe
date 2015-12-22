package mypipe.mysql

import com.github.shyiko.mysql.binlog.event.{ Event ⇒ MEvent, _ }
import com.typesafe.config.Config

case class MySQLBinaryLogConsumer(override val config: Config, override val id: String)
  extends AbstractMySQLBinaryLogConsumer
  with HeartBeatClientWithOnStartPool
  with ConfigBasedErrorHandlingBehaviour[MEvent]
  with ConfigBasedEventSkippingBehaviour
  with CacheableTableMapBehaviour
