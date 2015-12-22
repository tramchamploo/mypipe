package mypipe.api.repo

import com.typesafe.config.Config
import mypipe.api.consumer.BinaryLogConsumer
import mypipe.mysql.BinaryLogFilePosition
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.RetryUntilElapsed
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.util.{ Failure, Try }

/** Created by tramchamploo on 15/12/3.
 */
class ZookeeperBasedBinaryLogPositionRepository(pathPrefix: String, zkConn: String, maxRetry: Duration) extends BinaryLogPositionRepository {

  private var lastBinlogFilePos: Option[BinaryLogFilePosition] = None

  protected val log = LoggerFactory.getLogger(getClass)

  lazy val zkClient = CuratorFrameworkFactory.newClient(zkConn,
    new RetryUntilElapsed(maxRetry.toMillis.toInt, 1000))

  zkClient.start()

  def zkPath(consumerId: String) = s"$pathPrefix/$consumerId.pos"

  def ensureExists(path: String): Unit = {
    val exists = zkClient.checkExists().creatingParentContainersIfNeeded().forPath(path)
    if (exists == null) {
      log.info("binlog pos not exists, creating: {}", path)
      zkClient.create().forPath(path)
    }
  }

  override def loadBinaryLogPosition(consumer: BinaryLogConsumer[_]): Option[BinaryLogFilePosition] = Try {
    val path = zkPath(consumer.id)
    log.info("loading binlog pos: {}", path)

    ensureExists(path)

    val dataInByte = zkClient.getData.forPath(path)

    BinaryLogFilePosition.fromString(new String(dataInByte))
  }.toOption.flatten

  override def saveBinaryLogPosition(consumer: BinaryLogConsumer[_]): Unit = {
    val consumerId = consumer.id
    val filePos = consumer.getBinaryLogPosition.getOrElse({
      log.warn(s"Tried saving non-existent binary log position for consumer $consumerId, saving ${BinaryLogFilePosition.current} instead.")
      BinaryLogFilePosition.current
    })

    val path = zkPath(consumer.id)

    Try {
      if (!lastBinlogFilePos.exists(_.equals(filePos))) {
        ensureExists(path)
        zkClient.setData().forPath(path, s"${filePos.filename}:${filePos.pos}".getBytes)

        lastBinlogFilePos = Some(filePos)
      }
    } match {
      case Failure(e) ⇒
        log.error(s"Failed saving binary log position $filePos for consumer $consumerId: ", e)
      case _ ⇒
    }
  }
}

class ConfigurableZookeeperBasedBinaryLogPositionRepository(override val config: Config)
  extends ZookeeperBasedBinaryLogPositionRepository(
    config.getString("path-prefix"),
    config.getString("conn"),
    config.getInt("max-retry-seconds").seconds)
  with ConfigurableBinaryLogPositionRepository

