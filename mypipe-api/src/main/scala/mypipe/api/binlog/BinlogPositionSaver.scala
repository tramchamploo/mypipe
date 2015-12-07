package mypipe.api.binlog

import java.io.{ File, PrintWriter }

import com.typesafe.config.ConfigFactory
import mypipe.mysql.BinaryLogFilePosition
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.RetryUntilElapsed
import org.slf4j.LoggerFactory

import scala.util.{ Success, Failure, Try }

/** Created by tramchamploo on 15/12/3.
 */
trait BinlogPositionSaver {

  protected val conf = ConfigFactory.load()

  def binlogLoadFilePosition(consumerId: String, pipeName: String): Option[BinaryLogFilePosition]

  def binlogSaveFilePosition(consumerId: String, filePos: BinaryLogFilePosition, pipe: String): Boolean

}

object BinlogPositionSaver {

  val conf = ConfigFactory.load()

  def apply() = conf.getString("mypipe.pos-saver") match {
    case "mypipe.api.binlog.ZookeeperBinlogPositionSaver" ⇒ ZookeeperBinlogPositionSaver
    case _ ⇒ FileBinlogPositionSaver
  }

}

object FileBinlogPositionSaver extends BinlogPositionSaver {

  val log = LoggerFactory.getLogger(getClass)

  val DATADIR = conf.getString("mypipe.data-dir")
  val LOGDIR = conf.getString("mypipe.log-dir")

  private val lastBinlogFilePos = scala.collection.concurrent.TrieMap[String, BinaryLogFilePosition]()

  try {
    new File(DATADIR).mkdirs()
    new File(LOGDIR).mkdirs()
  } catch {
    case e: Exception ⇒ println(s"Error while creating data and log dir $DATADIR, $LOGDIR: ${e.getMessage}")
  }

  def binlogGetStatusFilename(consumerId: String, pipe: String): String = {
    s"$DATADIR/$pipe-$consumerId.pos"
  }

  override def binlogLoadFilePosition(consumerId: String, pipeName: String): Option[BinaryLogFilePosition] = {
    try {

      val statusFile = binlogGetStatusFilename(consumerId, pipeName)
      val filePos = scala.io.Source.fromFile(statusFile).getLines().mkString.split(":")
      Some(BinaryLogFilePosition(filePos(0), filePos(1).toLong))

    } catch {
      case e: Exception ⇒ None
    }
  }

  override def binlogSaveFilePosition(consumerId: String, filePos: BinaryLogFilePosition, pipe: String): Boolean = {

    try {
      val fileName = binlogGetStatusFilename(consumerId, pipe)

      if (!lastBinlogFilePos.getOrElse(fileName, "").equals(filePos)) {

        val file = new File(fileName)
        val writer = new PrintWriter(file)

        log.info(s"Saving binlog position for pipe $pipe/$consumerId -> $filePos")
        writer.write(s"${filePos.filename}:${filePos.pos}")
        writer.close()

        lastBinlogFilePos(fileName) = filePos
      }

      true
    } catch {
      case e: Exception ⇒
        log.error(s"Failed saving binary log position $filePos for consumer $consumerId and pipe $pipe: ${e.getMessage}\n${e.getStackTraceString}")
        false
    }
  }

}

object ZookeeperBinlogPositionSaver extends BinlogPositionSaver {

  val log = LoggerFactory.getLogger(getClass)

  val PATH_PREFIX = conf.getString("mypipe.zk.path-prefix")

  lazy val zkClient = CuratorFrameworkFactory.newClient(conf.getString("mypipe.zk.conn"),
    new RetryUntilElapsed(Option(conf.getInt("mypipe.zk.max-retry-seconds")).getOrElse(20) * 1000, 1000))

  zkClient.start()

  private val lastBinlogFilePos = scala.collection.concurrent.TrieMap[String, BinaryLogFilePosition]()

  def zkPath(consumerId: String, pipe: String) = s"$PATH_PREFIX/$pipe-$consumerId"

  def ensureExists(path: String): Unit = {
    val exists = zkClient.checkExists().creatingParentContainersIfNeeded().forPath(path)
    if (exists == null) {
      log.info("binlog pos not exists, creating: {}", path)
      zkClient.create().forPath(path)
    }
  }

  override def binlogLoadFilePosition(consumerId: String, pipeName: String): Option[BinaryLogFilePosition] = Try {

    val path = zkPath(consumerId, pipeName)
    log.info("loading binlog pos: {}", path)

    ensureExists(path)

    val dataInByte = zkClient.getData.forPath(path)

    BinaryLogFilePosition.fromString(new String(dataInByte))
  }.toOption.flatten

  override def binlogSaveFilePosition(consumerId: String, filePos: BinaryLogFilePosition, pipe: String): Boolean = Try {

    val path = zkPath(consumerId, pipe)

    if (!lastBinlogFilePos.getOrElse(path, "").equals(filePos)) {

      ensureExists(path)

      zkClient.setData().forPath(path, s"${filePos.filename}:${filePos.pos}".getBytes)
      log.info(s"Binlog position for pipe $pipe/$consumerId -> $filePos has saved")

      lastBinlogFilePos(path) = filePos
    }
  } match {
    case Success(_) ⇒ true

    case Failure(e) ⇒
      log.error(s"Failed saving binary log position $filePos for consumer $consumerId and pipe $pipe: ", e)
      false
  }

}