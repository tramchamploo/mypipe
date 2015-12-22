package mypipe.mysql

import java.util.concurrent.{ LinkedBlockingQueue, TimeUnit }

import akka.actor.{ ActorSystem, Cancellable }
import com.github.mauricio.async.db.mysql.MySQLConnection
import com.github.mauricio.async.db.{ Configuration, Connection }
import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.typesafe.config.Config
import mypipe.api.consumer.{ BinaryLogConsumer, BinaryLogConsumerErrorHandler, BinaryLogConsumerListener, BinaryLogConsumerTableFinder }
import mypipe.api.data.Table
import mypipe.api.event._
import mypipe.api.{ Conf, HostPortUserPass }
import mypipe.util
import mypipe.util.{ Eval, Listenable, Listener }
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{ Failure, Success, Try }

trait Connections {
  val configs: List[HostPortUserPass]
}

trait ConfigBasedConnections extends Connections {
  protected val config: Config
  override val configs = config.getStringList("source").map(HostPortUserPass(_)).toList
}

trait ClientPool extends Connections {
  def getClient: BinaryLogClient

  def getClientInfo: HostPortUserPass
}

trait HeartBeatClientPool extends ClientPool with ConfigBasedConnections {

  private val log = LoggerFactory.getLogger(getClass)
  protected val pool = new LinkedBlockingQueue[BinaryLogClient]()

  protected val instances = HashMap.empty[BinaryLogClient, HostPortUserPass]

  protected var current: BinaryLogClient = null

  configs.foreach { i ⇒
    Try(new BinaryLogClient(i.host, i.port, i.user, i.password)) match {
      case Success(c) ⇒
        log.info(s"Adding mysql client to pool: ${i.host}:${i.port}")

        pool.offer(c)
        instances += c -> i

      case Failure(e) ⇒ log.error("BinaryLogClient init error: ", e)
    }
  }

  override def getClient: BinaryLogClient = Option(pool.peek()) match {
    case Some(client) ⇒
      if (current != client) current = client
      client
    case None ⇒ throw new RuntimeException("No available client at the time!")
  }

  def getClientInfo: HostPortUserPass = instances(getClient)

}

trait HeartBeatClientWithOnStartPool extends HeartBeatClientPool { self: MySQLBinaryLogConsumer ⇒

  protected var heartBeat: HeartBeat = null
  protected var heartbeatThread: Cancellable = null
  protected var recoveryThread: Cancellable = null

  override def getClient: BinaryLogClient = Option(pool.peek()) match {
    case Some(client) ⇒
      if (current != client) current = client

      if (heartBeat == null) {
        heartBeat = newHeartBeat(instances(current))
        heartbeatThread = heartBeat.beat()
      }

      client
    case None ⇒ throw new RuntimeException("No available client at the time!")
  }

  def newHeartBeat(info: HostPortUserPass) = {
    val hb = new HeartBeat(info.host, info.port, info.user, info.password) {

      override def onFailure() = {
        val prev = pool.poll(5, TimeUnit.SECONDS)

        val next = pool.peek()
        val nextInfo = instances(next)

        if (next != null && !next.isConnected) {
          log.error(s"Switching to another mysql instance..., [${nextInfo.host}:${nextInfo.port}], " +
            s"binlog file: ${prev.getBinlogFilename}, pos: ${prev.getBinlogPosition}")
          next.setBinlogFilename(prev.getBinlogFilename)
          next.setBinlogPosition(prev.getBinlogPosition)
          onStart()
        }

        heartBeat = null

        if (Conf.MYSQL_DO_RECOVER_AFTER_DOWN) doRecover(info)
      }

      override def onSuccess(): Unit = if (log.isDebugEnabled) log.debug(s"Heartbeat detection success: ${info.host}-${info.port}")
    }

    hb.addListener(new Listener {
      override def onEvent(evt: util.Event) = evt match {
        case BeatFailure ⇒ heartbeatThread.cancel()
        case _           ⇒
      }
    })

    hb
  }

  protected def doRecover(info: HostPortUserPass): Unit = {
    val heartBeat = new HeartBeat(info.host, info.port, info.user, info.password, Conf.MYSQL_SECONDS_BEFORE_RECOVER_AFTER_DOWN.seconds) {
      @volatile var nSuccess = 0

      override def onFailure(): Unit = {}

      override def onSuccess(): Unit = {
        nSuccess += 1
        if (nSuccess == 3) {
          log.info(s"Recover success! Putting mysql client back to pool: ${info.host}-${info.port}")
          pool.offer(new BinaryLogClient(info.host, info.port, info.user, info.password))
        }
      }
    }

    heartBeat.addListener(new Listener {

      @volatile var nFailure = 0

      override def onEvent(evt: util.Event) = evt match {
        case BeatSuccess ⇒ recoveryThread.cancel()
        case _ ⇒
          nFailure += 1
          if (nFailure == Conf.MYSQL_HEARTBEAT_MAX_RETRY) {
            log.info(s"Recover failed: ${info.host}-${info.port}")
            recoveryThread.cancel()
          }
      }
    })
    recoveryThread = heartBeat.beat()
  }
}

case class Db(hostname: String, port: Int, username: String, password: String, dbName: String) {

  private val configuration = new Configuration(username, hostname, port, Some(password))
  var connection: Connection = _

  def connect(): Unit = connect(timeoutMillis = 5000)

  def connect(timeoutMillis: Int) {
    connection = new MySQLConnection(configuration)
    val future = connection.connect
    Await.result(future, timeoutMillis.millis)
  }

  def select(db: String): Unit = {
  }

  def disconnect(): Unit = disconnect(timeoutMillis = 5000)

  def disconnect(timeoutMillis: Int) {
    val future = connection.disconnect
    Await.result(future, timeoutMillis.millis)
  }
}

/** Used when no event skipping behaviour is desired.
 */
trait NoEventSkippingBehaviour {
  this: BinaryLogConsumer[_] ⇒

  protected def skipEvent(e: TableContainingEvent): Boolean = false
}

/** Used alongside the the configuration in order to read in and
 *  compile the code responsible for keeping or skipping events.
 */
// TODO: write a test for this functionality
trait ConfigBasedEventSkippingBehaviour {
  this: BinaryLogConsumer[_] ⇒

  val includeEventCond = Conf.INCLUDE_EVENT_CONDITION

  val skipFn: (String, String) ⇒ Boolean =
    if (includeEventCond.isDefined)
      Eval(s"""{ (db: String, table: String) => { ! ( ${includeEventCond.get} ) } }""")
    else
      (_, _) ⇒ false

  protected def skipEvent(e: TableContainingEvent): Boolean = {
    skipFn(e.table.db, e.table.name)
  }
}

trait ConfigBasedErrorHandlingBehaviour[BinaryLogEvent] extends BinaryLogConsumerErrorHandler[BinaryLogEvent] {

  val handler = Conf.loadClassesForKey[BinaryLogConsumerErrorHandler[BinaryLogEvent]]("mypipe.error.handler")
    .headOption
    .map(_._2.map(_.newInstance()))
    .getOrElse(None)

  def handleEventError(event: Option[Event], binaryLogEvent: BinaryLogEvent): Boolean =
    handler.exists(_.handleEventError(event, binaryLogEvent))

  def handleMutationError(listeners: List[BinaryLogConsumerListener[BinaryLogEvent]], listener: BinaryLogConsumerListener[BinaryLogEvent])(mutation: Mutation): Boolean =
    handler.exists(_.handleMutationError(listeners, listener)(mutation))

  def handleMutationsError(listeners: List[BinaryLogConsumerListener[BinaryLogEvent]], listener: BinaryLogConsumerListener[BinaryLogEvent])(mutations: Seq[Mutation]): Boolean =
    handler.exists(_.handleMutationsError(listeners, listener)(mutations))

  def handleTableMapError(listeners: List[BinaryLogConsumerListener[BinaryLogEvent]], listener: BinaryLogConsumerListener[BinaryLogEvent])(table: Table, event: TableMapEvent): Boolean =
    handler.exists(_.handleTableMapError(listeners, listener)(table, event))

  def handleAlterError(listeners: List[BinaryLogConsumerListener[BinaryLogEvent]], listener: BinaryLogConsumerListener[BinaryLogEvent])(table: Table, event: AlterEvent): Boolean =
    handler.exists(_.handleAlterError(listeners, listener)(table, event))

  def handleCommitError(mutationList: List[Mutation], faultyMutation: Mutation): Boolean =
    handler.exists(_.handleCommitError(mutationList, faultyMutation))

  def handleEventDecodeError(binaryLogEvent: BinaryLogEvent): Boolean =
    handler.exists(_.handleEventDecodeError(binaryLogEvent))

  def handleEmptyCommitError(queryList: List[QueryEvent]): Boolean =
    handler.exists(_.handleEmptyCommitError(queryList))
}

class ConfigBasedErrorHandler[BinaryLogEvent] extends BinaryLogConsumerErrorHandler[BinaryLogEvent] {
  private val log = LoggerFactory.getLogger(getClass)

  private val quitOnEventHandlerFailure = Conf.QUIT_ON_EVENT_HANDLER_FAILURE
  private val quitOnEventDecodeFailure = Conf.QUIT_ON_EVENT_DECODE_FAILURE
  private val quitOnEmptyMutationCommitFailure = Conf.QUIT_ON_EMPTY_MUTATION_COMMIT_FAILURE
  private val quitOnEventListenerFailure = Conf.QUIT_ON_LISTENER_FAILURE

  override def handleEventError(event: Option[Event], binaryLogEvent: BinaryLogEvent): Boolean = {
    log.error("Could not handle event {} from raw event {}", event, binaryLogEvent)
    !quitOnEventHandlerFailure
  }

  override def handleMutationError(listeners: List[BinaryLogConsumerListener[BinaryLogEvent]], listener: BinaryLogConsumerListener[BinaryLogEvent])(mutation: Mutation): Boolean = {
    log.error("Could not handle mutation {} from listener {}", mutation.asInstanceOf[Any], listener)
    !quitOnEventListenerFailure
  }

  override def handleMutationsError(listeners: List[BinaryLogConsumerListener[BinaryLogEvent]], listener: BinaryLogConsumerListener[BinaryLogEvent])(mutations: Seq[Mutation]): Boolean = {
    log.error("Could not handle {} mutation(s) from listener {}", mutations.length, listener)
    !quitOnEventListenerFailure
  }

  override def handleTableMapError(listeners: List[BinaryLogConsumerListener[BinaryLogEvent]], listener: BinaryLogConsumerListener[BinaryLogEvent])(table: Table, event: TableMapEvent): Boolean = {
    log.error("Could not handle table map event {} for table from listener {}", table, event, listener)
    !quitOnEventListenerFailure
  }

  override def handleAlterError(listeners: List[BinaryLogConsumerListener[BinaryLogEvent]], listener: BinaryLogConsumerListener[BinaryLogEvent])(table: Table, event: AlterEvent): Boolean = {
    log.error("Could not handle alter event {} for table {} from listener {}", table, event, listener)
    !quitOnEventListenerFailure
  }

  override def handleCommitError(mutationList: List[Mutation], faultyMutation: Mutation): Boolean = {
    log.error("Could not handle commit due to faulty mutation {} for mutations {}", faultyMutation.asInstanceOf[Any], mutationList)
    !quitOnEventHandlerFailure
  }

  override def handleEmptyCommitError(queryList: List[QueryEvent]): Boolean = {
    val l: (String, Any) ⇒ Unit = if (quitOnEmptyMutationCommitFailure) log.error else log.debug
    l("Could not handle commit due to empty mutation list, missed queries: {}", queryList)
    !quitOnEmptyMutationCommitFailure
  }

  override def handleEventDecodeError(binaryLogEvent: BinaryLogEvent): Boolean = {
    log.trace("Event could not be decoded {}", binaryLogEvent)
    !quitOnEventDecodeFailure
  }
}

trait CacheableTableMapBehaviour extends BinaryLogConsumerTableFinder with HeartBeatClientPool {

  private var _tableInfo: HostPortUserPass = null
  private var _tableCache: TableCache = null

  protected def tableCache = if (_tableInfo != getClientInfo) {
    _tableInfo = getClientInfo
    _tableCache = new TableCache(_tableInfo.host, _tableInfo.port, _tableInfo.user, _tableInfo.password)
    _tableCache
  } else _tableCache

  override protected def findTable(tableMapEvent: TableMapEvent): Option[Table] = {
    Await.result(tableCache.addTableByEvent(tableMapEvent), Duration.Inf)
  }

  override protected def findTable(tableId: java.lang.Long): Option[Table] =
    tableCache.getTable(tableId)

  override protected def findTable(database: String, table: String): Option[Table] = {
    Await.result(tableCache.refreshTable(database, table), Duration.Inf)
  }
}
