package mypipe.mysql

import java.util.concurrent.{ TimeUnit, LinkedBlockingQueue }

import akka.actor.ActorSystem
import com.github.mauricio.async.db.mysql.MySQLConnection
import com.github.mauricio.async.db.{ Configuration, Connection }
import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.typesafe.config.Config
import mypipe.api.consumer.{ BinaryLogConsumer, BinaryLogConsumerErrorHandler, BinaryLogConsumerListener, BinaryLogConsumerTableFinder }
import mypipe.api.data.Table
import mypipe.api.event._
import mypipe.api.{ Conf, HostPortUserPass }
import mypipe.util.Eval
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable.{HashMap, ListBuffer}
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

  private val pool = new LinkedBlockingQueue[BinaryLogClient]()
  private val log = LoggerFactory.getLogger(getClass)

  val instances = HashMap.empty[BinaryLogClient, HostPortUserPass]

  val downInstances = ListBuffer.empty[BinaryLogClient]

  var current: BinaryLogClient = null

  var heartBeat: HeartBeat = null

  configs.foreach { i ⇒
    Try(new BinaryLogClient(i.host, i.port, i.user, i.password)) match {
      case Success(c) ⇒
        log.info(s"Adding mysql client to pool: ${i.host}-${i.port}")

        pool.offer(c)
        instances += c -> i

      case Failure(e) ⇒ log.error("BinaryLogClient init error: ", e)
    }
  }

  override def getClient: BinaryLogClient = Option(pool.peek()) match {
    case Some(client) ⇒
      if (!client.isConnected) client.connect()
      if (current != client) current = client

      if (heartBeat == null) {
        val info = instances(current)

        heartBeat = new HeartBeat(info.host, info.port, info.user, info.password) {

          override def onFailure() = {
            log.error("Switching to another mysql instance...")

            pool.poll(5, TimeUnit.SECONDS)
            heartBeat = null

            downInstances += current
          }

          override def onSuccess(): Unit = {}
        }

        heartBeat.beat()
      }

      client
    case None ⇒ throw new RuntimeException("No available client at the time!")
  }

  def getClientInfo: HostPortUserPass = instances(getClient)

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

abstract class HeartBeat(hostname: String, port: Int, username: String, password: String) {
  val system = ActorSystem("mypipe")
  implicit val ec = system.dispatcher

  val log = LoggerFactory.getLogger(getClass)

  var db: Db = null

  var failTimes = 0

  @volatile var connecting = false

  def onFailure(): Unit

  def onSuccess(): Unit

  def beat(): Unit = {
    system.scheduler.schedule(0 millis, Conf.MYSQL_HEARTBEAT_INTERVAL_MILLIS.millis) { () ⇒
      if (!connecting) {
        connecting = true

        Try {
          db = Db(hostname, port, username, password, "mysql")
          db.connect(Conf.MYSQL_HEARTBEAT_TIMEOUT_MILLIS)
        } match {
          case Failure(e) ⇒
            failTimes += 1

            if (failTimes == Conf.MYSQL_HEARTBEAT_MAX_RETRY) {
              log.error(s"Mysql server [$hostname:$port] is down!")
              onFailure()
            } else {
              log.warn(s"Mysql server [$hostname:$port] suspended, retrying...")
            }

          case Success(_) ⇒ onSuccess()
        }

        connecting = false
      }
    }
  }
}

/** Used when no event skipping behaviour is desired.
 */
trait NoEventSkippingBehaviour {
  this: BinaryLogConsumer[_, _] ⇒

  protected def skipEvent(e: TableContainingEvent): Boolean = false
}

/** Used alongside the the configuration in order to read in and
 *  compile the code responsible for keeping or skipping events.
 */
// TODO: write a test for this functionality
trait ConfigBasedEventSkippingBehaviour {
  this: BinaryLogConsumer[_, _] ⇒

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

trait ConfigBasedErrorHandlingBehaviour[BinaryLogEvent, BinaryLogPosition] extends BinaryLogConsumerErrorHandler[BinaryLogEvent, BinaryLogPosition] {

  val handler = Conf.loadClassesForKey[BinaryLogConsumerErrorHandler[BinaryLogEvent, BinaryLogPosition]]("mypipe.error.handler")
    .headOption
    .map(_._2.map(_.newInstance()))
    .getOrElse(None)

  def handleEventError(event: Option[Event], binaryLogEvent: BinaryLogEvent): Boolean =
    handler.exists(_.handleEventError(event, binaryLogEvent))

  def handleMutationError(listeners: List[BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition]], listener: BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition])(mutation: Mutation): Boolean =
    handler.exists(_.handleMutationError(listeners, listener)(mutation))

  def handleMutationsError(listeners: List[BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition]], listener: BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition])(mutations: Seq[Mutation]): Boolean =
    handler.exists(_.handleMutationsError(listeners, listener)(mutations))

  def handleTableMapError(listeners: List[BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition]], listener: BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition])(table: Table, event: TableMapEvent): Boolean =
    handler.exists(_.handleTableMapError(listeners, listener)(table, event))

  def handleAlterError(listeners: List[BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition]], listener: BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition])(table: Table, event: AlterEvent): Boolean =
    handler.exists(_.handleAlterError(listeners, listener)(table, event))

  def handleCommitError(mutationList: List[Mutation], faultyMutation: Mutation): Boolean =
    handler.exists(_.handleCommitError(mutationList, faultyMutation))

  def handleEventDecodeError(binaryLogEvent: BinaryLogEvent): Boolean =
    handler.exists(_.handleEventDecodeError(binaryLogEvent))

  def handleEmptyCommitError(queryList: List[QueryEvent]): Boolean =
    handler.exists(_.handleEmptyCommitError(queryList))
}

class ConfigBasedErrorHandler[BinaryLogEvent, BinaryLogPosition] extends BinaryLogConsumerErrorHandler[BinaryLogEvent, BinaryLogPosition] {
  private val log = LoggerFactory.getLogger(getClass)

  private val quitOnEventHandlerFailure = Conf.QUIT_ON_EVENT_HANDLER_FAILURE
  private val quitOnEventDecodeFailure = Conf.QUIT_ON_EVENT_DECODE_FAILURE
  private val quitOnEmptyMutationCommitFailure = Conf.QUIT_ON_EMPTY_MUTATION_COMMIT_FAILURE
  private val quitOnEventListenerFailure = Conf.QUIT_ON_LISTENER_FAILURE

  override def handleEventError(event: Option[Event], binaryLogEvent: BinaryLogEvent): Boolean = {
    log.error("Could not handle event {} from raw event {}", event, binaryLogEvent)
    !quitOnEventHandlerFailure
  }

  override def handleMutationError(listeners: List[BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition]], listener: BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition])(mutation: Mutation): Boolean = {
    log.error("Could not handle mutation {} from listener {}", mutation.asInstanceOf[Any], listener)
    !quitOnEventListenerFailure
  }

  override def handleMutationsError(listeners: List[BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition]], listener: BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition])(mutations: Seq[Mutation]): Boolean = {
    log.error("Could not handle {} mutation(s) from listener {}", mutations.length, listener)
    !quitOnEventListenerFailure
  }

  override def handleTableMapError(listeners: List[BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition]], listener: BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition])(table: Table, event: TableMapEvent): Boolean = {
    log.error("Could not handle table map event {} for table from listener {}", table, event, listener)
    !quitOnEventListenerFailure
  }

  override def handleAlterError(listeners: List[BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition]], listener: BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition])(table: Table, event: AlterEvent): Boolean = {
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

  private val clientInfo = getClientInfo

  protected var tableCache = new TableCache(clientInfo.host, clientInfo.port, clientInfo.user, clientInfo.password)

  override protected def findTable(tableMapEvent: TableMapEvent): Option[Table] = {
    val table = tableCache.addTableByEvent(tableMapEvent)
    Some(table)
  }

  override protected def findTable(tableId: java.lang.Long): Option[Table] =
    tableCache.getTable(tableId)

  override protected def findTable(database: String, table: String): Option[Table] = {
    tableCache.refreshTable(database, table)
  }
}
