package mypipe.mysql

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import mypipe.api.data.{ ColumnMetadata, PrimaryKey, Table }
import mypipe.api.event.TableMapEvent
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{ Failure, Success }

/** A cache for tables whose metadata needs to be looked up against
 *  the database in order to determine column and key structure.
 *
 *  @param hostname of the database
 *  @param port     of the database
 *  @param username used to authenticate against the database
 *  @param password used to authenticate against the database
 */
class TableCache(val hostname: String, val port: Int, val username: String, val password: String) {
  protected val system = ActorSystem("mypipe")
  protected implicit val ec = system.dispatcher
  protected val tablesById = scala.collection.mutable.HashMap[Long, Table]()
  protected val tableNameToId = scala.collection.mutable.HashMap[String, Long]()
  protected lazy val dbMetadata = system.actorOf(MySQLMetadataManager.props(hostname, port, username, Some(password)), s"DBMetadataActor-$hostname:$port")
  protected val log = LoggerFactory.getLogger(getClass)

  def getTable(tableId: Long): Option[Table] = {
    println(tablesById.keySet)
    tablesById.get(tableId)
  }

  def refreshTable(tableId: Long): Future[Option[Table]] = {
    // FIXME: if the table is not in the map we can't refresh it.
    Future(tablesById.get(tableId)).flatMap {
      case Some(t) ⇒ refreshTable(t)
      case None    ⇒ Future.successful(None)
    }
  }

  def refreshTable(database: String, table: String): Future[Option[Table]] = {
    // FIXME: if the table is not in the map we can't refresh it.
    Future(tableNameToId.get(database + table)).flatMap {
      case Some(t) ⇒ refreshTable(t)
      case None    ⇒ Future.successful(None)
    }
  }

  def refreshTable(table: Table): Future[Option[Table]] = {
    // FIXME: if the table is not in the map we can't refresh it.
    addTable(table.id, table.db, table.name, flushCache = true)
  }

  def addTableByEvent(ev: TableMapEvent, flushCache: Boolean = false): Future[Option[Table]] = {
    addTable(ev.tableId, ev.database, ev.tableName, flushCache)
  }

  def addTable(tableId: Long, database: String, tableName: String, flushCache: Boolean): Future[Option[Table]] = {

    if (flushCache) {

      lookupTable(tableId, database, tableName) map {
        case Some(t) ⇒
          tablesById += tableId -> t
          tableNameToId += (t.db + t.name) -> t.id
          Some(t)
        case None ⇒ None
      }

    } else {
      Future(tablesById.get(tableId)) flatMap {
        case Some(t) ⇒ Future.successful(Some(t))
        case None ⇒
          lookupTable(tableId, database, tableName) andThen {
            case Success(t) ⇒
              val tt = t.get
              tableNameToId += (tt.db + tt.name) -> tt.id; tablesById += tableId -> tt
            case Failure(_) ⇒
          }
      }
    }
  }

  private def lookupTable(tableId: Long, database: String, tableName: String): Future[Option[Table]] = {

    // TODO: make this configurable
    implicit val timeout = Timeout(2.second)

    ask(dbMetadata, GetColumns(database, tableName, flushCache = true)) map {
      case (meta, pKey) ⇒ Some(Table(tableId, tableName, database, meta.asInstanceOf[List[ColumnMetadata]], pKey.asInstanceOf[Option[PrimaryKey]]))
      case _            ⇒ None
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: TableCache ⇒
      val t = that.asInstanceOf[TableCache]
      hostname == t.hostname && port == t.port && username == t.username && password == t.username
    case _ ⇒ false
  }

}
