package mypipe.mysql

import akka.actor.ActorSystem
import mypipe.api.Conf
import mypipe.util.Listenable
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/** Created by guohang.bao on 15/12/14.
 */
abstract class HeartBeat(
  hostname:     String,
  port:         Int,
  username:     String,
  password:     String,
  initialDelay: FiniteDuration = Duration.Zero
) extends Listenable {

  val system = ActorSystem("mypipe-heartbeat")
  implicit val ec = system.dispatcher

  val log = LoggerFactory.getLogger(getClass)

  val db: Db = Db(hostname, port, username, password, "mysql")

  var failTimes = 0

  @volatile var connecting = false

  def onFailure(): Unit

  def onSuccess(): Unit

  def beat() = system.scheduler.schedule(initialDelay, Conf.MYSQL_HEARTBEAT_INTERVAL_MILLIS.millis) {
    if (!connecting) {
      connecting = true

      Try {
        db.connect(Conf.MYSQL_HEARTBEAT_TIMEOUT_MILLIS)
      } match {
        case Failure(e) ⇒
          failTimes += 1

          if (failTimes == Conf.MYSQL_HEARTBEAT_MAX_RETRY) {
            log.error(s"Mysql server [$hostname:$port] is down!")
            notifyAll(BeatFailure)
            onFailure()
            failTimes = 0
          } else {
            log.warn(s"Mysql server [$hostname:$port] suspended, retrying...")
          }

        case Success(_) ⇒
          notifyAll(BeatSuccess)
          onSuccess()
          db.disconnect()

          if (failTimes > 0) {
            log.warn(s"Mysql server [$hostname:$port] recover from suspended.")
            failTimes = 0
          }
      }

      connecting = false
    }
  }
}

object BeatSuccess extends mypipe.util.Event

object BeatFailure extends mypipe.util.Event
