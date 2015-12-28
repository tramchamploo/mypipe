package mypipe.snapshotter

import com.github.mauricio.async.db.Connection
import com.typesafe.config.{ Config, ConfigFactory }
import mypipe.api.consumer.BinaryLogConsumer
import mypipe.api.producer.Producer
import mypipe.mysql.Db
import mypipe.pipe.Pipe
import mypipe.runner.PipeRunnerUtil
import org.slf4j.LoggerFactory
import scopt.OptionParser

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object Snapshotter extends App {

  private[Snapshotter] case class CmdLineOptions(tables: Seq[String] = Seq.empty, noTransaction: Boolean = false)

  val log = LoggerFactory.getLogger(getClass)
  val conf = ConfigFactory.load()
  val Array(dbHost, dbPort, dbUsername, dbPassword, dbName) = conf.getString("mypipe.snapshotter.database.info").split(":")
  val db = Db(dbHost, dbPort.toInt, dbUsername, dbPassword, dbName)
  implicit lazy val c: Connection = db.connection
  val parser = new OptionParser[CmdLineOptions]("mypipe-snapshotter") {
    head("mypipe-snapshotter", "1.0")
    opt[Seq[String]]('t', "tables") required () valueName "<db1.table1>,<db1.table2>,<db2.table1>..." action { (x, c) ⇒
      c.copy(tables = x)
    } text "tables to include, format is: database.table" validate (t ⇒ if (t.nonEmpty) success else failure("at least one table must be given"))
    opt[Unit]('x', "no-transaction") optional () action { (_, c) ⇒
      c.copy(noTransaction = true)
    } text "do not perform the snapshot using a consistent read in a transaction"
  }

  val cmdLineOptions = parser.parse(args, CmdLineOptions())

  cmdLineOptions match {
    case Some(c) ⇒
      snapshot(c.tables, c.noTransaction)
    case None ⇒
      log.debug(s"Could not parser parameters, aborting: ${args.mkString(" ")}")
  }

  private def snapshot(tables: Seq[String], noTransaction: Boolean): Unit = {

    db.connect()

    while (!db.connection.isConnected) Thread.sleep(10)

    log.info(s"Connected to ${db.hostname}:${db.port} (withTransaction=${!noTransaction})")

    val events = MySQLSnapshotter.snapshotToEvents(MySQLSnapshotter.snapshot(tables, !noTransaction))

    log.info("Fetched snapshot.")

    lazy val producers: Map[String, Option[Class[Producer]]] = PipeRunnerUtil.loadProducerClasses(conf, "mypipe.snapshotter.producers")
    lazy val consumers: Seq[(String, Config, Option[Class[BinaryLogConsumer[_]]])] = PipeRunnerUtil.loadConsumerConfigs(conf, "mypipe.snapshotter.consumers")
    lazy val pipes: Seq[Pipe[_]] = PipeRunnerUtil.createPipes(conf, "mypipe.snapshotter.pipes", producers, consumers)

    if (pipes.length != 1) {
      throw new Exception("Exactly 1 pipe should be configured for the snapshotter.")
    }

    if (!pipes.head.consumer.isInstanceOf[SelectConsumer]) {
      throw new Exception(s"Snapshotter requires a SelectConsumer, ${pipes.head.consumer.getClass.getName} found instead.")
    }

    val pipe = pipes.head

    sys.addShutdownHook({
      pipe.disconnect()
      log.info(s"Disconnecting from ${db.hostname}:${db.port}.")
      db.disconnect()
      log.info("Shutting down...")
    })

    log.info("Consumer setup done.")

    pipe.connect()
    pipe.consumer.asInstanceOf[SelectConsumer].handleEvents(Await.result(events, 10.seconds))

    log.info("All events handled, safe to shut down.")
  }
}
