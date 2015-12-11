package mypipe.runner

import com.typesafe.config.{ Config, ConfigFactory }
import mypipe.api.Conf
import mypipe.api.consumer.BinaryLogConsumer
import mypipe.api.producer.Producer
import mypipe.mysql.MySQLBinaryLogConsumer
import mypipe.pipe.{ State, Pipe }
import org.apache.curator.framework.recipes.leader.{ CancelLeadershipException, LeaderSelector, LeaderSelectorListener }
import org.apache.curator.framework.state.ConnectionState
import org.apache.curator.framework.{ CuratorFramework, CuratorFrameworkFactory }
import org.apache.curator.retry.RetryUntilElapsed
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

object PipeRunner extends App {

  import PipeRunnerUtil._

  protected val log = LoggerFactory.getLogger(getClass)
  protected val conf = ConfigFactory.load()

  val LEADER_PATH = conf.getString("mypipe.zk.leader-path")

  lazy val producers: Map[String, Option[Class[Producer]]] = loadProducerClasses(conf, "mypipe.producers")
  lazy val consumers: Seq[(String, Config, Option[Class[BinaryLogConsumer[_, _]]])] = loadConsumerConfigs(conf, "mypipe.consumers")

  val zkClient = CuratorFrameworkFactory.newClient(conf.getString("mypipe.zk.conn"),
    new RetryUntilElapsed(Option(conf.getInt("mypipe.zk.max-retry-seconds")).getOrElse(20) * 1000, 1000))

  val pipes: Seq[Pipe[_, _]] = createPipes(conf, "mypipe.pipes", producers, consumers)

  if (pipes.isEmpty) {
    log.info("No pipes defined, exiting.")
    sys.exit()
  }

  sys.addShutdownHook({
    log.info("Shutting down...")
    pipes.foreach(_.disconnect())
  })

  val leaderSelectorListener = new LeaderSelectorListener {

    override def takeLeadership(client: CuratorFramework): Unit =
      while (true) {
        pipes.filter(p ⇒ !p.isConnected && p.state != State.STARTING).foreach { p ⇒
          log.info(s"Connecting pipe [{}]...", p)
          p.loadPosition()
          p.connect()
        }

        Thread.sleep(1000)
      }

    override def stateChanged(client: CuratorFramework, newState: ConnectionState): Unit =
      if ((newState == ConnectionState.SUSPENDED) || (newState == ConnectionState.LOST)) {
        log.info("Zookeeper connection lost, shutting down...")
        pipes.foreach(_.disconnect())
        throw new CancelLeadershipException
      }
  }

  val leaderSelector = new LeaderSelector(zkClient, LEADER_PATH, leaderSelectorListener)
  leaderSelector.autoRequeue()

  zkClient.start()
  leaderSelector.start()

}

object PipeRunnerUtil {

  protected val log = LoggerFactory.getLogger(getClass)

  def loadProducerClasses(conf: Config, key: String): Map[String, Option[Class[Producer]]] =
    Conf.loadClassesForKey[Producer](key)

  def loadConsumerClasses(conf: Config, key: String): Map[String, Option[Class[BinaryLogConsumer[_, _]]]] =
    Conf.loadClassesForKey[BinaryLogConsumer[_, _]](key)

  def loadConsumerConfigs(conf: Config, key: String): Seq[(String, Config, Option[Class[BinaryLogConsumer[_, _]]])] = {
    val consumerClasses = loadConsumerClasses(conf, key)
    val consumers = conf.getObject(key).asScala
    consumers.map(kv ⇒ {
      val name = kv._1
      val consConf = conf.getConfig(s"$key.$name")
      val clazz = consumerClasses.get(name).orElse(None)
      (name, consConf, clazz.getOrElse(None))
    }).toSeq
  }

  def createPipes(conf: Config,
                  key: String,
                  producerClasses: Map[String, Option[Class[Producer]]],
                  consumerConfigs: Seq[(String, Config, Option[Class[BinaryLogConsumer[_, _]]])]): Seq[Pipe[_, _]] = {

    val pipes = conf.getObject(key).asScala

    pipes.map(kv ⇒ {
      val name = kv._1
      val pipeConf = conf.getConfig(s"$key.$name")
      createPipe(name, pipeConf, producerClasses, consumerConfigs)
    }).filter(_ != null).toSeq
  }

  def createPipe(name: String, pipeConf: Config, producerClasses: Map[String, Option[Class[Producer]]], consumerConfigs: Seq[(String, Config, Option[Class[BinaryLogConsumer[_, _]]])]): Pipe[_, _] = {

    log.info(s"Loading configuration for $name pipe")

    val enabled = if (pipeConf.hasPath("enabled")) pipeConf.getBoolean("enabled") else true

    if (enabled) {

      val consumersConf = pipeConf.getStringList("consumers").asScala

      // config allows for multiple consumers, we only take the first one
      val consumerInstance = {
        val c = consumersConf.head
        createConsumer(pipeName = name, consumerConfigs.head, c)
      }

      // the following hack assumes a single producer per pipe
      // since we don't support multiple producers correctly when
      // tracking offsets (we'll track offsets for the entire
      // pipe and not per producer
      val producers = pipeConf.getObject("producer")
      val producerName = producers.entrySet().asScala.head.getKey
      val producerConfig = pipeConf.getConfig(s"producer.$producerName")
      // TODO: handle None
      val producerInstance = createProducer(producerName, producerConfig, producerClasses(producerName).get)

      new Pipe(name, consumerInstance, producerInstance)

    } else {
      // disabled
      null
    }
  }

  protected def createConsumer(pipeName: String, params: (String, Config, Option[Class[BinaryLogConsumer[_, _]]]), confName: String): BinaryLogConsumer[_, _] = {
    try {
      val consumer = params._3 match {
        case None ⇒ MySQLBinaryLogConsumer(params._2, confName).asInstanceOf[BinaryLogConsumer[_, _]]
        case Some(clazz) ⇒
          val consumer = {
            clazz.getConstructors.find(
              _.getParameterTypes.headOption.exists(_.equals(classOf[Config])))
              .map(_.newInstance(params._2))
              .getOrElse(clazz.newInstance())
          }

          //if (ctor == null) throw new NullPointerException(s"Could not load ctor for class $clazz, aborting.")

          // TODO: this is done specifically for the SelectConsumer for now, other consumers will fail since there is nothing forcing this ctor
          //val consumer = ctor.newInstance(params._2.user, params._2.host, params._2.password, new Integer(params._2.port))
          consumer.asInstanceOf[BinaryLogConsumer[_, _]]
      }

      consumer
    } catch {
      case e: Exception ⇒
        log.error(s"Failed to configure consumer ${params._1}: ${e.getMessage}\n${e.getStackTrace.mkString("\n")}")
        null
    }
  }

  protected def createProducer(id: String, config: Config, clazz: Class[Producer]): Producer = {
    try {
      val ctor = clazz.getConstructor(classOf[Config])

      if (ctor == null) throw new NullPointerException(s"Could not load ctor for class $clazz, aborting.")

      val producer = ctor.newInstance(config)
      producer
    } catch {
      case e: Exception ⇒
        log.error(s"Failed to configure producer $id: ${e.getMessage}\n${e.getStackTrace.mkString("\n")}")
        null
    }
  }
}
