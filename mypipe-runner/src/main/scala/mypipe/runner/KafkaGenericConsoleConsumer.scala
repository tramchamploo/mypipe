package mypipe.runner

import mypipe.kafka.KafkaConsumer
import mypipe.kafka.consumer.GenericConsoleConsumer

object KafkaGenericConsoleConsumer extends App {
//
//  val topic: String = args(0)
//  val zkConnect: String = args(1)
//  val groupId: String = args(2)
//
//  val consumer = new GenericConsoleConsumer(topic = topic, zkConnect = zkConnect, groupId = groupId)
//
//  consumer.start
//
//  sys.addShutdownHook({
//    consumer.stop()
//  })

  val topics = List("medusa_forum_post_p0_generic", "medusa_forum_post_p1_generic", "medusa_forum_post_p2_generic", "medusa_forum_post_p3_generic",
    "medusa_forum_post_p4_generic", "medusa_forum_post_p5_generic", "medusa_forum_post_p6_generic", "medusa_forum_post_p7_generic",
    "medusa_forum_post_p8_generic", "medusa_forum_post_p9_generic", "medusa_forum_post_p10_generic", "medusa_forum_post_p11_generic",
    "medusa_forum_post_p12_generic", "medusa_forum_post_p13_generic", "medusa_forum_post_p14_generic", "medusa_forum_post_p15_generic",
    "medusa_forum_post_p16_generic", "medusa_forum_post_p17_generic", "medusa_forum_post_p18_generic", "medusa_forum_post_p19_generic")
  val zkConnect = "192.168.32.88:2181,192.168.32.41:2181,192.168.32.42:2181,192.168.32.43:2181,192.168.32.53:2181/kafka/bbs"
  val groupIdPrefix = "mypipe-test"

  topics.indices zip topics foreach { case (i, topic) =>
    val consumer = new KafkaConsumer(topic = topic, zkConnect = zkConnect, groupId = groupIdPrefix + i) {
      /** Called every time a new message is pulled from
        * the Kafka topic.
        *
        * @param bytes the message
        * @return true to continue reading messages, false to stop
        */
      override def onEvent(bytes: Array[Byte]): Boolean = log.info(bytes)
    }

    consumer.start

    sys.addShutdownHook({
      consumer.stop
    })

  }
}