package mypipe.kafka.consumer

import mypipe.api.event.Mutation
import mypipe.avro._
import mypipe.avro.schema.{GenericSchemaRepository, AvroSchemaUtils}
import mypipe.kafka._
import org.apache.avro.Schema

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Await, Future}
import ExecutionContext.Implicits.global
import scala.reflect.runtime.universe._

/** Created by guohang.bao on 16/1/26.
 */
abstract class ForumPostMutationAvroConsumer(zkConnect: String, groupId: String) {

  val DATABASE = "medusa"
  val TABLE = "forum_post"

  val timeout = 10 seconds
  var future: Option[Future[Unit]] = None

  val _onInsert: ForumPostInsert ⇒ java.lang.Boolean

  val _onUpdate: ForumPostUpdate ⇒ java.lang.Boolean

  val _onDelete: ForumPostDelete ⇒ java.lang.Boolean

  val kafkaConsumer = new KafkaMutationAvroConsumer[ForumPostInsert, ForumPostUpdate, ForumPostDelete, Short](
    topic = KafkaUtil.specificTopic("medusa", "forum_post"),
    zkConnect = zkConnect,
    groupId = groupId,
    schemaIdSizeInBytes = 2
  )(

    insertCallback = _onInsert(_),

    updateCallback = _onUpdate(_),

    deleteCallback = _onDelete(_),

    implicitly[TypeTag[ForumPostInsert]],
    implicitly[TypeTag[ForumPostUpdate]],
    implicitly[TypeTag[ForumPostDelete]]
  ) {

    protected val schemaRepoClient: GenericSchemaRepository[Short, Schema] = GenericInMemorySchemaRepo

    override def bytesToSchemaId(bytes: Array[Byte], offset: Int): Short = byteArray2Short(bytes, offset)
    private def byteArray2Short(data: Array[Byte], offset: Int) = ((data(offset) << 8) | (data(offset + 1) & 0xff)).toShort

    override protected def avroSchemaSubjectForMutationByte(byte: Byte): String = AvroSchemaUtils.specificSubject(DATABASE, TABLE, Mutation.byteToString(byte))

    override val insertDeserializer: AvroVersionedRecordDeserializer[ForumPostInsert] = new AvroVersionedRecordDeserializer[ForumPostInsert]()
    override val updateDeserializer: AvroVersionedRecordDeserializer[ForumPostUpdate] = new AvroVersionedRecordDeserializer[ForumPostUpdate]()
    override val deleteDeserializer: AvroVersionedRecordDeserializer[ForumPostDelete] = new AvroVersionedRecordDeserializer[ForumPostDelete]()
  }

  def start(): Unit = {
    future = Some(kafkaConsumer.start)
  }

  def stop(): Unit = {

    kafkaConsumer.stop
    future.map(Await.result(_, timeout))
  }
}
