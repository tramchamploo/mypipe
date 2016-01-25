package mypipe.producer

import com.typesafe.config.Config
import mypipe.api.event.{ AlterEvent, UpdateMutation, SingleValuedMutation, Mutation }
import mypipe.avro.schema.{ AvroSchemaUtils, GenericSchemaRepository }
import mypipe.avro.AvroVersionedRecordSerializer
import mypipe.kafka.KafkaUtil
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

class KafkaMutationSpecificAvroProducer(config: Config)
    extends KafkaMutationAvroProducer[Short](config) {

  private val schemaRepoClientClassName = config.getString("schema-repo-client")

  override protected val schemaRepoClient = Class.forName(schemaRepoClientClassName + "$")
    .getField("MODULE$").get(null)
    .asInstanceOf[GenericSchemaRepository[Short, Schema]]

  override protected val serializer = new AvroVersionedRecordSerializer[InputRecord](schemaRepoClient)

  override def handleAlter(event: AlterEvent): Boolean = {
    // FIXME: if the table is not in the cache already, by it's ID, this will fail
    // refresh insert, update, and delete schemas
    (for (
      i ← schemaRepoClient.getLatestSchema(AvroSchemaUtils.specificSubject(event.database, event.table.name, Mutation.InsertString), flushCache = true);
      u ← schemaRepoClient.getLatestSchema(AvroSchemaUtils.specificSubject(event.database, event.table.name, Mutation.UpdateString), flushCache = true);
      d ← schemaRepoClient.getLatestSchema(AvroSchemaUtils.specificSubject(event.database, event.table.name, Mutation.DeleteString), flushCache = true)
    ) yield {
      true
    }).getOrElse(false)
  }

  override protected def schemaIdToByteArray(s: Short) = Array[Byte](((s & 0xFF00) >> 8).toByte, (s & 0x00FF).toByte)

  override protected def getKafkaTopic(mutation: Mutation): String = KafkaUtil.specificTopic(mutation)

  override protected def avroRecord(mutation: Mutation, schema: Schema): List[GenericData.Record] = {

    Mutation.getMagicByte(mutation) match {
      case Mutation.InsertByte ⇒ insertOrDeleteMutationToAvro(mutation.asInstanceOf[SingleValuedMutation], schema)
      case Mutation.UpdateByte ⇒ updateMutationToAvro(mutation.asInstanceOf[UpdateMutation], schema)
      case Mutation.DeleteByte ⇒ insertOrDeleteMutationToAvro(mutation.asInstanceOf[SingleValuedMutation], schema)
      case _ ⇒
        logger.error(s"Unexpected mutation type ${mutation.getClass} encountered; retuning empty Avro GenericData.Record(schema=$schema")
        List(new GenericData.Record(schema))
    }
  }

  /** Given a mutation, returns the Avro subject that this mutation's
   *  Schema is registered under in the Avro schema repository.
   *
   *  @param mutation mutation to get subject for
   *  @return returns "mutationDbName_mutationTableName_mutationType" where mutationType is "insert", "update", or "delete"
   */
  override protected def avroSchemaSubject(mutation: Mutation): String = AvroSchemaUtils.specificSubject(mutation)

  private def putInRecord(record: GenericData.Record, key: String, value: Any) = value match {
    case v: java.util.Date ⇒ record.put(key, v.getTime)
    case _                 ⇒ record.put(key, value)
  }

  protected def insertOrDeleteMutationToAvro(mutation: SingleValuedMutation, schema: Schema): List[GenericData.Record] = {

    mutation.rows.map(row ⇒ {
      val record = new GenericData.Record(schema)
      row.columns.foreach(col ⇒ Option(schema.getField(col._1)).foreach(f ⇒ putInRecord(record, f.name(), col._2.value)))
      header(record, mutation)
      record
    })
  }

  protected def updateMutationToAvro(mutation: UpdateMutation, schema: Schema): List[GenericData.Record] = {

    mutation.rows.map(row ⇒ {
      val record = new GenericData.Record(schema)
      row._1.columns.foreach(col ⇒ Option(schema.getField("old_" + col._1)).foreach(f ⇒ putInRecord(record, f.name(), col._2.value)))
      row._2.columns.foreach(col ⇒ Option(schema.getField("new_" + col._1)).foreach(f ⇒ putInRecord(record, f.name(), col._2.value)))
      header(record, mutation)
      record
    })
  }
}
