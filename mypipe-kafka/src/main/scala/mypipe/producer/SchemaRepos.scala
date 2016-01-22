package mypipe.producer

import mypipe.api.event.Mutation
import mypipe.avro.schema.{AvroSchema, AvroSchemaUtils, ShortSchemaId}
import mypipe.avro.{ForumPostDelete, ForumPostInsert, ForumPostUpdate, InMemorySchemaRepo}
import org.apache.avro.Schema

/**
  * Created by guohang.bao on 16/1/22.
  */
object ForumPostSchemaRepo extends InMemorySchemaRepo[Short, Schema] with ShortSchemaId with AvroSchema {
  val DATABASE = "medusa"
  val TABLE = "forum_post"
  val insertSchemaId = registerSchema(AvroSchemaUtils.specificSubject(DATABASE, TABLE, Mutation.InsertString), new ForumPostInsert().getSchema)
  val updateSchemaId = registerSchema(AvroSchemaUtils.specificSubject(DATABASE, TABLE, Mutation.UpdateString), new ForumPostUpdate().getSchema)
  val deleteSchemaId = registerSchema(AvroSchemaUtils.specificSubject(DATABASE, TABLE, Mutation.DeleteString), new ForumPostDelete().getSchema)
}