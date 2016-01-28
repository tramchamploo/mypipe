package mypipe.avro

import mypipe.api.event.Mutation
import mypipe.avro.schema.{AvroSchema, AvroSchemaUtils, ShortSchemaId}
import org.apache.avro.Schema

/** Created by guohang.bao on 16/1/22.
 */
object ForumPostSchemaRepo extends InMemorySchemaRepo[Short, Schema] with ShortSchemaId with AvroSchema {
  val DATABASE = "medusa"

  val insert = new ForumPostInsert()
  val update = new ForumPostUpdate()
  val delete = new ForumPostDelete()

  for (i ← 0 until 20) {
    registerSchema(AvroSchemaUtils.specificSubject(DATABASE, s"forum_post_p$i", Mutation.InsertString), insert.getSchema)
    registerSchema(AvroSchemaUtils.specificSubject(DATABASE, s"forum_post_p$i", Mutation.UpdateString), update.getSchema)
    registerSchema(AvroSchemaUtils.specificSubject(DATABASE, s"forum_post_p$i", Mutation.DeleteString), delete.getSchema)
  }

  registerSchema(AvroSchemaUtils.specificSubject(DATABASE, s"forum_post", Mutation.InsertString), insert.getSchema)
  registerSchema(AvroSchemaUtils.specificSubject(DATABASE, s"forum_post", Mutation.UpdateString), update.getSchema)
  registerSchema(AvroSchemaUtils.specificSubject(DATABASE, s"forum_post", Mutation.DeleteString), delete.getSchema)

}