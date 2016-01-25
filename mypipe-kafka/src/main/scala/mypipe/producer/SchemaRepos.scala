package mypipe.producer

import mypipe.api.event.Mutation
import mypipe.avro.schema.{AvroSchema, AvroSchemaUtils, ShortSchemaId}
import mypipe.avro._
import org.apache.avro.Schema

/**
  * Created by guohang.bao on 16/1/22.
  */
object ForumPostSchemaRepo extends InMemorySchemaRepo[Short, Schema] with ShortSchemaId with AvroSchema {
  val DATABASE = "medusa"

  for (i <- 0 until 20) {
    registerSchema(AvroSchemaUtils.specificSubject(DATABASE, s"forum_post_p$i", Mutation.InsertString), new ForumPostInsert().getSchema)
    registerSchema(AvroSchemaUtils.specificSubject(DATABASE, s"forum_post_p$i", Mutation.UpdateString), new ForumPostUpdate().getSchema)
    registerSchema(AvroSchemaUtils.specificSubject(DATABASE, s"forum_post_p$i", Mutation.DeleteString), new ForumPostDelete().getSchema)
  }

}