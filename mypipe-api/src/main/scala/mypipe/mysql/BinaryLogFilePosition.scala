package mypipe.mysql

import scala.util.Try

case class BinaryLogFilePosition(filename: String, pos: Long) {
  override def toString(): String = s"$filename:$pos"
  override def equals(o: Any): Boolean = {
    o != null &&
      filename.equals(o.asInstanceOf[BinaryLogFilePosition].filename) &&
      pos.equals(o.asInstanceOf[BinaryLogFilePosition].pos)
  }
}

object BinaryLogFilePosition {
  val current = BinaryLogFilePosition("", 0)

  def fromString(saved: String) = Try {
    val parts = saved.split(":")
    BinaryLogFilePosition(parts(0), parts(1).toLong)
  }.toOption

}