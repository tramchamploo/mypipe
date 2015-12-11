package mypipe.util

import scala.collection.mutable.ArrayBuffer

/** Created by guohang.bao on 15/12/10.
 */
trait Listener {
  def onEvent(evt: Event)
}

trait Listenable {

  val listeners = ArrayBuffer.empty[Listener]

  def addListener(listener: Listener) = listeners += listener

  def notifyAll(evt: Event) = listeners.foreach(_.onEvent(evt))
}

trait Event
