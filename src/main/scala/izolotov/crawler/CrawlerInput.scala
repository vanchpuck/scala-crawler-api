package izolotov.crawler

import java.net.URL

import scala.collection.mutable

object CrawlerInput {

  trait Input {
    def url(): String
    def onPickUp(): Unit
    def onSuccess(): Unit
    def onFailure(throwable: Throwable): Unit
  }

  case class InputItem(url: String) extends Input{
    def onPickUp(): Unit = {
      println(s"${url.toString} picked up")
    }

    def onSuccess(): Unit = {
      println(s"${url.toString} success")
    }

    def onFailure(throwable: Throwable): Unit = {
      println(s"${url.toString} failure")
    }
  }

  case class QueueItem(inputItem: Input, url: URL, depth: Int)

  class CrawlingQueue(data: Iterable[String]) extends Iterator[String]{
    val queue: mutable.Queue[String] = mutable.Queue()
    queue ++= data

    def add(url: String): Unit = {
      queue += url
    }

    override def hasNext: Boolean = {
      val a = queue.length > 0
      //      println(s"hasNext: ${a}")
      queue.length > 0
    }

    override def next(): String = {
      //      println("next")
      queue.dequeue()
    }
  }
}
