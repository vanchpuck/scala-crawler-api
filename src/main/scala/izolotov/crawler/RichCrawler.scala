package izolotov.crawler
import java.net.URL
import java.util.concurrent.Executors

import com.google.common.util.concurrent.ThreadFactoryBuilder
import izolotov.FixedDelayModerator

import scala.concurrent.{Await, ExecutionContext, Future}
import RichCrawler._
import izolotov.crawler.CrawlerInput.QueueItem

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

object RichCrawler {
  case class HostQueue(ec: ExecutionContext, moderator: FixedDelayModerator)
}

class RichCrawler extends RichManager with HasDelay {

  private var _printer: PartialFunction[URL, Boolean] = _ => false
  private var _extractor: PartialFunction[URL, URL => Unit] = {case _ if true => _ => println(1)}

  val map = collection.mutable.Map[String, HostQueue]()

  val threadFactory = new ThreadFactoryBuilder().setDaemon(false).build
  val sharedEC = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10, threadFactory))
  val onCompleteEC = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10, threadFactory))


  override def manage(url: QueueItem): Unit = {
    val queue = map.getOrElseUpdate(url.url.getHost, HostQueue(ec(), new FixedDelayModerator(delay.apply(url.url))))
    if(_printer.apply(url.url)) {
      println("Hi!")
    }
    Future {
      val f = Future {
        queue.moderator.apply(url.url, _extractor.apply(url.url))// extractor.apply(url.toString)
      }(sharedEC)
      f.onComplete {
        case Failure(e) => url.inputItem.onFailure(e)
        case Success(_) => url.inputItem.onSuccess()
      }(onCompleteEC)
      Await.result(f, Duration.Inf)
    }(queue.ec)
  }

  override def setExtractor(extractor: PartialFunction[URL, URL => Unit]): Unit = {
    this._extractor = extractor.orElse(this._extractor)
  }

  def setDummy(dummy: PartialFunction[URL, Boolean]): Unit = {
    this._printer = dummy.orElse(_printer)
  }

  private def ec(): ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1, threadFactory))

}
