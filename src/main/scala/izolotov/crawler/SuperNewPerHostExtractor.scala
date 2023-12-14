package izolotov.crawler

import java.net.URL
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.util.concurrent.{LinkedBlockingDeque, ThreadPoolExecutor, TimeUnit}

import SuperNewPerHostExtractor._
import com.google.common.collect.Lists
import crawlercommons.robots.SimpleRobotRulesParser
import izolotov.{CrawlingQueue, DelayedApplier}
import izolotov.crawler.PerHostExtractor.{DaemonThreadFactory, HostQueueIsFullException, ProcessingQueueIsFullException, Queue}
import izolotov.crawler.SuperNewCrawlerApi.Configuration

import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}
import PartialFunction.fromFunction
import scala.concurrent.duration.Duration

object SuperNewPerHostExtractor {
  // TODO add initial delay parameter
  class Queue[Out](capacity: Int = Int.MaxValue) {

    private val localEC = ExecutionContext.fromExecutorService(new ThreadPoolExecutor(
      1,
      1,
      0L,
      TimeUnit.MILLISECONDS,
      new LinkedBlockingDeque[Runnable](capacity),
      DaemonThreadFactory,
      (r: Runnable, e: ThreadPoolExecutor) => {
        throw new HostQueueIsFullException(s"Task ${r.toString()} rejected from ${e.toString()}")
      }
    ))

    private val applier = new DelayedApplier

    def extract(url: URL, fnExtract: URL => Out, delay: Long)(implicit ec: ExecutionContext): Future[Out] = {
      println(delay)
      Future {
        val f = Future {
          applier.apply(url, fnExtract, delay)
        }(ec)
        // TODO add timeout parameter along with delay
        Await.result(f, Duration.Inf)
      }(localEC)
    }
  }

  private def shutdown(executor: ExecutionContextExecutorService): Unit = {
    executor.shutdown
    try
      if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
        executor.shutdownNow
      }
    catch {
      case _: InterruptedException => {
        executor.shutdownNow
        Thread.currentThread.interrupt()
      }
    }
  }

  case class RobotsRules(delay: Option[Long])

  def getRobotsTxtURL: URL => URL = url => new URL(s"${url.getProtocol}://${url.getHost}/robots.txt")
}

class SuperNewPerHostExtractor[Raw, Doc](conf: Configuration[Raw, Doc]) {

  // TODO remove me
//  val processingQueueCapacity = 10

  implicit val ec = ExecutionContext.fromExecutorService(new ThreadPoolExecutor(
    conf.parallelism,
    conf.parallelism,
    0L,
    TimeUnit.MILLISECONDS,
    new LinkedBlockingDeque[Runnable](conf.queueLength),
    DaemonThreadFactory,
    (r: Runnable, e: ThreadPoolExecutor) => {
      // TODO block instead of throw an exception
      throw new ProcessingQueueIsFullException(s"Task ${r.toString()} rejected from ${e.toString()}")
    }
  ))

  val hostMap = collection.mutable.Map[String, (Queue[Doc], RobotsRules)]()

  def extract(url: URL, fn: URL => Doc, delay: Long): Future[Doc] = {
    val hostKit = hostMap.getOrElseUpdate(
      url.getHost(),
      (new Queue[Doc], Await.result(new Queue[RobotsRules]().extract(getRobotsTxtURL(url), conf.robotsTxtPolicy), Duration.Inf))
    )
    val future = hostKit._1.extract(url, fn, hostKit._2.delay.getOrElse(delay))
    future
  }

  def shutdown(): Unit = {
    SuperNewPerHostExtractor.shutdown(ec)
  }

}
