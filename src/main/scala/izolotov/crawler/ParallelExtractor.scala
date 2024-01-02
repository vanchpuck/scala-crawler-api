package izolotov.crawler

import java.net.URL
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.util.concurrent.{Callable, Executors, LinkedBlockingDeque, ThreadPoolExecutor, TimeUnit}

import ParallelExtractor._
import com.google.common.collect.Lists
import crawlercommons.robots.SimpleRobotRulesParser
import izolotov.CrawlingQueue.Item
import izolotov.{CrawlingQueue, DelayedApplier}
import izolotov.crawler.PerHostExtractor.{DaemonThreadFactory, HostQueueIsFullException, ProcessingQueueIsFullException}
import izolotov.crawler.SuperNewCrawlerApi.Configuration

import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}
import PartialFunction.fromFunction
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

object ParallelExtractor {
  // TODO add initial delay parameter
  class TimeoutException(message: String) extends Exception(message: String)

//  class Result[Doc](doc: Doc, redirect: Option[URL], outLinks: Iterable[URL])

  trait Attmpt[Doc] {
    def handleDoc[Out](fn: Doc => Out): Out
    def handleRedirect(fn: URL => Unit): Unit
  }


  case class Result[Doc](url: String, doc: Try[Doc], redirectTarget: Option[String], outLinks: Iterable[String])

  class ParsingException[Raw](raw: Raw) extends Exception

  class FetchingException() extends Exception

  object Queue {
    def apply(capacity: Int = Int.MaxValue): Queue = new Queue(capacity)
    def apply(): Queue = new Queue()
  }

  class Queue(capacity: Int = Int.MaxValue) {

    private val localEC = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(1, DaemonThreadFactory))
    private val extractionEC = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(1, DaemonThreadFactory))

//    private val extractionExecutor = Executors.newFixedThreadPool(1, DaemonThreadFactory)

    private val applier = new DelayedApplier

    private var isClosed = false

    def extract[Doc](
                      url: URL,
                      fnExtract: URL => Doc,
                      delay: Long = 0L,
                      timeout: Long = 10000L
                    )(implicit ec: ExecutionContext): Future[Doc] = {
      if (isClosed) throw new IllegalStateException("Queue is closed")
      Future {
        val f = Future {
          try
            Await.result(
              Future{applier.apply(url, fnExtract, delay)}(extractionEC),
              Duration.apply(timeout, TimeUnit.MILLISECONDS)
            )
          catch {
            case _: concurrent.TimeoutException =>
              throw new TimeoutException(s"URL ${url.toString} could not be extracted within ${timeout} milliseconds timeout")
          }

        }(ec)
        // TODO add timeout parameter along with delay
        Await.result(f, Duration.Inf)
      }(localEC)
    }

    def close(): Unit = {
      isClosed = true
      ParallelExtractor.shutdown(localEC)
      ParallelExtractor.shutdown(extractionEC)
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

//  case class Result[Doc](doc: Doc, redirectTarget: Option[String])

  def getRobotsTxtURL: URL => URL = url => new URL(s"${url.getProtocol}://${url.getHost}/robots.txt")
}

class ParallelExtractor[Raw, Doc](conf: Configuration[Raw, Doc]) {

  object ExtractionTask {
    def start(url: URL): Result[Doc] = {
      val raw = Try(conf.fetcher(url)).recover{case _ if true => throw new FetchingException}
      val redirectTarget: Option[String] = raw.toOption.map(conf.redirect).flatten
      val doc = raw.map(r => conf.parser(url)(r))
      Result[Doc](url.toString, doc, redirectTarget, Seq.empty)
    }
  }

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

  val hostMap = collection.mutable.Map[String, (Queue, RobotsRules)]()

  def extract(url: String): Future[Result[Doc]] = {
    Try(new URL(url)) match {
      case Success(u) => {
        val hostKit = hostMap.getOrElseUpdate(
          u.getHost(),
          {
            val queue = new Queue()
            val rules = Await.result(new Queue().extract(getRobotsTxtURL(u), conf.fetcher.andThen(conf.robotsHandler)), Duration.Inf)
            (queue, rules)
          }
        )
        hostKit._1.extract(u, ExtractionTask.start, hostKit._2.delay.getOrElse(conf.delay(u)))
      }
      case Failure(exc) => Future.fromTry(Success(Result(url, Failure(exc), None, Seq.empty)))
    }
  }

//  private def extractResult(url: URL): Result[Doc] = {
//    val raw = Try(conf.fetcher(url)).recover{case _ if true => throw new FetchingException}
//    val redirectTarget: Option[String] = raw.toOption.map(conf.redirect).flatten
//    val doc = raw.map(r => conf.parser(url)(r))
//    Result[Doc](url.toString, doc, redirectTarget, Seq.empty)
//  }

  def extract(url: URL, fn: URL => Doc): Future[Doc] = {
    val hostKit = hostMap.getOrElseUpdate(
      url.getHost(),
      (new Queue, Await.result(new Queue().extract(getRobotsTxtURL(url), conf.fetcher.andThen(conf.robotsHandler)), Duration.Inf))
    )
//    val fnExtract: URL => Doc = conf
//      .fetcher
////      .andThen{raw => conf.redirect(raw).foreach(target => conf.redirectHandler(target)); raw}.andThen(conf.parser(url))
//      .andThen{raw => raw}.andThen(conf.parser(url))
//    hostKit._1.extract(url, fnExtract, hostKit._2.delay.getOrElse(conf.delay(url)))
    hostKit._1.extract(url, fn, hostKit._2.delay.getOrElse(conf.delay(url)))
  }

//  def extract(crawlingQueue: CrawlingQueue): Iterator[Future[Doc]] = {
//    crawlingQueue.map{
//      item =>
//        val url = new URL(item.url)
//        val hostKit = hostMap.getOrElseUpdate(
//          url.getHost(),
//          (new Queue, Await.result(new Queue().extract(getRobotsTxtURL(url), conf.fetcher.andThen(conf.robotsHandler)), Duration.Inf))
//        )
//        val fnExtract: URL => Doc = conf
//          .fetcher
//          .andThen{raw => conf.redirect(raw).foreach(target => crawlingQueue.add(target.toString, item.depth + 1)); raw}
//          .andThen(conf.parser(url))
//        hostKit._1.extract(url, fnExtract, hostKit._2.delay.getOrElse(conf.delay(url)))
//    }
//  }

//  def extract(url: URL, fn: URL => Doc, delay: Long): Future[Doc] = {
//    val hostKit = hostMap.getOrElseUpdate(
//      url.getHost(),
//      (new Queue[Doc], Await.result(new Queue[RobotsRules]().extract(getRobotsTxtURL(url), conf.robotsTxtPolicy), Duration.Inf))
//    )
//    val future = hostKit._1.extract(url, fn, hostKit._2.delay.getOrElse(delay))
//    future
//  }

  def shutdown(): Unit = {
    ParallelExtractor.shutdown(ec)
  }

}
