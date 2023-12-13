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
  class Queue[Out](
//                    fnExtract: URL => Doc,
//                    delay: Long,
                    capacity: Int = Int.MaxValue
                  ) {

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
}

class SuperNewPerHostExtractor[Raw, Doc](conf: Configuration[Raw, Doc]) {

  // TODO remove me
  val processingQueueCapacity = 10

  implicit val ec = ExecutionContext.fromExecutorService(new ThreadPoolExecutor(
    conf.parallelism,
    conf.parallelism,
    0L,
    TimeUnit.MILLISECONDS,
    new LinkedBlockingDeque[Runnable](processingQueueCapacity),
    DaemonThreadFactory,
    (r: Runnable, e: ThreadPoolExecutor) => {
      // TODO block instead of throw an exception
      throw new ProcessingQueueIsFullException(s"Task ${r.toString()} rejected from ${e.toString()}")
    }
  ))

//  val hostMap = collection.mutable.Map[String, Queue[Doc]]()
  val hostRobotsRules = collection.mutable.Map[String, RobotsRules]()
  val q = new Queue[Doc](10)
  def extract(url: URL): Future[Doc] = {
    val fnExtractRules: URL => RobotsRules = {
      url =>
        val client = HttpClient.newHttpClient();
        val request = HttpRequest.newBuilder()
          .uri(url.toURI)
          .build();
        val response = client.send(request, HttpResponse.BodyHandlers.ofString())
        val raw = new SimpleRobotRulesParser().parseContent(
          url.toString,
          response.body().getBytes,
          response.headers().firstValue("Content-Type").orElse("text/plain"),
          Lists.newArrayList("bot")
        )
        println(raw)
        RobotsRules(Some(raw.getCrawlDelay))
    }
    val rulesFuture = new Queue[RobotsRules].extract(
      new URL(s"${url.getProtocol}://${url.getHost}/robots.txt"),
      fnExtractRules
    )
    val rules = Await.result(rulesFuture, Duration.Inf)
    println(rules)
    val fnExtract: URL => Doc = conf
      .fetcher(url)
      .andThen{
        raw =>
          conf.redirect(raw).foreach(target => if (conf.redirectPattern(url) /*&& conf.redirectDepth(url) < item.depth()*/) conf.redirectHandler(target))
          raw
      }.andThen(conf.parser(url))
    val future = q.extract(url, fnExtract, rules.delay.getOrElse(conf.delay(url)))
    future
  }

  def shutdown(): Unit = {
    SuperNewPerHostExtractor.shutdown(ec)
  }

}
