package izolotov.crawler

import java.net.URL
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets
import java.util.concurrent.Executors

import com.google.common.util.concurrent.ThreadFactoryBuilder
import izolotov.FixedDelayModerator
import izolotov.crawler.CrawlerApi.{CrawlerOption, ManagerBuilder}
import izolotov.crawler.CrawlerInput.QueueItem
//import izolotov.Sandbox.{Fetched, HostQueue, QueueItem, SeleniumResp}
import izolotov.crawler.CrawlerApi.{Direct, Redirect, RedirectAnalyzer, Redirectable}
import org.jsoup.Jsoup
import org.jsoup.nodes.{Document, Element}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

object DefaultCrawler {

  implicit val httpClient = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build()

  implicit object JsoupDocBodyHandler extends HttpResponse.BodyHandler[Document] {
    override def apply(responseInfo: HttpResponse.ResponseInfo): HttpResponse.BodySubscriber[Document] = {
      val upstream = HttpResponse.BodySubscribers.ofString(StandardCharsets.UTF_8)
      HttpResponse.BodySubscribers.mapping(
        upstream,
        (html: String) => {
          Jsoup.parse(html)
        }
      )
    }
  }

  implicit object JsoupDocFetchedJsonable extends RedirectAnalyzer[HttpResponse[Document]]{

    import scala.jdk.CollectionConverters._
    val Pattern = ".*url=(.+)\\s*".r

    def analyze(resp: HttpResponse[Document]): Redirectable[HttpResponse[Document]] = {
      val f: PartialFunction[Element, String] = {
        case el if el.attributes().asScala.exists(a => a.getKey == "http-equiv" && a.getValue == "refresh") =>
          el.attributes().get("content")
      }
      // TODO other http codes
      // TODO missing Location header
      // TODO check headrs cas sensitivity
      resp.statusCode match {
        case 302 => Redirect(resp.headers().firstValue("location").get(), resp)
        case _ => {
          resp.body()
            .getElementsByTag("meta")
            .iterator().asScala
            .collectFirst(f)
            .map{
              content =>
                val Pattern(url) = content
                Redirect(url, resp)
            }
            .getOrElse(Direct(resp))
        }
      }
    }
  }

  class HostQueueManagerBuilder() extends ManagerBuilder{
    private var _delay: Long = 0
    def delay: Long = _delay
    def delay_= (delay: Long): Unit = _delay = delay
    override def build(): CrawlingManager = {
      new HostQueueManager(delay, 10)
    }
  }
//  implicit val managerBuilder = new HostQueueManagerBuilder()


  class HostQueueManager(delay: Long, parallelism: Int) extends CrawlingManager {

    case class HostQueue(ec: ExecutionContext, moderator: FixedDelayModerator)

    val map = collection.mutable.Map[String, HostQueue]()

    val threadFactory = new ThreadFactoryBuilder().setDaemon(false).build
    val sharedEC = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(parallelism, threadFactory))
    val onCompleteEC = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1, threadFactory))

    def manage(item: QueueItem, fn: QueueItem => Unit, success: () => Unit, err: Throwable => Unit): Unit = {
      val queue = map.getOrElseUpdate(item.url.getHost, HostQueue(ec(), new FixedDelayModerator(delay)))
      Future {
        val f = Future {
          queue.moderator.apply(item, fn)// extractor.apply(url.toString)
        }(sharedEC)
        f.onComplete {
          case Failure(e) => err.apply(e)
          case Success(_) => success.apply()
        }(onCompleteEC)
        Await.result(f, Duration.Inf)
      }(queue.ec)
    }

    def ofFailure[A](e: Throwable, fn: Throwable => A): Unit = {
      fn.apply(e)
    }

    private def ec(): ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1, threadFactory))
  }

  object HttpFetcher {
    def apply[T]()(implicit bodyHandler: HttpResponse.BodyHandler[T]): HttpFetcher[T] = new HttpFetcher[T](bodyHandler)
  }

  class HttpFetcher[T](bodyHandler: HttpResponse.BodyHandler[T]) {
    def fetch(url: URL)(implicit client: HttpClient): HttpResponse[T] = {
      val request = HttpRequest.newBuilder()
        .uri(url.toURI)
        .build()
      val resp = client.send(request, bodyHandler /*BaseHttpFetcher.CustomBodyHandler*/)
      resp
    }
  }

//  class CrawlerBuilderExt {
//    def delay(ms: Long)
//  }
//
//  class CrawlerExt(b: CrawlerApi.CrawlerBuilder) {
//    def delay(ms: Long): CrawlerApi.CrawlerBuilder = {
//      new CrawlerApi.CrawlerBuilder(b)
//    }
//  }
//  implicit def stringToString(s: CrawlerApi.CrawlerBuilder) = new CrawlerExt(s)
//  class CrawlerExt(val s: CrawlerApi.Crawler) {
//    def increment = s.map(c => (c + 1).toChar)
//  }
  class DefaultManagerBuilder extends ManagerBuilder {
    private var delay: Long = 0
    private var parallelism: Int = 10
    def delay(ms: Long): ManagerBuilder = {
      delay = ms
      this
    }
    def parallelism(threadNum: Int): ManagerBuilder = {
      parallelism = threadNum
      this
    }
    override def build(): CrawlingManager = {
      new HostQueueManager(delay, parallelism)
    }
  }
  implicit val managerBuilder = new DefaultManagerBuilder()

  object Delay extends CrawlerOption[DefaultManagerBuilder, Long] {
    override def apply(managerBuilder: DefaultManagerBuilder, value: Long): ManagerBuilder = {
      managerBuilder.delay(value)
    }
  }
  object Parallelism extends CrawlerOption[DefaultManagerBuilder, Int] {
    override def apply(managerBuilder: DefaultManagerBuilder, value: Int): ManagerBuilder = {
      managerBuilder.parallelism(value)
    }
  }


}
