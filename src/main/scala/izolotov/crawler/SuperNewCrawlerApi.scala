package izolotov.crawler

import java.net.URL
import java.net.http.HttpResponse

import izolotov.CrawlingQueue
import izolotov.CrawlingQueue.Item
import izolotov.crawler.SuperNewCrawlerApi.{BranchConfigurationBuilder}
//import izolotov.crawler.NewCrawlerApi.Configuration
import org.jsoup.nodes.Document

import scala.collection.mutable

object SuperNewCrawlerApi {

  class Context() {
    private var _queue: CrawlingQueue = null
    def queue = _queue
    def queue (urls: CrawlingQueue) = _queue = urls
  }

  object NewCrawler {
    def read(urls: CrawlingQueue): ExtractionBuilder = {
      new ExtractionBuilder(urls)
    }

  }

  //  val DummyParser = Raw => Doc =
  //  object ExtractionBuilder {
  //
  //  }

  val DefaultDelay = 0L
  val DefaultRedirectDepth = 1

  case class GlobalConf[Raw](
                              parallelism: Int,
                              redirect: Raw => Option[URL]
                            )

  case class UrlConf[Raw, Doc](
                                fetcher: URL => Raw,
                                parser: Raw => Doc,
                                delay: Long,
                                redirectPattern: URL => Boolean,
                                redirectDepth: Int
                              )

  case class ConfBuilder[Raw, Doc](
                                  global: GlobalConf[Raw],
                                  default: UrlConf[Raw, Doc]
                             ) {
    var getConf: PartialFunction[URL, UrlConf[Raw, Doc]] = {case _ if true => default}
    def addConf(predicate: URL => Boolean, conf: UrlConf[Raw, Doc]): ConfBuilder[Raw, Doc] = {
      val pf: PartialFunction[URL, UrlConf[Raw, Doc]] = {case url if predicate(url) => conf}
      getConf = pf.orElse(getConf)
      this
    }
    def conf(url: URL): UrlConf[Raw, Doc] = {
      getConf(url)
    }
  }


  class ExtractionBuilder(urls: CrawlingQueue) {
    def extract[Raw, Doc](fetcher: URL => Raw, parser: Raw => Doc)(implicit redirect: Raw => Option[URL]): BranchPredicateBuilder[Raw, Doc] = {
      new BranchPredicateBuilder[Raw, Doc](
        urls, new ConfBuilder[Raw, Doc](GlobalConf(10, redirect), UrlConf(fetcher, parser, 0L, _ => true, 0))
      )
    }
  }

  class BranchPredicateBuilder[Raw, Doc](
                                          urls: CrawlingQueue,
                                          builder: ConfBuilder[Raw, Doc]
//                                          fn: PartialFunction[URL, Configuration[Raw, Doc]],
//                                          default: Configuration[Raw, Doc]
                                        ) {
    def foreach[Out](fn: Doc => Out): Unit = {
      urls.foreach { item =>
        val url = new URL(item.url())
        val conf = builder.getConf(url)
//        val fetcher: URL => Raw = builder.fetcher.applyOrElse(url, default.fetcher)
//        val parser: Raw => Doc = builder.parser.applyOrElse(url, default.parser)
//        val delay: Long = builder.delay.applyOrElse(url, _ => default.delay)
//        val redirectPattern: URL => Boolean = builder.redirectPattern.applyOrElse(url, default.redirectPattern)
//        val redirectDepth: Int = builder.redirectDepth.applyOrElse(url, _ => default.redirectDepth)
        conf.fetcher.andThen{raw =>
          println(conf.delay)
          if (item.depth() < conf.redirectDepth) builder.global.redirect.apply(raw).map(target => urls.add(target.toString, item.depth()))
          conf.parser(raw)
        }.andThen(fn).apply(new URL(item.url()))
      }
    }
    def when(predicate: URL => Boolean): BranchConfigurationBuilder[Raw, Doc] = {
      new BranchConfigurationBuilder[Raw, Doc](urls, builder, predicate)
    }
  }

  class BranchConfigurationBuilder[Raw, Doc](urls: CrawlingQueue, conf: ConfBuilder[Raw, Doc], predicate: URL => Boolean) {
    def set(
             fetcher: URL => Raw = conf.default.fetcher,
             parser: Raw => Doc = conf.default.parser,
             delay: Long = conf.default.delay,
             redirectPattern: URL => Boolean = conf.default.redirectPattern,
             redirectDepth: Int = conf.default.redirectDepth
           ): BranchPredicateBuilder[Raw, Doc] = {
      conf.addConf(predicate, UrlConf[Raw, Doc](
        fetcher, parser, delay, redirectPattern, redirectDepth
      ))

      new BranchPredicateBuilder[Raw, Doc](urls, conf)
    }
  }







  import izolotov.crawler.DefaultCrawler._
  implicit val httpFetcher: Function[URL, HttpResponse[Document]] = HttpFetcher().fetch
  implicit val defaultParser: HttpResponse[Document] => String = resp => "resp from: " + resp.uri().toString
  implicit val anotherParser: HttpResponse[Document] => String = resp => "body: " + resp.body().html().substring(0, 10)
  implicit val redirectExtractor: HttpResponse[Document] => Option[URL] = {
    resp => {
      if (resp.uri().toString() == "http://example.com")
        Some(new URL("http://redirect.com"))
      else
        None
    }
  }

  def main(args: Array[String]): Unit = {
    NewCrawler
      .read(new CrawlingQueue(Seq("http://example.com", "http://example.net")))
      .extract(fetcher = httpFetcher, parser = defaultParser)
      .when(url("http://example.com")).set(delay = 100L, redirectDepth = 1, parser = anotherParser)
      .foreach(println)
  }


}
