package izolotov.crawler

import java.net.URL
import java.net.http.{HttpClient, HttpRequest, HttpResponse}

import com.google.common.collect.Lists
import crawlercommons.robots.{BaseRobotRules, SimpleRobotRulesParser}
import izolotov.CrawlingQueue
import izolotov.CrawlingQueue.Item
import izolotov.crawler.SuperNewCrawlerApi.{BranchConfigurationBuilder, RobotsRulesExtractor}
import izolotov.crawler.SuperNewPerHostExtractor.RobotsRules

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
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

  case class Configuration[Raw, Doc] (
                                       parallelism: Int,
                                       redirect: Raw => Option[URL],
                                       redirectHandler: URL => Unit,
//                                       robotsRulesExtractor: URL => RobotsRules,
                                       fetcher: URL => URL => Raw,
                                       parser: URL => Raw => Doc,
                                       delay: URL => Long,
                                       redirectPattern: URL => Boolean,
                                       redirectDepth: URL => Int,
                                       robotsTxtPolicy: URL => URL => RobotsRules
                                     )

  case class ConfigurationBuilder[Raw, Doc] (
                                              parallelism: Int,
                                              redirect: Raw => Option[URL],
                                              redirectHandler: URL => Unit,
//                                              robotsRulesExtractor: URL => RobotsRules,
                                              fetcher: URL => Raw,
                                              parser: Raw => Doc,
                                              delay: Long,
                                              redirectPattern: Boolean,
                                              redirectDepth: Int,
                                              robotsTxtPolicy: URL => RobotsRules
                                            ) {
    var getFetcher: PartialFunction[URL, URL => Raw] = {case _ if true => this.fetcher}
    var getParser: PartialFunction[URL, Raw => Doc] = {case _ if true => this.parser}
    var getDelay: PartialFunction[URL, Long] = {case _ if true => this.delay}
    var getRedirectPattern: PartialFunction[URL, Boolean] = {case _ if true => this.redirectPattern}
    var getRedirectDepth: PartialFunction[URL, Int] = {case _ if true => this.redirectDepth}
    var getRobotsTxtPolicy: PartialFunction[URL, URL => RobotsRules] = {case _ if true => this.robotsTxtPolicy}
    def addConf (
                  predicate: URL => Boolean,
                  fetcher: URL => Raw,
                  parser: Raw => Doc,
                  delay: Long,
                  redirectPattern: Boolean,
                  redirectDepth: Int,
                  robotsTxtPolicy: URL => RobotsRules
                ): Unit = {
      if (fetcher != this.fetcher) getFetcher = toPF(predicate, fetcher).orElse(getFetcher)
      if (parser != this.parser) getParser = toPF(predicate, parser).orElse(getParser)
      if (delay != this.delay) getDelay = toPF(predicate, delay).orElse(getDelay)
      if (redirectPattern != this.redirectPattern) getRedirectPattern =
        toPF(predicate, redirectPattern).orElse(getRedirectPattern)
      if (redirectDepth != this.redirectDepth) getRedirectDepth =
        toPF(predicate, redirectDepth).orElse(getRedirectDepth)
      if (robotsTxtPolicy != this.robotsTxtPolicy) getRobotsTxtPolicy =
        toPF(predicate, robotsTxtPolicy).orElse(getRobotsTxtPolicy)
    }

    def build(): Configuration[Raw, Doc] = {
      Configuration[Raw, Doc] (
        parallelism,
        redirect,
        redirectHandler,
//        robotsRulesExtractor,
        getFetcher,
        getParser,
        getDelay,
        getRedirectPattern,
        getRedirectDepth,
        getRobotsTxtPolicy
      )
    }

    private def toPF[A](predicate: URL => Boolean, fn: A): PartialFunction[URL, A] = {
      val pf: PartialFunction[URL, A] = {case url if predicate(url) => fn}
      pf
    }
  }

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

//  object DummyRobotsRulesExtractor {
//    def extract(robotsTxtURL: URL): RobotsRules = {
//      Robo
//    }
//  }

  object RobotsRulesExtractor {
    // TODO specify user agent
    def extract(robotsTxtURL: URL): RobotsRules = {
      val client = HttpClient.newHttpClient();
      val request = HttpRequest.newBuilder()
        .uri(robotsTxtURL.toURI)
        .build();
      val response = client.send(request, HttpResponse.BodyHandlers.ofString())
      val raw = new SimpleRobotRulesParser().parseContent(
        robotsTxtURL.toString,
        response.body().getBytes,
        response.headers().firstValue("Content-Type").orElse("text/plain"),
        Lists.newArrayList("bot")
      )
      RobotsRules(if (raw.getCrawlDelay == BaseRobotRules.UNSET_CRAWL_DELAY) None else Some(raw.getCrawlDelay))
    }
  }

  class ExtractionBuilder(urls: CrawlingQueue) {
    def extract[Raw, Doc](fetcher: URL => Raw, parser: Raw => Doc)(implicit redirect: Raw => Option[URL]): BranchPredicateBuilder[Raw, Doc] = {
      // TODO hardcode
      val redirectHandler: URL => Unit = url => urls.add(url.toString, 1)
      val confBuilder = ConfigurationBuilder(
        10, redirect, redirectHandler, fetcher, parser, 0L, true, 1, RobotsRulesExtractor.extract
      )
      new BranchPredicateBuilder[Raw, Doc](
        urls, confBuilder
      )
    }
  }

  class BranchPredicateBuilder[Raw, Doc](
                                          urls: CrawlingQueue,
                                          builder: ConfigurationBuilder[Raw, Doc]
//                                          builder: ConfBuilder[Raw, Doc]
//                                          fn: PartialFunction[URL, Configuration[Raw, Doc]],
//                                          default: Configuration[Raw, Doc]
                                        ) {
    def foreach[Out, Err](onSuccess: Doc => Out, onErr: Throwable => Err = (exc: Throwable) => throw exc): Unit = {
      implicit val ec = ExecutionContext.global
      val conf: Configuration[Raw, Doc] = builder.build()
      val extractor = new SuperNewPerHostExtractor[Raw, Doc](conf)
      val futures = urls.map{
        item =>
          item.markAsInProgress()
          val url: URL = new URL(item.url())
          val fnExtract: URL => Doc = conf
            .fetcher(url)
            .andThen{
              raw =>
                conf.redirect(raw).foreach(target => if (conf.redirectPattern(url) && conf.redirectDepth(url) < item.depth()) conf.redirectHandler(target))
                raw
            }.andThen(conf.parser(url))
          val future = extractor.extract(url, fnExtract, conf.delay(url))
          future.onComplete{
            res =>
              res.map(onSuccess).recover({case e if true => onErr(e)})
              item.markAsProcessed()
          }
          future
      }
      Await.result(Future.sequence(futures), Duration.Inf)
      extractor.shutdown()
    }
    def when(predicate: URL => Boolean): BranchConfigurationBuilder[Raw, Doc] = {
      new BranchConfigurationBuilder[Raw, Doc](urls, builder, predicate)
    }
  }

  class BranchConfigurationBuilder[Raw, Doc](urls: CrawlingQueue, conf: ConfigurationBuilder[Raw, Doc], predicate: URL => Boolean) {
    def set(
             fetcher: URL => Raw = conf.fetcher,
             parser: Raw => Doc = conf.parser,
             delay: Long = conf.delay,
             redirectPattern: Boolean = conf.redirectPattern,
             redirectDepth: Int = conf.redirectDepth,
             robotsTxtPolicy: URL => RobotsRules = conf.robotsTxtPolicy
           ): BranchPredicateBuilder[Raw, Doc] = {
      conf.addConf(predicate, fetcher, parser, delay, redirectPattern, redirectDepth, robotsTxtPolicy)
//      conf.addConf(predicate, UrlConf[Raw, Doc](
//        fetcher, parser, delay, redirectPattern, redirectDepth
//      ))

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

  def respect(): URL => RobotsRules = url => RobotsRulesExtractor.extract(url)
  def ignore(): URL => RobotsRules = _ => RobotsRules(None)

  def main(args: Array[String]): Unit = {
    NewCrawler
      .read(new CrawlingQueue(Seq(
//        "http://example.com",
//        "http://example.com",
//        "http://example.com",
//        "http://example.net",
//        "http://example.net",
//        "http://example.net",
//        "http://example.org",
//        "http://example.org",
//        "http://example.org",
        "https://www.densurka.ru",
        "https://www.densurka.ru"
      )))
//      .read(new CrawlingQueue(Seq("https://www.densurka.ru", "https://www.densurka.ru", "https://www.densurka.ru")))
      .extract(fetcher = httpFetcher, parser = defaultParser)
      .when(url("http://example.com")).set(delay = 3000L, redirectDepth = 0, parser = anotherParser, robotsTxtPolicy = respect())
      .when(url("http://example.net")).set(delay = 3000L, redirectDepth = 0, parser = anotherParser, robotsTxtPolicy = respect())
      .when(url("http://example.org")).set(delay = 3000L, redirectDepth = 0, parser = anotherParser, robotsTxtPolicy = respect())
      .when(url("https://www.densurka.ru")).set(delay = 3000L, robotsTxtPolicy = respect())
      .foreach(println)
  }


}
