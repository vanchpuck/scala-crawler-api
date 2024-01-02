package izolotov.crawler

import java.net.{CookieManager, URL}
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.Charset

import com.google.common.collect.Lists
import crawlercommons.robots.{BaseRobotRules, SimpleRobotRulesParser}
import izolotov.CrawlingQueue
import izolotov.CrawlingQueue.Item
import izolotov.crawler.SuperNewCrawlerApi.{BranchConfigurationBuilder, RobotsRulesExtractor}
import izolotov.crawler.ParallelExtractor.{Queue, Result, RobotsRules, getRobotsTxtURL}
import org.apache.hc.core5.http.ContentType
import org.jsoup.Jsoup

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
//import izolotov.crawler.NewCrawlerApi.Configuration
import org.jsoup.nodes.Document

import scala.collection.mutable

object SuperNewCrawlerApi {

  class ExtractionProcess[Raw, Doc](conf: Configuration[Raw, Doc]) {


    class RobotsRulesExtractor(fetcher: URL => Raw, robotsTxtParser: Raw => RobotsRules) {
      implicit val executionContext = ExecutionContext.global
      def extract(url: URL): RobotsRules = {
        Await.result(new Queue().extract(url, fetcher.andThen(conf.robotsHandler)), Duration.Inf)
      }
    }
  }

  object Spec {
    def apply[Raw, Doc](url: URL, conf: Configuration[Raw, Doc], robotsRules: RobotsRules): Spec[Raw, Doc] = {
      new Spec[Raw, Doc](url, conf, robotsRules)
    }
  }

  class Spec[Raw, Doc](url: URL, conf: Configuration[Raw, Doc], robotsRules: RobotsRules) {
    def url(): URL = url
    def delay(): Long = if (conf.robotsTxtPolicy(url)) robotsRules.delay.getOrElse(conf.delay(url)) else conf.delay(url)
    def extractionFn(): URL => Doc = {
      conf.fetcher
        .andThen{raw => conf.redirect(raw); raw}
        .andThen(conf.parser(url))
    }
  }

  class HostCache(getRules: URL => RobotsRules, getQueue: () => Queue) {
    val hostMap = collection.mutable.Map[String, (Queue, RobotsRules)]()
    def getOrElseUpdate(url: URL): (Queue, RobotsRules) = {
      hostMap.getOrElseUpdate(
        url.getHost,
        (getQueue(), getRules(new URL(s"${url.getProtocol}://${url.getHost}/robots.txt")))
      )
    }
    def getQueue(url: URL): Option[Queue] = {
      hostMap.get(url.getHost).map(value => value._1)
    }
    def getRobotsRules(url: URL): Option[RobotsRules] = {
      hostMap.get(url.getHost).map(value => value._2)
    }
    def update(url: URL): HostCache = {
      hostMap(url.getHost) = (getQueue(), getRules(url))
      this
    }
  }

  class ExtractionManager[Raw, Doc](conf: Configuration[Raw, Doc]) {
    val extractRulesFn: URL => RobotsRules = conf.fetcher.andThen(conf.robotsHandler)
    val createQueueFn: () => Queue = Queue.apply

    val hostCache = new HostCache(extractRulesFn, createQueueFn)

    implicit val ec = ExecutionContext.global

//    def extract(url: URL): Att[Raw, Doc] = {
//      val (queue, robotsRules) = hostCache.getOrElseUpdate(url)
//      Att(queue, Spec(url, conf, robotsRules))
//    }
//    def extract(item: Item): Future[Doc] = {
//
//    }
    def extract(url: URL): Future[Doc] = {
      val (queue, robotsRules) = hostCache.getOrElseUpdate(url)
      val spec = Spec(url, conf, robotsRules)
      queue.extract(url, spec.extractionFn(), spec.delay())
    }
  }

//  class HostKit[Raw](redirector: Raw => Option[URL], queueLength: Int)(implicit executionContext: ExecutionContext) {
//    val hostMap = collection.mutable.Map[String, (Queue, RobotsRules)]()
//    def get(url: URL): (Queue, RobotsRules) = {
//      hostMap.getOrElseUpdate(
//        url.getHost(),
//        {
//          val queue = new Queue(queueLength)
//          val rules = Await.result(new Queue().extract(getRobotsTxtURL(url), conf.fetcher.andThen(conf.robotsHandler)), Duration.Inf)
//          (queue, rules)
//        }
//        //          (new Queue, Await.result(new Queue().extract(getRobotsTxtURL(url), conf.fetcher.andThen(conf.robotsHandler)), Duration.Inf))
//      )
//    }
//  }

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
                                       redirect: Raw => Option[String],
//                                       redirectHandler: URL => Unit,
                                       robotsHandler: Raw => RobotsRules,
                                       queueLength: Int,
                                       fetcher: URL => Raw,
                                       parser: URL => Raw => Doc,
                                       delay: URL => Long,
                                       timeout: URL => Long,
                                       redirectPolicy: URL => Int,
                                       robotsTxtPolicy: URL => Boolean
                                     )

  case class ConfigurationBuilder[Raw, Doc] (
                                              parallelism: Int,
                                              redirect: Raw => Option[String],
//                                              redirectHandler: URL => Unit,
                                              robotsHandler: Raw => RobotsRules,
                                              queueLength: Int,
                                              fetcher: URL => Raw,
                                              parser: Raw => Doc,
                                              delay: Long,
                                              timeout: Long,
                                              redirectPolicy: URL => Int,
                                              robotsTxtPolicy: Boolean
                                            ) {
    var getFetcher: PartialFunction[URL, Raw] = {case url if true => this.fetcher(url)}
    var getParser: PartialFunction[URL, Raw => Doc] = {case _ if true => this.parser}
    var getDelay: PartialFunction[URL, Long] = {case _ if true => this.delay}
    var getTimeout: PartialFunction[URL, Long] = {case _ if true => this.timeout}
    var getRedirectPolicy: PartialFunction[URL, Int] = {case url if true => this.redirectPolicy(url)}
    var getRobotsTxtPolicy: PartialFunction[URL, Boolean] = {case url if true => this.robotsTxtPolicy}
    def addConf (
                  predicate: URL => Boolean,
                  fetcher: URL => Raw,
                  parser: Raw => Doc,
                  delay: Long,
                  redirectPolicy: URL => Int,
                  robotsTxtPolicy: Boolean
                ): Unit = {
      // FIXME
      val fnRedirectPolicy: PartialFunction[URL, Int] = {case url if predicate(url) => redirectPolicy(url)}
//      val fnRobotsTxtPolicy: PartialFunction[URL, Boolean] = {case url if predicate(url) => robotsTxtPolicy(url)}
      val fnFetcher: PartialFunction[URL, Raw] = {case url if predicate(url) => fetcher(url)}
      if (fetcher != this.fetcher) getFetcher = fnFetcher.orElse(getFetcher)
      if (parser != this.parser) getParser = toPF(predicate, parser).orElse(getParser)
      if (delay != this.delay) getDelay = toPF(predicate, delay).orElse(getDelay)
      if (robotsTxtPolicy != this.robotsTxtPolicy) getRobotsTxtPolicy = toPF(predicate, robotsTxtPolicy).orElse(getRobotsTxtPolicy)
      if (timeout != this.timeout) getTimeout = toPF(predicate, timeout).orElse(getTimeout)
      println(redirectPolicy != this.redirectPolicy)
      if (redirectPolicy != this.redirectPolicy) getRedirectPolicy =
        fnRedirectPolicy.orElse(getRedirectPolicy)
//      if (robotsTxtPolicy != this.robotsTxtPolicy) getRobotsTxtPolicy =
//        fnRobotsTxtPolicy.orElse(getRobotsTxtPolicy)
    }

    def build(): Configuration[Raw, Doc] = {
      println("Test")
      println(getRedirectPolicy(new URL("http://example.com")))
      Configuration[Raw, Doc] (
        parallelism,
        redirect,
//        redirectHandler,
        robotsHandler,
        queueLength,
        getFetcher,
        getParser,
        getDelay,
        getTimeout,
        getRedirectPolicy,
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

//  object ExtractionBuilder {
//    val DefaultRedirectPolicy: URL => Int = _ => 0
//  }

//  sealed trait Attmpt[Doc]

  class ExtractionBuilder(urls: CrawlingQueue) {
    def extractWith[Raw, Doc](
                               fetcher: URL => Raw,
                               parser: Raw => Doc,
                               parallelism: Int = 10,
                               queueLength: Int = Int.MaxValue,
                               delay: Long = 0L,
                               timeout: Long = 30000L,
                               redirectPolicy: URL => Int = _=> 0,
                               robotsTxtPolicy: Boolean = true
                             )(implicit redirect: Raw => Option[String], robots: Raw => RobotsRules): BranchPredicateBuilder[Raw, Doc] = {
      // TODO hardcode
//      val redirectHandler: URL => Unit = url => urls.add(url.toString, 1)
      val confBuilder = ConfigurationBuilder(
        parallelism,
        redirect,
//        redirectHandler,
        robots,
        queueLength,
        fetcher,
        parser,
        delay,
        timeout,
        redirectPolicy,
        robotsTxtPolicy
      )
      new BranchPredicateBuilder[Raw, Doc](
        urls, confBuilder
      )
    }
  }

  class BranchPredicateBuilder[Raw, Doc](
                                          urls: CrawlingQueue,
                                          builder: ConfigurationBuilder[Raw, Doc]
                                        ) {
    // FIXME get rid of global
    implicit val ec = ExecutionContext.global

    def foreach[Err](onSuccess: Doc => Unit, onErr: Throwable => Err = (exc: Throwable) => throw exc): Unit = {
      foreachRaw(res => res.doc)(doc => doc.map(onSuccess).recover{case e if true => onErr(e)})

    }

    def foreach(fn: Result[Doc] => Unit): Unit = {
      foreachRaw(res => res)(fn)
    }

    private def foreachRaw[A](get: Result[Doc] => A)(fn: A => Unit): Unit = {
      val conf: Configuration[Raw, Doc] = builder.build()
      val extractor = new ParallelExtractor[Raw, Doc](conf)
      val futures = urls
        .map{
          item =>
            item.markAsInProgress()
            val future = extractor.extract(item.url)
              .map{
                res =>
                  val targetRedirect = res.redirectTarget
                    .flatMap(target => Try(new URL(target)).toOption)
                    .filter(target => item.depth < conf.redirectPolicy(target))
                    .map{target => urls.add(target.toString, item.depth + 1); target}
                    .map(u => u.toString)
                  println(targetRedirect)
                  Result(res.url+"1", res.doc, targetRedirect, Seq.empty)
              }
            future.onComplete{
              t =>
                t match {
                  case Success(result) => {
                    try
                      fn(get(result))
                    finally
                      item.markAsProcessed()
                  }
                }
            }
            future
        }
      Await.result(Future.sequence(futures), Duration.Inf)
    }

//    def foreach(fn: Result[Doc] => Unit): Unit = {
//      val conf: Configuration[Raw, Doc] = builder.build()
//      val extractor = new ParallelExtractor[Raw, Doc](conf)
//      val futures = urls
//        .map{
//          item =>
//            item.markAsInProgress()
//            val future = extractor.extract(item.url)
//            .map{
//              res =>
//                val targetRedirect = res.redirectTarget
//                  .flatMap(target => Try(new URL(target)).toOption)
//                  .filter(target => item.depth < conf.redirectPolicy(target))
//                  .map{target => urls.add(target.toString, item.depth + 1); target}
//                  .map(u => u.toString)
////                  .map(toString)
//                println(targetRedirect)
//                Result(res.url+"1", res.doc, targetRedirect, Seq.empty)
////                fn(res)
//            }
//            future.onComplete{
//              t =>
////                t.map{
////                  res =>
////                    res.redirectTarget
////                      .map(target => Try(new URL(target)).toOption)
////                      .flatten
////                      .filter(target => item.depth < conf.redirectPolicy(target))
////                      .map{target => urls.add(target.toString, item.depth + 1); target}
////                    fn(res)
////                }
//                fn(t.get)
//                item.markAsProcessed()
//            }
//            future
//        }
//      Await.result(Future.sequence(futures), Duration.Inf)
//    }



//    def foreach[Err](onSuccess: Doc => Unit, onErr: Throwable => Err = (exc: Throwable) => throw exc): Unit = {
//      implicit val ec = ExecutionContext.global
//      val conf: Configuration[Raw, Doc] = builder.build()
//      val extractor = new ParallelExtractor[Raw, Doc](conf)
////      Future.from
//      val futures = urls
//        .map{
//          item =>
//            val t: Try[Future[ParallelExtractor.Result[Doc]]] = Try(new URL(item.url))
//              .map{u => item.markAsInProgress(); u}
//              .map(u => extractor.extract(u))
//            val future = Future.fromTry(t).flatten
//            future.onComplete{
//              t match {
//                case Success(a) => a.map(res => onSuccess(res.)).re
//              }
//            }
////              .on
//
//
//
//
////              .map{ftr => ftr.onComplete{
////                ftr =>
////                  ftr.foreach{
////                    result =>
////                      result
////                        .redirectTarget
////                        .map(target => Try(new URL(target)).toOption)
////                        .flatten
////                        .filter(target => item.depth < conf.redirectPolicy(target))
////                        .foreach(target => urls.add(target.toString, item.depth + 1))
////                  }
////                  ftr.map(result => result.doc)
////                    .foreach(t => onSuccess(t.get))
//////                    .map(t => onSuccess(t.get))
//////                    .recover{case e if true => onErr(e)}
////                  item.markAsProcessed()
////              }(ec); ftr}
////              .recover(e => onErr).
////
//////              .recover{a => a}
////              .map{ftr => ftr.recover{case e if true => onErr(e)}; ftr}
////              .get
//
////              .recover{case e if true => onErr(e)}
//
////            val url = new URL(item.url)
////            item.markAsInProgress()
////            val future = extractor.extract(url)
////            future.onComplete{
////              f =>
////                f.foreach{
////                  result =>
////                    result
////                      .redirectTarget
////                      .map(target => Try(new URL(target)).toOption)
////                      .flatten
////                      .filter(target => item.depth < conf.redirectPolicy(target))
////                      .foreach(target => urls.add(target.toString, item.depth + 1))
////                }
////                f.map(result => result.doc)
////                  .map(t => onSuccess(t.get))
////                  .recover{case e if true => onErr(e)}
////                item.markAsProcessed()
////            }(ec)
////            future
//        }
//      Await.result(Future.sequence(futures), Duration.Inf)
//    }
    def when(predicate: URL => Boolean): BranchConfigurationBuilder[Raw, Doc] = {
      new BranchConfigurationBuilder[Raw, Doc](urls, builder, predicate)
    }
  }

  class BranchConfigurationBuilder[Raw, Doc](urls: CrawlingQueue, conf: ConfigurationBuilder[Raw, Doc], predicate: URL => Boolean) {
    def set(
             fetcher: URL => Raw = conf.fetcher,
             parser: Raw => Doc = conf.parser,
             delay: Long = conf.delay,
             redirectPolicy: URL => Int = conf.redirectPolicy,
             robotsTxtPolicy: Boolean = conf.robotsTxtPolicy
           ): BranchPredicateBuilder[Raw, Doc] = {
      conf.addConf(predicate, fetcher, parser, delay, redirectPolicy, robotsTxtPolicy)
      new BranchPredicateBuilder[Raw, Doc](urls, conf)
    }
  }

  class JsoupHttpFetcher

  import izolotov.crawler.DefaultCrawler._
//  implicit val httpFetcher: Function[URL, HttpResponse[Document]] = HttpFetcher().fetch
  implicit val httpFetcher = DefaultHttpFetcher(HttpClient.newBuilder().build())
  implicit val defaultParser: HttpResponse[Document] => String = resp => "resp from: " + resp.uri().toString
//  implicit val anotherParser: HttpResponse[Document] => String = resp => "body: " + resp.body().html().substring(0, 10)
  implicit val redirectExtractor: HttpResponse[Document] => Option[String] = {
    resp => {
      if (resp.uri().toString() == "http://example.com")
        Some("http://redirect.com")
      else
        None
    }
  }
  implicit val robotsPolicyExtractor: HttpResponse[Document] => RobotsRules = {
    resp =>
      val raw = new SimpleRobotRulesParser().parseContent(
        resp.uri().toString,
        resp.body().wholeText().getBytes,
        resp.headers().firstValue("Content-Type").orElse("text/plain"),
        Lists.newArrayList(resp.request().headers().firstValue("User-Agent").orElse("")) //Lists.newArrayList("bot")
      )
      RobotsRules(if (raw.getCrawlDelay == BaseRobotRules.UNSET_CRAWL_DELAY) None else Some(raw.getCrawlDelay))
  }

  def respect(): URL => RobotsRules = url => RobotsRulesExtractor.extract(url)
  def ignore(): URL => RobotsRules = _ => RobotsRules(None)

  def all(depth: Int = 5): URL => Int = url => depth
  def no(): URL => Int = url => 0

  object DefaultHttpFetcher {
    def apply[T](httpClient: HttpClient)(implicit bodyHandler: HttpResponse.BodyHandler[T]): DefaultHttpFetcher[T] = new DefaultHttpFetcher[T](httpClient)
  }

  class DefaultHttpFetcher[T](httpClient: HttpClient)(implicit bodyHandler: HttpResponse.BodyHandler[T]) {
    if (httpClient.followRedirects() != HttpClient.Redirect.NEVER)
      throw new IllegalStateException("Only the NEVER redirect policy allowed")
    def fetcher(userAgent: String = null, cookies: Map[String, String] = Map.empty): URL => HttpResponse[T] = {
      url =>
        println(f"Fetch: ${url}")
        val builder = HttpRequest.newBuilder()
          .uri(url.toURI)
        if (userAgent != null) builder.setHeader("User-Agent", userAgent)
        builder.setHeader("Cookie", cookies.map(entry => s"${entry._1}=${entry._2}").mkString("; "))
        httpClient.send(builder.build(), bodyHandler)
    }
    def fetch(url: URL): HttpResponse[T] = {
      println(f"Fetch: ${url}")
      val request = HttpRequest.newBuilder()
        .uri(url.toURI)
        .build()
      httpClient.send(request, bodyHandler)
    }
  }

  val trekkinnParser: HttpResponse[Document] => String = {
    resp =>
//      val doc = Jsoup.parse(inStream, charset.name(), urlString)
      val priceStr = resp.body().select("p#total_dinamic").first().text()
      priceStr.replaceAll("[^0-9,.]", "")
  }// resp => "resp from: " + resp.uri().toString

  def main(args: Array[String]): Unit = {
    val userAgent = "Mozilla/5.0 (platform; rv:geckoversion) Gecko/geckotrail Firefox/firefoxversion"
    NewCrawler
      .read(new CrawlingQueue(Seq(
        "http://sdf:sds:sf",
        "http://example.com",
        "http://example.com",
        "httm",
//        "https://www.densurka.ru/",
//        "https://www.densurka.ru/",
//        "https://www.tradeinn.com/trekkinn/ru/petzl-nomic-%D0%9B%D0%B5%D0%B4%D0%BE%D1%80%D1%83%D0%B1/137053842/p"
      )))
      .extractWith(fetcher = httpFetcher.fetcher(userAgent, cookies = Map("id_pais" -> "75")), parser = defaultParser, timeout = 10000L, redirectPolicy = all())
      .when(url("http://example.com")).set(delay = 1000L, redirectPolicy = all())
//      .when(url("https://www.tradeinn.com/trekkinn/ru/petzl-nomic-%D0%9B%D0%B5%D0%B4%D0%BE%D1%80%D1%83%D0%B1/137053842/p")).set(parser = trekkinnParser)
//      .when(url("https://www.densurka.ru")).set(delay = 3000L, robotsTxtPolicy = true)
      .foreach(println, println)
  }

  // FIXME
  // 1. Redirect doesn't work if set to NO globally

  // TODO:
  // 1. User agent for robots.txt extractor
  // 2. Initial delay afret tobots.txt extraction
  // 3. Allow/disallow (allowance policy)
  // 4. Queue item sealed trait (Redirect, Outlink)


}
