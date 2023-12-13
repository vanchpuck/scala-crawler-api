package izolotov.crawler

import java.net.URL
import java.net.http.HttpResponse

import izolotov.CrawlingQueue
import izolotov.CrawlingQueue.Item
import izolotov.crawler.SuperNewCrawlerApi.BranchConfigurationBuilder

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
                                       fetcher: URL => URL => Raw,
                                       parser: URL => Raw => Doc,
                                       delay: URL => Long,
                                       redirectPattern: URL => Boolean,
                                       redirectDepth: URL => Int
                                     )

  case class ConfigurationBuilder[Raw, Doc] (
                                              parallelism: Int,
                                              redirect: Raw => Option[URL],
                                              redirectHandler: URL => Unit,
                                              fetcher: URL => Raw,
                                              parser: Raw => Doc,
                                              delay: Long,
                                              redirectPattern: Boolean,
                                              redirectDepth: Int
                                            ) {
    var getFetcher: PartialFunction[URL, URL => Raw] = {case _ if true => this.fetcher}
    var getParser: PartialFunction[URL, Raw => Doc] = {case _ if true => this.parser}
    var getDelay: PartialFunction[URL, Long] = {case _ if true => this.delay}
    var getRedirectPattern: PartialFunction[URL, Boolean] = {case _ if true => this.redirectPattern}
    var getRedirectDepth: PartialFunction[URL, Int] = {case _ if true => this.redirectDepth}
    def addConf (
                  predicate: URL => Boolean,
                  fetcher: URL => Raw,
                  parser: Raw => Doc,
                  delay: Long,
                  redirectPattern: Boolean,
                  redirectDepth: Int
                ): Unit = {
      if (fetcher != this.fetcher) getFetcher = toPF(predicate, fetcher).orElse(getFetcher)
      if (parser != this.parser) getParser = toPF(predicate, parser).orElse(getParser)
      if (delay != this.delay) getDelay = toPF(predicate, delay).orElse(getDelay)
      if (redirectPattern != this.redirectPattern) getRedirectPattern =
        toPF(predicate, redirectPattern).orElse(getRedirectPattern)
      if (redirectDepth != this.redirectDepth) getRedirectDepth =
        toPF(predicate, redirectDepth).orElse(getRedirectDepth)
    }

    def build(): Configuration[Raw, Doc] = {
      Configuration[Raw, Doc] (
        parallelism,
        redirect,
        redirectHandler,
        getFetcher,
        getParser,
        getDelay,
        getRedirectPattern,
        getRedirectDepth
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


  class ExtractionBuilder(urls: CrawlingQueue) {
    def extract[Raw, Doc](fetcher: URL => Raw, parser: Raw => Doc)(implicit redirect: Raw => Option[URL]): BranchPredicateBuilder[Raw, Doc] = {
      // TODO hardcode
      val redirectHandler: URL => Unit = url => urls.add(url.toString, 1)
      val confBuilder = ConfigurationBuilder(
        10, redirect, redirectHandler, fetcher, parser, 0L, true, 1
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
//      val extractor = new SuperNewPerHostExtractor.Queue[Doc](10)
      val extractor = new SuperNewPerHostExtractor[Raw, Doc](conf)
      val futures = urls.map{
        item =>
          item.markAsInProgress()
          val url: URL = new URL(item.url())
//          val fnExtract: URL => Doc = conf
//            .fetcher(url)
//            .andThen{
//              raw =>
//                conf.redirect(raw).foreach(target => if (conf.redirectPattern(url) /*&& conf.redirectDepth(url) < item.depth()*/) conf.redirectHandler(target))
//                raw
//            }.andThen(conf.parser(url))
          val future = extractor.extract(url)
          future.onComplete{
            res =>
              res.map(onSuccess).recover({case e if true => onErr(e)})
              item.markAsProcessed()
          }
          future
      }
      Await.result(Future.sequence(futures), Duration.Inf)
//      extractor.shutdown()
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
             redirectDepth: Int = conf.redirectDepth
           ): BranchPredicateBuilder[Raw, Doc] = {
      conf.addConf(predicate, fetcher, parser, delay, redirectPattern, redirectDepth)
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

  def main(args: Array[String]): Unit = {
    NewCrawler
      .read(new CrawlingQueue(Seq("http://example.com", "http://example.com", "http://example.com", "http://example.net")))
      .extract(fetcher = httpFetcher, parser = defaultParser)
      .when(url("http://example.com")).set(delay = 2000L, redirectDepth = 5, parser = anotherParser)
      .foreach(println)
  }


}
