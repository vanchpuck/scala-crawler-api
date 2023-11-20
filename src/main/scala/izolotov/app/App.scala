package izolotov.app

import java.net.URL
import java.net.http.HttpResponse

import izolotov.crawler.{CrawlerApi, CrawlerParameterBuilder, Extractor, Manager, PerHostExtractor, PerHostExtractorLegacy}
import izolotov.crawler.CrawlerApi.{Crawler, CrawlerBuilder, ParsingException}
import org.jsoup.nodes.Document

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
//import izolotov.crawler.DefaultCrawler.CrawlerExt

import scala.collection.mutable

object App {

  def main(args: Array[String]): Unit = {
    import izolotov.crawler.DefaultCrawler._

    val httpFetcher: Function[URL, HttpResponse[Document]] = HttpFetcher().fetch

    CrawlerParameterBuilder

    case class Container(str: String)
    case class Container1(str: String)

    def factory(conf: CrawlerParameterBuilder.Conf[Container]): Extractor[Container] = {
      new PerHostExtractor[Container](
        conf.parallelism,
        conf.extractor,
        conf.delay
      )
    }

    def parseSuccess(pr: HttpResponse[Document]): Container = {
      Container(pr.body().title())
    }
    def printOut(container: Container): Unit = {
      println("Out: " + container)
    }

    val f = Crawler
      .conf()
      .default().set(
        fetcher = httpFetcher,
        parser = parseSuccess,
        delay = 10000L,
        redirectPattern = host("example.com"),
        parallelism = 9
      ).when(host("facebook.com")).set(
        fetcher = httpFetcher,
        delay = 10000L,
      )
      .read(mutable.Seq(
        "http://facebook.com",
        "http://example.com",
        "http://google.com",
//        "malformed",
        "http://example.com",
        "http://example.com",
//        "malformed",
        "http://facebook.com",
        "http://twitter.com",
        "http://twitter.com",
        "http://twitter.com",
        "http://yahoo.com",
        "http://yahoo.com",
        "http://yahoo.com",
        "http://yahoo.com",
        "http://okta.com",
        "http://okta.com",
        "http://okta.com",
//        "http://example.com",
//        "http://example.com",
//        "http://example.com"
      ))
      .extract()(factory)
      .foreach(printOut)

  }

}
