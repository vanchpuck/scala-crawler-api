package izolotov.app

import java.net.URL
import java.net.http.HttpResponse

import izolotov.crawler.{CrawlerApi, CrawlerParameterBuilder, ExtractionManager, Extractor, Manager, PerHostExtractor}
import izolotov.crawler.CrawlerApi.{Crawler, CrawlerBuilder, ParsingException}
import org.jsoup.nodes.Document

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
//import izolotov.crawler.DefaultCrawler.CrawlerExt

import scala.collection.mutable

object App {

  val f: String => Boolean = a => a.isEmpty

  def main(args: Array[String]): Unit = {
    import izolotov.crawler.DefaultCrawler._
    import izolotov.crawler.NewCrawler._
//    val fetcher = HttpFetcher().fetch(_)
    val httpFetcher: Function[URL, HttpResponse[Document]] = HttpFetcher().fetch

    CrawlerParameterBuilder

    case class Container(str: String)
    case class Container1(str: String)

    def factory(conf: CrawlerParameterBuilder.Conf[Container]): Extractor[Container] = {
      new PerHostExtractor[Container](conf)
    }
    def foreachHandler(pr: Future[Container]):Unit = {
      pr.onComplete{
        case Success(pr) => println(pr)
        case Failure(exc) => println(exc)
      }(ExecutionContext.global)
    }

    def write(pr :Product):Unit = println(pr)
    def parseFail(pr: HttpResponse[Document]): Container = {
      throw new Exception("!")
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
        delay = 1000L,
        followRedirectPattern = host("example.com"),
        parallelism = 2
      ).when(host("example1.com")).set(
        fetcher = httpFetcher,
        delay = 100L
      )
      .read(mutable.Seq(
        "http://example.com",
        /*"malformed",*/
        "http://google.com",
        "http://example.com",
        "http://example.com",
        "http://facebook.com",
//        "http://example.com",
//        "http://example.com",
//        "http://example.com"
      ))
      .extract()(factory)
      .foreach(printOut)

//    Thread.sleep(5000)

//      .foreach(foreachHandler)(factory)
//    println(f.delay(new URL("http://example.com/1")))
//    f.extractor.apply(new URL("http://example.com/1")).apply(new URL("http://example.com/1"))

//      .richConfigure()
//      .default.setFetcher(fetcher).setParser(resp => Container(resp.body().title())).setWriter(write).set(RichDelay, 10L)
//      .when(host("www.buran.ru")).setParser(parseFail).setWriter(w => println(f"Buran:: ${w}"))
//      .when(host("example.com")).setWriter(w => println(f"Example:: ${w}")).set(RichDelay, 3000L)
//      .when(url("dummy1.com")).set(RichDelay, 100L)
//      .when(url("dummy2.com")).setWriter(write)
//      .read(mutable.Seq("http://example.com/1", "http://example.com/2", "http://www.buran.ru/", "http://www.buran.ru/"))
//      .crawl()


      //.configure().set(NewDelay, 5000L).set(NewPrinter, false)
//      .branchConfigure()
//      .set(NewDelay, 1000L).set(NewPrinter, true)
//      .when(host("example.com")).set(NewPrinter, false)
//      .otherwise()//.set(NewDelay, 1000L)
//      .when(host("example.com"))
//      .withHostSettings().when(host("example.com")).option(HostDelay,  5000L)
//      .withSettings().option(Parallelism, 1).option(Delay, 5000L)
//      .withHostSettings()(null)
//      .when(host("example.com")).option(Delay, 3000L).option(Parallelism, 10)
//      .when(host("example1.com")).option(Delay, 2000L).option(Parallelism, 10)
//      .otherwise().option(HostDelay, 20000L)

//      .read(mutable.Seq("http://example.com/1", "http://example.com/2"))
//      .when(host("example.com")).fetch(fetcher).parse(resp => throw new ParsingException[String]("sdf", "sdf", new RuntimeException()))
//      .when(host("example.com")).fetch(fetcher).parse(resp => Container(resp.body().html()))
//      .when(url("http://example.com/1")).fetch(fetcher).parse(resp => Container(resp.body().html()))
//      .otherwise().fetch(fetcher).parse(resp => Container(resp.body().html()))
//      .followRedirects()
//      .write(write)
//      .ofFailure{
//        err =>
//          err match {
//            case e: ParsingException[_] => println("f " + e)
//          }
//      }
//      .crawl()
//      .re
//    val mb = f.apply(new URL("http://example1.com/1"))
//    println(mb.getDelay())
//      .option(FlexibleDelay, _: URL => 5000L)
//      .option(Parallelism, 10)

  }

}
