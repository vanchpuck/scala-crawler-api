package izolotov.app

import java.net.URL

import izolotov.crawler.CrawlerApi
import izolotov.crawler.CrawlerApi.{Crawler, CrawlerBuilder, ParsingException}
//import izolotov.crawler.DefaultCrawler.CrawlerExt

import scala.collection.mutable

object App {

  val f: String => Boolean = a => a.isEmpty

  def main(args: Array[String]): Unit = {
    import izolotov.crawler.DefaultCrawler._
    import izolotov.crawler.NewCrawler._
    val fetcher = HttpFetcher().fetch(_)

    case class Container(str: String)
    case class Container1(str: String)

    def write(pr :Product):Unit = println(pr)
    Option("a").map(f => f)
//    Crawler
//      .withHostSettings()
//      .when(a => a.isEmpty).option(Parallelism, 1)
    val f = Crawler
      //.configure().set(NewDelay, 5000L).set(NewPrinter, false)
      .branchConfigure()
      .set(NewDelay, 1000L).set(NewPrinter, true)
      .when(host("example.com")).set(NewDelay, 2000L).set(NewPrinter, false)
      .otherwise()//.set(NewDelay, 1000L)
//      .when(host("example.com"))
//      .withHostSettings().when(host("example.com")).option(HostDelay,  5000L)
//      .withSettings().option(Parallelism, 1).option(Delay, 5000L)
//      .withHostSettings()(null)
//      .when(host("example.com")).option(Delay, 3000L).option(Parallelism, 10)
//      .when(host("example1.com")).option(Delay, 2000L).option(Parallelism, 10)
//      .otherwise().option(HostDelay, 20000L)
      .read(mutable.Seq("http://example.com/1", "http://example.com/2"))
      .when(host("example.com")).fetch(fetcher).parse(resp => throw new ParsingException[String]("sdf", "sdf", new RuntimeException()))
      .when(host("example.com")).fetch(fetcher).parse(resp => Container(resp.body().html()))
      .when(url("http://example.com/1")).fetch(fetcher).parse(resp => Container(resp.body().html()))
      .otherwise().fetch(fetcher).parse(resp => Container(resp.body().html()))
      .followRedirects()
      .write(write)
      .ofFailure{
        err =>
          err match {
            case e: ParsingException[_] => println("f " + e)
          }
      }
      .crawl()
//      .re
//    val mb = f.apply(new URL("http://example1.com/1"))
//    println(mb.getDelay())
//      .option(FlexibleDelay, _: URL => 5000L)
//      .option(Parallelism, 10)

  }

}
