package izolotov.app

import java.net.URL

import izolotov.crawler.CrawlerApi.Crawler

import scala.collection.mutable

object App {

  def main(args: Array[String]): Unit = {
    import izolotov.crawler.DefaultCrawler._
    val fetcher = HttpFetcher().fetch(_)
    Crawler.flow()
      .read(mutable.Seq("http://example.com/1", "http://example.com/1"))
      .when(url => url.getPath == "1").fetch(fetcher).parse(resp => resp.body().html())
      .otherwise().fetch(fetcher).parse(resp => resp.body().html())
      .followRedirects()
      .write(data => println(data))
      .ofFailure(err => err)
      .crawl()(new HostQueueManager(2000L, 10))
  }

}
