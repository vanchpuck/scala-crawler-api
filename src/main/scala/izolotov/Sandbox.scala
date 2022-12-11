package izolotov

import java.net.URL

import scala.collection.mutable
import scala.util.Try

object Sandbox {

  case class QueueItem(url: String, depth: Int)

  class CrawlingQueue(data: Iterable[String]) extends Iterator[String]{
    val queue: mutable.Queue[String] = mutable.Queue()
    queue ++= data

    def add(url: String): Unit = {
      queue += url
    }

    override def hasNext: Boolean = {
      val a = queue.length > 0
      //      println(s"hasNext: ${a}")
      queue.length > 0
    }

    override def next(): String = {
      //      println("next")
      queue.dequeue()
    }
  }

  object Crawler {
    def read(data: Iterable[String]): InitialBranchBuilder = {
      val q = new mutable.Queue[String]()
      q ++= data
      new InitialBranchBuilder(new CrawlingQueue(data))
    }
  }

  class InitialBranchBuilder(queue: CrawlingQueue) {
//    def fetch[Raw](fetcher: String => Raw): ParserBuilder[Raw] = {
//      new FetcherBuilder(queue).fetch(fetcher)
//    }

    def when(predicate: URL => Boolean): FetcherBranchBuilder = {
      new FetcherBranchBuilder(queue, predicate)
    }

  }

  // Fetcher
  class FetcherBranchBuilder(queue: CrawlingQueue, predicate: URL => Boolean) {
    def fetch[Raw](fetcher: URL => Raw): ParserBranchBuilder[Raw] = {
      new ParserBranchBuilder[Raw](queue, {case url if predicate.apply(url) => fetcher.apply(url)})
    }
  }
  class SuccessiveFetcherBranchBuilder[Doc](queue: CrawlingQueue, predicate: URL => Boolean, partial: PartialFunction[URL, Doc]) {
    def fetch[Raw](fetcher: URL => Raw): SuccessiveParserBranchBuilder[Raw, Doc] = {
      new SuccessiveParserBranchBuilder[Raw, Doc](queue, {case url if predicate.apply(url) => fetcher.apply(url)}, partial)
    }
  }
  class FinalFetcherBranchBuilder[Doc](queue: CrawlingQueue, partial: PartialFunction[URL, Doc]) {
    def fetch[Raw](fetcher: URL => Raw): FinalParserBranchBuilder[Raw, Doc] = {
      new FinalParserBranchBuilder[Raw, Doc](queue, {case url => fetcher.apply(url)}, partial)
    }
  }

  //Parser

  class ParserBranchBuilder[Raw](queue: CrawlingQueue, fetcher: PartialFunction[URL, Raw]) {
    def parse[Doc](parser: Raw => Doc): SubsequentBranchBuilder[Doc] = {
      new SubsequentBranchBuilder[Doc](queue, fetcher.andThen(parser))
    }
  }
  class SuccessiveParserBranchBuilder[Raw, Doc](queue: CrawlingQueue, fetcher: PartialFunction[URL, Raw], partial: PartialFunction[URL, Doc]) {
    def parse(parser: Raw => Doc): SubsequentBranchBuilder[Doc] = {
      new SubsequentBranchBuilder[Doc](queue, partial.orElse(fetcher.andThen(parser)))
    }
  }
  class FinalParserBranchBuilder[Raw, Doc](queue: CrawlingQueue, fetcher: PartialFunction[URL, Raw], partial: PartialFunction[URL, Doc]) {
    def parse(parser: Raw => Doc): FinalBranchBuilder[Doc] = {
      new FinalBranchBuilder[Doc](queue, partial.orElse(fetcher.andThen(parser)))
    }
  }

  class SubsequentBranchBuilder[Doc](queue: CrawlingQueue, partialParser: PartialFunction[URL, Doc]) {
    def when(predicate: URL => Boolean): SuccessiveFetcherBranchBuilder[Doc] = {
      new SuccessiveFetcherBranchBuilder(queue, predicate, partialParser)
    }

    def otherwise(): FinalFetcherBranchBuilder[Doc] = {
      new FinalFetcherBranchBuilder[Doc](queue, partialParser)
    }
  }
  class FinalBranchBuilder[Doc](queue: CrawlingQueue, parser: URL => Doc) {
    def write(writer: Doc => Unit): PipelineRunner = {
      new PipelineRunner(queue, parser.andThen(writer))
    }
  }

  class PipelineRunner(queue: CrawlingQueue, writer: URL => Unit) {
    def crawl(): Unit = {
      queue.foreach(url => writer.apply(new URL(url)))
    }
  }

  case class Fetched(url: URL, body: String)
  case class Parsed(url: URL, content: String)

  def main(args: Array[String]): Unit = {
    val queue = Seq("http://1", "https://2", "ftp://3", "http://4")
    Crawler.read(queue)
      .when(url => url.getProtocol == "https")
        .fetch(url => Fetched(url, s"url - https fetcher"))
        .parse(fetched => Parsed(fetched.url, s"parsed - ${fetched.body}"))
      .when(url => url.getProtocol == "ftp")
        .fetch(url => Fetched(url, s"url - ftp fetcher"))
        .parse(fetched => Parsed(fetched.url, s"parsed - ${fetched.body}"))
      .otherwise()
        .fetch(url => Fetched(url, s"url - default fetcher"))
        .parse(fetched => Parsed(fetched.url, s"parsed - ${fetched.body}"))
      .write(parsed => println(parsed))
      .crawl()
  }

}
