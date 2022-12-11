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

    def when(predicate: String => Boolean): FetcherBranchBuilder = {
      new FetcherBranchBuilder(queue, predicate)
    }

  }

  // Fetcher
  class FetcherBranchBuilder(queue: CrawlingQueue, predicate: String => Boolean) {
    def fetch[Raw](fetcher: String => Raw): ParserBranchBuilder[Raw] = {
      new ParserBranchBuilder[Raw](queue, {case url if predicate.apply(url) => fetcher.apply(url)})
    }
  }
  class SuccessiveFetcherBranchBuilder[Doc](queue: CrawlingQueue, predicate: String => Boolean, partial: PartialFunction[String, Doc]) {
    def fetch[Raw](fetcher: String => Raw): SuccessiveParserBranchBuilder[Raw, Doc] = {
      new SuccessiveParserBranchBuilder[Raw, Doc](queue, {case url if predicate.apply(url) => fetcher.apply(url)}, partial)
    }
  }
  class FinalFetcherBranchBuilder[Doc](queue: CrawlingQueue, partial: PartialFunction[String, Doc]) {
    def fetch[Raw](fetcher: String => Raw): FinalParserBranchBuilder[Raw, Doc] = {
      new FinalParserBranchBuilder[Raw, Doc](queue, {case url => fetcher.apply(url)}, partial)
    }
  }

  //Parser

  class ParserBranchBuilder[Raw](queue: CrawlingQueue, fetcher: PartialFunction[String, Raw]) {
    def parse[Doc](parser: Raw => Doc): SubsequentBranchBuilder[Doc] = {
      new SubsequentBranchBuilder[Doc](queue, fetcher.andThen(parser))
    }
  }
  class SuccessiveParserBranchBuilder[Raw, Doc](queue: CrawlingQueue, fetcher: PartialFunction[String, Raw], partial: PartialFunction[String, Doc]) {
    def parse(parser: Raw => Doc): SubsequentBranchBuilder[Doc] = {
      new SubsequentBranchBuilder[Doc](queue, partial.orElse(fetcher.andThen(parser)))
    }
  }
  class FinalParserBranchBuilder[Raw, Doc](queue: CrawlingQueue, fetcher: PartialFunction[String, Raw], partial: PartialFunction[String, Doc]) {
    def parse(parser: Raw => Doc): FinalBranchBuilder[Doc] = {
      new FinalBranchBuilder[Doc](queue, partial.orElse(fetcher.andThen(parser)))
    }
  }

  class SubsequentBranchBuilder[Doc](queue: CrawlingQueue, partialParser: PartialFunction[String, Doc]) {
    def when(predicate: String => Boolean): SuccessiveFetcherBranchBuilder[Doc] = {
      new SuccessiveFetcherBranchBuilder(queue, predicate, partialParser)
    }

    def otherwise(): FinalFetcherBranchBuilder[Doc] = {
      new FinalFetcherBranchBuilder[Doc](queue, partialParser)
    }
  }
  class FinalBranchBuilder[Doc](queue: CrawlingQueue, parser: String => Doc) {
    def write(writer: Doc => Unit): PipelineRunner = {
      new PipelineRunner(queue, parser.andThen(writer))
    }
  }

  class PipelineRunner(queue: CrawlingQueue, writer: String => Unit) {
    def crawl(): Unit = {
      queue.foreach(url => writer.apply(url))
    }
  }

  case class Fetched(url: String, body: String)
  case class Parsed(url: String, content: String)

  def main(args: Array[String]): Unit = {
    val queue = Seq("http://1", "2", "ftp://3", "4")
    Crawler.read(queue)
      .when(url => url.startsWith("https"))
        .fetch(url => Fetched(url, s"url - https fetcher"))
        .parse(fetched => Parsed(fetched.url, s"parsed - ${fetched.body}"))
      .when(url => url.startsWith("ftp"))
        .fetch(url => Fetched(url, s"url - ftp fetcher"))
        .parse(fetched => Parsed(fetched.url, s"parsed - ${fetched.body}"))
      .otherwise()
        .fetch(url => Fetched(url, s"url - default fetcher"))
        .parse(fetched => Parsed(fetched.url, s"parsed - ${fetched.body}"))
      .write(parsed => println(parsed))
      .crawl()
  }

}
