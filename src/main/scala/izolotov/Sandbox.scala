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
    def fetch[Raw](fetcher: URL => Redirectable[Raw]): ParserBranchBuilder[Raw] = {
      new ParserBranchBuilder[Raw](queue, {case url if predicate.apply(url) => Try(fetcher.apply(url)).recover(e => throw new FetchingException(url.toString, e)).get})
    }
  }
  class SuccessiveFetcherBranchBuilder[Doc](queue: CrawlingQueue, predicate: URL => Boolean, partial: PartialFunction[URL, Redirectable[Doc]]) {
    def fetch[Raw](fetcher: URL => Redirectable[Raw]): SuccessiveParserBranchBuilder[Raw, Doc] = {
      new SuccessiveParserBranchBuilder[Raw, Doc](queue, {case url if predicate.apply(url) => Try(fetcher.apply(url)).recover(e => throw new FetchingException(url.toString, e)).get}, partial)
    }
  }
  class FinalFetcherBranchBuilder[Doc](queue: CrawlingQueue, partial: PartialFunction[URL, Redirectable[Doc]]) {
    def fetch[Raw](fetcher: URL => Redirectable[Raw]): FinalParserBranchBuilder[Raw, Doc] = {
//      val f = fetcher.andThen(k => k.map(parser))
      new FinalParserBranchBuilder[Raw, Doc](queue, {case url => fetcher.apply(url)}, partial)
    }
  }

  //Parser

  class ParserBranchBuilder[Raw](queue: CrawlingQueue, fetcher: PartialFunction[URL, Redirectable[Raw]]) {
    def parse[Doc](parser: Raw => Doc): SubsequentBranchBuilder[Doc] = {
      val f = fetcher.andThen(k => k.map(parser))
//      val p: PartialFunction[Redirectable[Raw], Redirectable[Doc]] = {
//        case redirectable => redirectable match {
//          case Direct(url, data) => Direct(url, parser.apply(data))
//        }
//      }
      new SubsequentBranchBuilder[Doc](queue, f)
    }
  }
  class SuccessiveParserBranchBuilder[Raw, Doc](queue: CrawlingQueue, fetcher: PartialFunction[URL, Redirectable[Raw]], partial: PartialFunction[URL, Redirectable[Doc]]) {
    def parse(parser: Raw => Doc): SubsequentBranchBuilder[Doc] = {
      val f = partial.orElse(fetcher.andThen(k => k.map(parser)))
      new SubsequentBranchBuilder[Doc](queue, f)
    }
  }
  class FinalParserBranchBuilder[Raw, Doc](queue: CrawlingQueue, fetcher: PartialFunction[URL, Redirectable[Raw]], partial: PartialFunction[URL, Redirectable[Doc]]) {
    def parse(parser: Raw => Doc): FinalBranchBuilder[Doc] = {
      val f = partial.orElse(fetcher.andThen(k => k.map(parser)))
      new FinalBranchBuilder[Doc](queue, partial.orElse(f))
    }
  }

  class SubsequentBranchBuilder[Doc](queue: CrawlingQueue, partialParser: PartialFunction[URL, Redirectable[Doc]]) {
    def when(predicate: URL => Boolean): SuccessiveFetcherBranchBuilder[Doc] = {
      new SuccessiveFetcherBranchBuilder(queue, predicate, partialParser)
    }

    def otherwise(): FinalFetcherBranchBuilder[Doc] = {
      new FinalFetcherBranchBuilder[Doc](queue, partialParser)
    }
  }
  class FinalBranchBuilder[Doc](queue: CrawlingQueue, parser: URL => Redirectable[Doc]) {
    def write(writer: Doc => Unit): FailureHandler = {
      // TODO add redirect handling here
      val f = parser.andThen(k => k.data()).andThen(writer)
      new FailureHandler(queue, f)
    }
  }

  class FailureHandler(queue: CrawlingQueue, writer: URL => Unit) {
    def ofFailure(handler: CrawlingException => Unit): PipelineRunner = {
      val v: PartialFunction[URL, Try[Unit]] = {case url => Try(writer(url))}
      new PipelineRunner(queue, writer, handler)
    }
  }

  class PipelineRunner(queue: CrawlingQueue, writer: URL => Unit, errHandler: CrawlingException => Unit) {
    def crawl(): Unit = {
      queue.foreach{
        url =>
          Try(writer.apply(Try(new URL(url)).recover(e => throw new URLParsingException(url, e)).get))
            .recover{
              exc => exc match {
                case exc: CrawlingException => errHandler.apply(exc)
                case _ => throw new RuntimeException("Unknown exception")
              }
            }
      }
    }
  }

  sealed class CrawlingException(url: String) extends Exception
  class URLParsingException(url: String, cause: Throwable) extends CrawlingException(url) {
    override def toString: String = {
      s"${this.getClass.getName} - ${url} - ${cause}"
    }
  }
  class FetchingException(url: String, cause: Throwable) extends CrawlingException(url) {
    override def toString: String = {
      s"${this.getClass.getName} - ${url} - ${cause}"
    }
  }
  class ParsingException[Raw](url: String, body: Raw, cause: Throwable) extends CrawlingException(url) {
    override def toString: String = {
      s"${this.getClass.getName} - ${url} - ${cause}"
    }
  }

  sealed trait Redirectable[A] {
    def url(): URL

    def data(): A

//    def map[U](f: T => U): Try[U]
    def map[B](f: A => B): Redirectable[B]
  }
  case class Direct[A](url: URL, data: A) extends Redirectable[A] {
    override def map[B](f: A => B): Redirectable[B] = {
      Direct(url, f.apply(data))
    }
  }
  case class Redirect[A](url: URL, target: String, data: A) extends Redirectable[A] {
    override def map[B](f: A => B): Redirectable[B] = {
      Redirect(url, target, f.apply(data))
    }
  }

  case class Fetched(url: URL, body: String)
  case class Parsed(url: URL, content: String)

  def main(args: Array[String]): Unit = {
    val queue = Seq("http://1", "2", "ftp://3", "http://4")
    Crawler.read(queue)
      .when(url => url.getProtocol == "https")
//        .fetch(url => Fetched(url, s"url - https fetcher"))
        .fetch(url => Direct(url, Fetched(url, s"url - https fetcher")))
//        .fetch(url => Fetched(url, s"url - https fetcher"))
        .parse(fetched => Parsed(fetched.url, s"parsed - ${fetched.body}"))
      .when(url => url.getProtocol == "ftp")
        .fetch(url => if (url.toString.endsWith("3")) throw new Exception("Can't fetch") else Direct(url, Fetched(url, s"url - ftp fetcher")))
        .parse(fetched => Parsed(fetched.url, s"parsed - ${fetched.body}"))
      .otherwise()
        .fetch(url => Direct(url, Fetched(url, s"url - default fetcher")))
        .parse(fetched => Parsed(fetched.url, s"parsed - ${fetched.body}"))
      .write(parsed => println(parsed))
      .ofFailure(exc => println(exc))
      .crawl()
  }

}
