package izolotov

import java.net.URL

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

object Sandbox {

  case class InputItem(id:String) {
    def onPickUp(): InputItem = {
      println(s"${id.toString} picked up")
      this
    }

    def onSuccess(): InputItem = {
      println(s"${id.toString} success")
      this
    }

    def onFailure(): InputItem = {
      println(s"${id.toString} failure")
      this
    }
  }

  case class QueueItem(inputItem: InputItem, url: URL, depth: Int)

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
      new ParserBranchBuilder[Raw](queue, {case qItem if predicate.apply(qItem.url) => Try(fetcher.apply(qItem.url)).recover(e => throw new FetchingException(qItem.url.toString, e)).get})
    }
  }
  class SuccessiveFetcherBranchBuilder[Doc](queue: CrawlingQueue, predicate: URL => Boolean, partial: PartialFunction[QueueItem, Redirectable[Doc]]) {
    def fetch[Raw](fetcher: URL => Redirectable[Raw]): SuccessiveParserBranchBuilder[Raw, Doc] = {
      new SuccessiveParserBranchBuilder[Raw, Doc](queue, {case qItem if predicate.apply(qItem.url) => Try(fetcher.apply(qItem.url)).recover(e => throw new FetchingException(qItem.url.toString, e)).get}, partial)
    }
  }
  class FinalFetcherBranchBuilder[Doc](queue: CrawlingQueue, partial: PartialFunction[QueueItem, Redirectable[Doc]]) {
    def fetch[Raw](fetcher: URL => Redirectable[Raw]): FinalParserBranchBuilder[Raw, Doc] = {
      new FinalParserBranchBuilder[Raw, Doc](queue, {case qItem => fetcher.apply(qItem.url)}, partial)
    }
  }

  //Parser

  class ParserBranchBuilder[Raw](queue: CrawlingQueue, fetcher: PartialFunction[QueueItem, Redirectable[Raw]]) {
    def parse[Doc](parser: Raw => Redirectable[Doc]): SubsequentBranchBuilder[Doc] = {
//      val ff: PartialFunction[QueueItem, Redirectable[Doc]] = {case qItem => parser.apply(fetcher.ap)}
      val f = fetcher.andThen(k => k.map(parser).flatten)
      new SubsequentBranchBuilder[Doc](queue, f)
    }
  }
  class SuccessiveParserBranchBuilder[Raw, Doc](queue: CrawlingQueue, fetcher: PartialFunction[QueueItem, Redirectable[Raw]], partial: PartialFunction[QueueItem, Redirectable[Doc]]) {
    def parse(parser: Raw => Redirectable[Doc]): SubsequentBranchBuilder[Doc] = {
      val f = partial.orElse(fetcher.andThen(k => k.map(parser).flatten))
      new SubsequentBranchBuilder[Doc](queue, f)
    }
  }
  class FinalParserBranchBuilder[Raw, Doc](queue: CrawlingQueue, fetcher: PartialFunction[QueueItem, Redirectable[Raw]], partial: PartialFunction[QueueItem, Redirectable[Doc]]) {
    def parse(parser: Raw => Redirectable[Doc]): FinalBranchBuilder[Doc] = {
      val f = partial.orElse(fetcher.andThen(k => k.map(parser).flatten))
      new FinalBranchBuilder[Doc](queue, partial.orElse(f))
    }
  }

  class SubsequentBranchBuilder[Doc](queue: CrawlingQueue, partialParser: PartialFunction[QueueItem, Redirectable[Doc]]) {
    def when(predicate: URL => Boolean): SuccessiveFetcherBranchBuilder[Doc] = {
      new SuccessiveFetcherBranchBuilder(queue, predicate, partialParser)
    }

    def otherwise(): FinalFetcherBranchBuilder[Doc] = {
      new FinalFetcherBranchBuilder[Doc](queue, partialParser)
    }
  }
  class FinalBranchBuilder[Doc](queue: CrawlingQueue, parser: QueueItem => Redirectable[Doc]) {
    def followRedirects(): RedirectHandler[Doc] = {
      new RedirectHandler[Doc](queue, parser)
    }
  }

  class RedirectHandler[Doc](queue: CrawlingQueue, parser: QueueItem => Redirectable[Doc]) {
    def write(writer: Doc => Unit): FailureHandler = {
      val f = parser.andThen{
        k => k match {
          case Direct(data) => data
          case Redirect(target, data) => {
            queue.add(target)
            data
          }
        }
      }.andThen(writer)
      new FailureHandler(queue, f)
    }
  }

//  class WriterHandler[Doc](queue: CrawlingQueue, parser: URL => Redirectable[Doc]) {
//    def write(writer: Doc => Unit): FailureHandler = {
//      val f = parser.andThen(k => k.data()).andThen(writer)
//      new FailureHandler(queue, f)
//    }
//  }

  class FailureHandler(queue: CrawlingQueue, writer: QueueItem => Unit) {
    def ofFailure(handler: CrawlingException => Unit): PipelineRunner = {
//      val v: PartialFunction[URL, Try[Unit]] = {case url => Try(writer(url))}
      new PipelineRunner(queue, writer, handler)
    }
  }

  class PipelineRunner(queue: CrawlingQueue, writer: QueueItem => Unit, errHandler: CrawlingException => Unit) {
    def crawl(): Unit = {
      queue.foreach{
        url =>
          val inItem = InputItem(url)
          inItem.onPickUp()
          Try{
            val qItem = Try(QueueItem(inItem, new URL(url), 0)).recover(e => throw new URLParsingException(url, e)).get
            writer.apply(qItem)
          } match {
            case Success(_) => inItem.onSuccess()
            case Failure(exc) => {
              exc match {
                case exc: CrawlingException => {
                  inItem.onFailure()
                  errHandler.apply(exc)
                }
                case _ => throw new RuntimeException("Unknown exception")
              }
            }
          }
      }
    }
  }


  sealed class CrawlingException(url: String) extends Exception {
    def url(): String = url
  }
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
    def data(): A

    def map[B](f: A => B): Redirectable[B]

    def flatten[B](implicit ev: A <:< Redirectable[B]): Redirectable[B]
  }
  case class Direct[A](data: A) extends Redirectable[A] {
    override def map[B](f: A => B): Redirectable[B] = {
      Direct(f.apply(data))
    }

    override def flatten[B](implicit ev: A <:< Redirectable[B]): Redirectable[B] = {
      ev(this.data)
    }
  }
  case class Redirect[A](target: String, data: A) extends Redirectable[A] {
    override def map[B](f: A => B): Redirectable[B] = {
      Redirect(target, f.apply(data))
    }

    override def flatten[B](implicit ev: A <:< Redirectable[B]): Redirectable[B] = Redirect(target, data.data())
  }

  case class Fetched(url: URL, body: String)
  case class Parsed(url: URL, content: String)

  def main(args: Array[String]): Unit = {
    val queue = Seq("http://1", "http://redirect", "2", "ftp://3", "http://4", "http://meta-redirect")
    Crawler.read(queue)
      .when(url => url.getProtocol == "https")
        .fetch(url => Direct(Fetched(url, s"url - https fetcher")))
        .parse(fetched => Direct(Parsed(fetched.url, s"parsed - ${fetched.body}")))
      .when(url => url.getHost == "redirect")
        .fetch(url => Redirect("http://target", Fetched(url, s"empty")))
        .parse(fetched => Direct(Parsed(fetched.url, s"parsed - ${fetched.body}")))
      .when(url => url.getHost == "meta-redirect")
        .fetch(url => Redirect("http://meta-target", Fetched(url, s"some contect")))
        .parse(fetched => Direct(Parsed(fetched.url, s"parsed - ${fetched.body}")))
      .when(url => url.getProtocol == "ftp")
        .fetch(url => if (url.toString.endsWith("3")) throw new Exception("Can't fetch") else Direct(Fetched(url, s"url - ftp fetcher")))
        .parse(fetched => Direct(Parsed(fetched.url, s"parsed - ${fetched.body}")))
      .otherwise()
        .fetch(url => Direct(Fetched(url, s"url - default fetcher")))
        .parse(fetched => Direct(Parsed(fetched.url, s"parsed - ${fetched.body}")))
      .followRedirects()
      .write(parsed => println(parsed))
      .ofFailure(exc => println(exc))
      .crawl()
  }

}
