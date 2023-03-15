package izolotov.crawler

import java.net.URL

import izolotov.crawler.CrawlerApi.{InitialBranchBuilder, ManagerBuilder}
import izolotov.crawler.CrawlerInput.{CrawlingQueue, Input, InputItem, QueueItem}
import izolotov.crawler.DefaultCrawler.HostQueueManager

//import izolotov.Sandbox.{CrawlingQueue, InputItem, Manager, QueueItem}

import scala.collection.mutable
import scala.util.Try

object CrawlerApi {

  sealed trait Redirectable[A] {
    def data(): A

    def map[B](f: A => B): Redirectable[B]
  }
  case class Direct[A](data: A) extends Redirectable[A] {
    override def map[B](f: A => B): Redirectable[B] = {
      Direct(f.apply(data))
    }
  }
  case class Redirect[A](target: String, data: A) extends Redirectable[A] {
    override def map[B](f: A => B): Redirectable[B] = {
      Redirect(target, f.apply(data))
    }
  }

  trait RedirectAnalyzer[T]{
    def analyze(t: T): Redirectable[T]
  }

//  trait Context {
//    def manager(): CrawlingManager
//    def queue(): mutable.Iterable[Input]
//  }
  case class Context(manager: CrawlingManager, queue: mutable.Iterable[Input])

//  trait ContextBuilder {
//    def withManager(): CrawlingManager
//    def withQueue(): mutable.Iterable[Input]
//    def build(): Context
//  }

  trait UrlConfSetter {
    def setInt(key: String, value: Int)
  }

  trait UrlConfGetter {
    def getInt(key: String): Int
  }

  trait UrlConf extends UrlConfSetter with UrlConfGetter

  trait ManagerBuilder {
    def build(): CrawlingManager
  }

//  trait UrlConf {
//    def apply(extraction: URL => Unit): Unit
//  }
//
//  class DefaultUrlConf() extends UrlConf {
//    def apply(extraction: URL => Unit)
//  }


  trait Opt[A <: CrawlingManager, B] {
    def set(manager: A, pf: PartialFunction[URL, B]): A
  }

  trait CrawlerOption[A <: ManagerBuilder, B] {
    def apply(managerBuilder: A, value: B): A
  }

  abstract class PartialCrawlerOption[A <: ManagerBuilder, B] extends CrawlerOption[A, B] {
//    def apply(managerBuilder: A, value: B): ManagerBuilder
//    abstract def apply(managerBuilder: A, value: B): ManagerBuilder
  }




  object Crawler {
    def read[A <: ManagerBuilder](data: mutable.Iterable[String])(implicit managerBuilder: A): InitialBranchBuilder = {
      new CrawlerBuilder[A](managerBuilder).read(data)
    }

    def withSettings[A <: ManagerBuilder]()(implicit managerBuilder: A): CrawlerBuilder[A] = {
      new CrawlerBuilder(managerBuilder)
    }

    def withHostSettings[A <: ManagerBuilder]()(implicit managerBuilder: A): InitialConfBuilder[A] = {
      new InitialConfBuilder(managerBuilder)
    }

    def configure[A <: CrawlingManager]()(implicit manager: A): NewConfigurationBuilder[A] = {
      new NewConfigurationBuilder(manager, _ => true)
    }

    def branchConfigure[A <: CrawlingManager]()(implicit manager: A): NewMultiHostConfigurationBuilder[A] = {
      new NewMultiHostConfigurationBuilder[A](manager)
    }

//    def configure(): ConfigurationBuilder = {
//
//    }
  }

//  class CrawlerBuilder[A <: ManagerBuilder](managerBuilder: A) {
//    def read(data: mutable.Iterable[String]): InitialBranchBuilder = {
//      val q = new mutable.Queue[String]()
//      q ++= data
//      new InitialBranchBuilder(Context(managerBuilder.build(), data.map(s => InputItem(s))))
//    }
//    def option[B](option: CrawlerOption[A, B], value: B): CrawlerBuilder[A] = {
//      option.apply(managerBuilder, value)
//      this
//    }
//  }

  class NewConfigurationBuilder[A <: CrawlingManager](manager: A, predicate: URL => Boolean) {
    def set[B](option: Opt[A, B], value: B): NewConfigurationBuilder[A] = {
      val pf: PartialFunction[URL, B] = {case url if predicate.apply(url) => value}
      option.set(manager, pf)
      this
    }
//    def when(predicate: URL => Boolean): NewInitialBranchConfigurationBuilder[A] = {
//      new NewInitialBranchConfigurationBuilder[A]
//    }

    def read(data: mutable.Iterable[String]): InitialBranchBuilder = {
      val q = new mutable.Queue[String]()
      q ++= data
      new InitialBranchBuilder(Context(manager, data.map(s => InputItem(s))))
    }
  }

  class NewMultiHostConfigurationBuilder[A <: CrawlingManager](manager: A) {
    def set[B](option: Opt[A, B], value: B): NewMultiHostConfigurationBuilder[A] = {
      val pf: PartialFunction[URL, B] = {case _ if true => value}
      option.set(manager, pf)
      this
    }
    def when(predicate: URL => Boolean): NewInitialBranchConfigurationBuilder[A] = {
      new NewInitialBranchConfigurationBuilder[A](manager, predicate)
    }

//    class NewConfBranchBuilder[A <: ManagerBuilder](managerBuilder: A, predicate: URL => Boolean) {
//      def option[B](option: CrawlerOption[A, B], value: B): ConfBuilder[A] = {
//        val f: PartialFunction[URL, A] = {case url if predicate.apply(url) => option.apply(managerBuilder, value)}
//        new ConfBuilder(managerBuilder, f)
//      }
//    }
  }

  class NewInitialBranchConfigurationBuilder[A <: CrawlingManager](manager: A, predicate: URL => Boolean) {
    def set[B](option: Opt[A, B], value: B): NewHostConfigurationBuilder[A] = {
      val pf: PartialFunction[URL, B] = {case url if predicate.apply(url) => value}
      option.set(manager, pf)
      new NewHostConfigurationBuilder(manager, predicate)
    }
  }

  class NewHostConfigurationBuilder[A <: CrawlingManager](manager: A, predicate: URL => Boolean) {
    def set[B](option: Opt[A, B], value: B): NewHostConfigurationBuilder[A] = {
      val pf: PartialFunction[URL, B] = {case url if predicate.apply(url) => value}
      option.set(manager, pf)
      this
    }
    def otherwise(): NewFinalBranchConfigurationBuilder[A] = {
      new NewFinalBranchConfigurationBuilder(manager)
    }
  }

  class NewFinalBranchConfigurationBuilder[A <: CrawlingManager](manager: A) {
    def set[B](option: Opt[A, B], value: B): NewFinalBranchConfigurationBuilder[A] = {
      val pf: PartialFunction[URL, B] = {case _ if true => value}
      option.set(manager, pf)
      this
    }
    def read(data: mutable.Iterable[String]): InitialBranchBuilder = {
      val q = new mutable.Queue[String]()
      q ++= data
      new InitialBranchBuilder(Context(manager, data.map(s => InputItem(s))))
    }
  }

//    def withSettings[A <: ManagerBuilder]()(implicit managerBuilder: A): CrawlerBuilder[A] = {
//      new CrawlerBuilder(managerBuilder)
//    }
//
//    def withHostSettings[A <: UrlConf]()(implicit urlConfFactory: () => A): InitialConfBuilder[A] = {
//      new InitialConfBuilder(urlConfFactory)
//    }
//  }
//
  class InitialConfBuilder[A <: ManagerBuilder](managerBuilder: A) {
    def when(predicate: URL => Boolean): NewConfBranchBuilder[A] = {
      new NewConfBranchBuilder[A](managerBuilder, predicate)
    }
  }

  class NewConfBranchBuilder[A <: ManagerBuilder](managerBuilder: A, predicate: URL => Boolean) {
    def option[B](option: CrawlerOption[A, B], value: B): ConfBuilder[A] = {
      val f: PartialFunction[URL, A] = {case url if predicate.apply(url) => option.apply(managerBuilder, value)}
      new ConfBuilder(managerBuilder, f)
    }
  }

  class SuccessiveConfBranchBuilder[A <: ManagerBuilder](managerBuilder: A, predicate: URL => Boolean, pf: PartialFunction[URL, A]) {
    def option[B](option: CrawlerOption[A, B], value: B): ConfBuilder[A] = {
      val f: PartialFunction[URL, A] = {case url if predicate.apply(url) => option.apply(managerBuilder, value)}
      new ConfBuilder(managerBuilder, pf.orElse(f))
    }
  }

  class FinalConfBranchBuilder[A <: ManagerBuilder](managerBuilder: A, pf: PartialFunction[URL, A]) {
//    def option[B](option: CrawlerOption[A, B], value: B): FinalConfBuilder[A] = {
//      val f: PartialFunction[URL, A] = {case url if true => option.apply(mb, value)}
////      val a: PartialFunction[URL, A] = pf.orElse(f)
//      new FinalConfBuilder(pf.orElse(f))
//    }
    def option[B](option: CrawlerOption[A, B], value: B): FinalConfBuilder[A] = {
      val f: PartialFunction[URL, A] = {case _ if true => option.apply(managerBuilder, value)}
      new FinalConfBuilder(managerBuilder, pf.orElse(f))
    }

    def read(data: mutable.Iterable[String]): InitialBranchBuilder = {
      val q = new mutable.Queue[String]()
      q ++= data
      new InitialBranchBuilder(Context(null, data.map(s => InputItem(s))))
    }
  }
//
////  class NewConfBuilder[A <: ManagerBuilder](mb: A, pf: PartialFunction[URL, A]) {
////    def option[B](option: CrawlerOption[A, B], value: B): ConfBuilder[A] = {
////      val f: Function[A, A] = mb => option.apply(mb, value)
////      new ConfBuilder[A](mb, pf.andThen(f))
////    }
////  }
//
  class ConfBuilder[A <: ManagerBuilder](managerBuilder: A, pf: PartialFunction[URL, A]) {
    def option[B](option: CrawlerOption[A, B], value: B): ConfBuilder[A] = {
      val f: Function[A, A] = mb => option.apply(mb, value)
      new ConfBuilder(managerBuilder, pf.andThen(f))
    }
    def when(predicate: URL => Boolean): SuccessiveConfBranchBuilder[A] = {
      new SuccessiveConfBranchBuilder[A](managerBuilder, predicate, pf)
    }
    def otherwise(): FinalConfBranchBuilder[A] = {
      new FinalConfBranchBuilder[A](managerBuilder, pf)
    }
  }

  class FinalConfBuilder[A <: ManagerBuilder](managerBuilder: A, pf: PartialFunction[URL, A]) {
    def option[B](option: CrawlerOption[A, B], value: B): ConfBuilder[A] = {
      val f: Function[A, A] = mb => option.apply(mb, value)
      new ConfBuilder(managerBuilder, pf.andThen(f))
    }
    def read(data: mutable.Iterable[String]): InitialBranchBuilder = {
      val q = new mutable.Queue[String]()
      q ++= data
      new InitialBranchBuilder(Context(managerBuilder.build(), data.map(s => InputItem(s))))
    }
  }


  class CrawlerBuilder[A <: ManagerBuilder](managerBuilder: A) {
    def read(data: mutable.Iterable[String]): InitialBranchBuilder = {
      val q = new mutable.Queue[String]()
      q ++= data
      new InitialBranchBuilder(Context(managerBuilder.build(), data.map(s => InputItem(s))))
    }
    def option[B](option: CrawlerOption[A, B], value: B): CrawlerBuilder[A] = {
      option.apply(managerBuilder, value)
      this
    }
  }

  class FlexibleCrawlerBuilder[A <: ManagerBuilder](managerBuilder: A) {

  }

//  class SettingsBuilder((context: Context) {
////    def op
//  }

  class InitialBranchBuilder(context: Context/*queue: mutable.Iterable[Input]*/) {
    //    def fetch[Raw](fetcher: String => Raw): ParserBuilder[Raw] = {
    //      new FetcherBuilder(queue).fetch(fetcher)
    //    }

    def when(predicate: URL => Boolean): FetcherBranchBuilder = {
      new FetcherBranchBuilder(context, predicate)
    }

  }

  // Fetcher
  class FetcherBranchBuilder(context: Context, predicate: URL => Boolean) {
    def fetch[Raw](fetcher: URL => Raw)(implicit c: RedirectAnalyzer[Raw]): ParserBranchBuilder[Raw] = {
      new ParserBranchBuilder[Raw](context, {case qItem if predicate.apply(qItem.url) => Try(c.analyze(fetcher.apply(qItem.url))).recover(e => throw new FetchingException(qItem.url.toString, e)).get})
    }
  }
  class SuccessiveFetcherBranchBuilder[Doc](context: Context, predicate: URL => Boolean, partial: PartialFunction[QueueItem, Redirectable[Doc]]) {
    def fetch[Raw](fetcher: URL => Raw)(implicit c: RedirectAnalyzer[Raw]): SuccessiveParserBranchBuilder[Raw, Doc] = {
      new SuccessiveParserBranchBuilder[Raw, Doc](context, {case qItem if predicate.apply(qItem.url) => Try(c.analyze(fetcher.apply(qItem.url))).recover(e => throw new FetchingException(qItem.url.toString, e)).get}, partial)
    }
  }
  class FinalFetcherBranchBuilder[Doc](context: Context, partial: PartialFunction[QueueItem, Redirectable[Doc]]) {
    def fetch[Raw](fetcher: URL => Raw)(implicit c: RedirectAnalyzer[Raw]): FinalParserBranchBuilder[Raw, Doc] = {
      new FinalParserBranchBuilder[Raw, Doc](context, {case qItem => c.analyze(fetcher.apply(qItem.url))}, partial)
    }
  }

  //Parser

  class ParserBranchBuilder[Raw](context: Context, fetcher: PartialFunction[QueueItem, Redirectable[Raw]]) {
    def parse[Doc](parser: Raw => Doc): SubsequentBranchBuilder[Doc] = {
      val f = fetcher.andThen(k => k.map(parser))
      new SubsequentBranchBuilder[Doc](context, f)
    }
  }
  class SuccessiveParserBranchBuilder[Raw, Doc](context: Context, fetcher: PartialFunction[QueueItem, Redirectable[Raw]], partial: PartialFunction[QueueItem, Redirectable[Doc]]) {
    def parse(parser: Raw => Doc): SubsequentBranchBuilder[Doc] = {
      val f = partial.orElse(fetcher.andThen(k => k.map(parser)))
      new SubsequentBranchBuilder[Doc](context, f)
    }
  }
  class FinalParserBranchBuilder[Raw, Doc](context: Context, fetcher: PartialFunction[QueueItem, Redirectable[Raw]], partial: PartialFunction[QueueItem, Redirectable[Doc]]) {
    def parse(parser: Raw => Doc): FinalBranchBuilder[Doc] = {
      val f = partial.orElse(fetcher.andThen(k => k.map(parser)))
      new FinalBranchBuilder[Doc](context, partial.orElse(f))
    }
  }

  class SubsequentBranchBuilder[Doc](context: Context, partialParser: PartialFunction[QueueItem, Redirectable[Doc]]) {
    def when(predicate: URL => Boolean): SuccessiveFetcherBranchBuilder[Doc] = {
      new SuccessiveFetcherBranchBuilder(context, predicate, partialParser)
    }

    def otherwise(): FinalFetcherBranchBuilder[Doc] = {
      new FinalFetcherBranchBuilder[Doc](context, partialParser)
    }
  }
  class FinalBranchBuilder[Doc](context: Context, parser: QueueItem => Redirectable[Doc]) {
    def followRedirects(): RedirectHandler[Doc] = {
      new RedirectHandler[Doc](context, parser)
    }
  }

  class RedirectHandler[Doc](context: Context, parser: QueueItem => Redirectable[Doc]) {
    def write(writer: Doc => Unit): FailureHandler = {
//      val pf: PartialFunction[QueueItem, Redirectable[Doc]] = {case qItem if true => qItem.depth}
      val f = parser.andThen{
        k => k match {
          case Direct(data) => data
          case Redirect(target, data) => {
            Seq(target).concat(context.queue)// queue.concat(target)//add(target)
            data
          }
        }
      }.andThen(writer)
      new FailureHandler(context, f)
    }
  }

  //  class WriterHandler[Doc](queue: CrawlingQueue, parser: URL => Redirectable[Doc]) {
  //    def write(writer: Doc => Unit): FailureHandler = {
  //      val f = parser.andThen(k => k.data()).andThen(writer)
  //      new FailureHandler(queue, f)
  //    }
  //  }

  class FailureHandler(context: Context, writer: QueueItem => Unit) {
    def ofFailure(handler: CrawlingException => Unit): PipelineRunner = {
      new PipelineRunner(context, writer, handler)
    }
  }

  class PipelineRunner(context: Context, writer: QueueItem => Unit, errHandler: CrawlingException => Unit) {
//    val manager = new CrawlingManager {}(2000L, 1)
    def crawl()/*(implicit manager: CrawlingManager)*/: Unit = {
      context.queue.foreach{
        inItem =>
//          val inItem = InputItem(url)
          inItem.onPickUp()
          try {
            val qItem = Try(QueueItem(inItem, new URL(inItem.url), 0)).recover(e => throw new URLParsingException(inItem.url, e)).get
            context.manager.manage(qItem, writer, inItem.onSuccess, inItem.onFailure)
          } catch {
            case exc: CrawlingException => {
              inItem.onFailure(exc)
              errHandler.apply(exc)
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
}
