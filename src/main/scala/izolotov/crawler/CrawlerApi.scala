package izolotov.crawler

import java.net.URL

import izolotov.crawler.CrawlerApi.{InitialBranchBuilder, ManagerBuilder}
import izolotov.crawler.CrawlerInput.{CrawlingQueue, Input, InputItem, QueueItem}
import izolotov.crawler.CrawlerParameterBuilder.DefaultBuilder
import izolotov.crawler.DefaultCrawler.HostQueueManager

import scala.collection.mutable

//import izolotov.Sandbox.{CrawlingQueue, InputItem, Manager, QueueItem}

import scala.collection.mutable
import scala.util.{Try,Success,Failure}

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

  case class RichContext(manager: RichManager, queue: mutable.Iterable[Input])

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


//  trait RichOption[Manager <: RichManager, Value] {
//    def set(manager: Manager, pf: PartialFunction[URL, Value]): Manager
//  }
//
//  sealed trait MandatoryRichOption[Manager <: RichManager, Value] extends RichOption[Manager, Value]
//
//  trait RichFetcher[Manager <: RichManager, Raw] extends MandatoryRichOption[Manager, Raw]
//
//  trait Opt[A <: CrawlingManager, B] {
//    def set(manager: A, pf: PartialFunction[URL, B]): A
//  }

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

//    def branchConfigure[A <: CrawlingManager]()(implicit manager: A): NewMultiHostConfigurationBuilder[A] = {
//      new NewMultiHostConfigurationBuilder[A](manager)
//    }

//    def richConfigure[A <: RichManager]()(implicit manager: A): RichConfigurationDefaultBuilder[A] = {
//      new RichConfigurationDefaultBuilder[A](manager)
//    }
//
//    def newRichConfigure[A <: RichManager]()(implicit manager: A): RichConfigurationDefaultBuilder[A] = {
//      new RichConfigurationDefaultBuilder[A](manager)
//    }

    def conf(): CrawlerParameterBuilder.ConfBuilder = {
      new CrawlerParameterBuilder.ConfBuilder()
    }

  }

//  trait HasManager[Manager] {
//    def manager(): Manager
//  }
//
//  trait OptionSetter[Manager, Value, Raw, Doc] extends HasManager[Manager] {
//    def set[Value](option: RichOption[Manager, Value], value: Value): RichConfigurationDefaultOptionBuilder[Raw, Doc, Manager] = {
//      val pf: PartialFunction[URL, Value] = {case _ if true => value}
//      option.set(manager, pf)
//      this
//    }
//  }


//  class RichConfigurationDefaultBuilder[Manager <: RichManager](manager: Manager) {
//    def default(): RichConfigurationDefaultFetcherBuilder[Manager] = {
//      new RichConfigurationDefaultFetcherBuilder(manager)
//    }
//  }
//
//  class RichConfigurationDefaultFetcherBuilder[Manager <: RichManager](manager: Manager) {
//    def setFetcher[Raw](fetcher: URL => Raw)(implicit c: RedirectAnalyzer[Raw]): RichConfigurationDefaultParserBuilder[Raw, Manager] = {
//      new RichConfigurationDefaultParserBuilder[Raw, Manager](manager, fetcher)
//    }
//  }
//  class RichConfigurationDefaultParserBuilder[Raw, Manager <: RichManager](manager: Manager, defFetcher: URL => Raw) {
//    def setParser[Doc](parser: Raw => Doc): RichConfigurationDefaultWriterBuilder[Raw, Doc, Manager] = {
//      new RichConfigurationDefaultWriterBuilder[Raw, Doc, Manager](manager, defFetcher, parser)
//    }
//  }
//  class RichConfigurationDefaultWriterBuilder[Raw, Doc, Manager <: RichManager](manager: Manager,
//                                                                                defFetcher: URL => Raw,
//                                                                                defParser: Raw => Doc) {
//    def setWriter(writer: Doc => Unit): RichConfigurationDefaultOptionBuilder[Raw, Doc, Manager] = {
//      val pf: PartialFunction[URL, URL => Unit] = {case _ if true => defFetcher.andThen(defParser).andThen(writer)}
//      manager.setExtractor(pf)
//      new RichConfigurationDefaultOptionBuilder[Raw, Doc, Manager](manager, (defFetcher, defParser, writer))
//    }
//  }
//  class RichConfigurationDefaultOptionBuilder[Raw, Doc, Manager <: RichManager](manager: Manager,
//                                                                                default: (URL => Raw, Raw => Doc, Doc => Unit)) {
//    def set[Value](option: RichOption[Manager, Value], value: Value): RichConfigurationDefaultOptionBuilder[Raw, Doc, Manager] = {
//      val pf: PartialFunction[URL, Value] = {case _ if true => value}
//      option.set(manager, pf)
//      this
//    }
//    def when(predicate: URL => Boolean): RichBranchConfigurationFetcherBuilder[Raw, Doc, Manager] = {
//      new RichBranchConfigurationFetcherBuilder[Raw, Doc, Manager](predicate, manager, default)
//    }
//    def read(data: mutable.Iterable[String]): ForEachBuilder = {
//      val q = new mutable.Queue[String]()
//      q ++= data
//      new ForEachBuilder(RichContext(manager, data.map(s => InputItem(s))))
//    }
//  }

//  class RichBranchConfigurationFetcherBuilder[DefRaw, DefDoc, Manager <: RichManager](predicate: URL => Boolean,
//                                                                                      manager: Manager,
//                                                                                      default: (URL => DefRaw, DefRaw => DefDoc, DefDoc => Unit)) {
//    def setFetcher[Raw](fetcher: URL => Raw)(implicit c: RedirectAnalyzer[Raw]): RichBranchConfigurationParserBuilder[Raw, DefRaw, DefDoc, Manager] = {
//      new RichBranchConfigurationParserBuilder[Raw, DefRaw, DefDoc, Manager](predicate, manager, fetcher, default)
//    }
//    def setParser[Doc](parser: DefRaw => Doc): RichBranchConfigurationWriterBuilder[DefRaw, Doc, DefRaw, DefDoc, Manager] = {
//      new RichBranchConfigurationWriterBuilder[DefRaw, Doc, DefRaw, DefDoc, Manager](predicate, manager, default._1.andThen(parser), default)
//    }
//    def setWriter[Doc](writer: DefDoc => Unit): RichConfigurationOptionBuilder[DefRaw, DefDoc, Manager] = {
//      val pf: PartialFunction[URL, URL => Unit] = {case url if predicate.apply(url) => default._1.andThen(default._2).andThen(writer)}
//      manager.setExtractor(pf)
//      new RichConfigurationOptionBuilder[DefRaw, DefDoc, Manager](predicate, manager, default)
//    }
//    def set[Value](option: RichOption[Manager, Value], value: Value): RichConfigurationOptionBuilder[DefRaw, DefDoc, Manager] = {
//      val pf: PartialFunction[URL, Value] = {case url if predicate(url) => value}
//      option.set(manager, pf)
//      new RichConfigurationOptionBuilder[DefRaw, DefDoc, Manager](predicate, manager, default)
//    }
//  }
//  class RichBranchConfigurationParserBuilder[Raw, DefRaw, DefDoc, Manager <: RichManager](predicate: URL => Boolean,
//                                                                                          manager: Manager,
//                                                                                          pipeline: URL => Raw,
//                                                                                          default: (URL => DefRaw, DefRaw => DefDoc, DefDoc => Unit)) {
//    def setParser[Doc](parser: Raw => Doc): RichBranchConfigurationWriterBuilder[Raw, Doc, DefRaw, DefDoc, Manager] = {
//      new RichBranchConfigurationWriterBuilder[Raw, Doc, DefRaw, DefDoc, Manager](predicate, manager, pipeline.andThen(parser), default)
//    }
//
//  }
//  class RichBranchConfigurationWriterBuilder[Raw, Doc, DefRaw, DefDoc, Manager <: RichManager](predicate: URL => Boolean,
//                                                                                               manager: Manager,
//                                                                                               pipeline: URL => Doc,
//                                                                                               default: (URL => DefRaw, DefRaw => DefDoc, DefDoc => Unit)) {
//    def setWriter(writer: Doc => Unit): RichConfigurationOptionBuilder[DefRaw, DefDoc, Manager] = {
//      val pf: PartialFunction[URL, URL => Unit] = {case url if predicate(url) => pipeline.andThen(writer)}
//      manager.setExtractor(pf)
//      new RichConfigurationOptionBuilder[DefRaw, DefDoc, Manager](predicate, manager, default)
//    }
//  }
//
//  class RichConfigurationOptionBuilder[Raw, Doc, Manager <: RichManager](predicate: URL => Boolean,
//                                                                         manager: Manager,
//                                                                         default: (URL => Raw, Raw => Doc, Doc => Unit)) {
//    def set[Value](option: RichOption[Manager, Value], value: Value): RichConfigurationOptionBuilder[Raw, Doc, Manager] = {
//      val pf: PartialFunction[URL, Value] = {case url if predicate(url) => value}
//      option.set(manager, pf)
//      this
//    }
//    def when(predicate: URL => Boolean): RichBranchConfigurationFetcherBuilder[Raw, Doc, Manager] = {
//      new RichBranchConfigurationFetcherBuilder[Raw, Doc, Manager](predicate, manager, default)
//    }
//    def read(data: mutable.Iterable[String]): ForEachBuilder = {
//      val q = new mutable.Queue[String]()
//      q ++= data
//      new ForEachBuilder(RichContext(manager, data.map(s => InputItem(s))))
//    }
//  }

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
//    def set[B](option: Opt[A, B], value: B): NewConfigurationBuilder[A] = {
//      val pf: PartialFunction[URL, B] = {case url if predicate.apply(url) => value}
//      option.set(manager, pf)
//      this
//    }
//    def when(predicate: URL => Boolean): NewInitialBranchConfigurationBuilder[A] = {
//      new NewInitialBranchConfigurationBuilder[A]
//    }

    def read(data: mutable.Iterable[String]): InitialBranchBuilder = {
      val q = new mutable.Queue[String]()
      q ++= data
      new InitialBranchBuilder(Context(manager, data.map(s => InputItem(s))))
    }
  }

//  class NewMultiHostConfigurationBuilder[A <: CrawlingManager](manager: A) {
////    def set[B](option: Opt[A, B], value: B): NewMultiHostConfigurationBuilder[A] = {
////      val pf: PartialFunction[URL, B] = {case _ if true => value}
////      option.set(manager, pf)
////      this
////    }
//    def when(predicate: URL => Boolean): NewInitialBranchConfigurationBuilder[A] = {
//      new NewInitialBranchConfigurationBuilder[A](manager, predicate)
//    }
//
////    class NewConfBranchBuilder[A <: ManagerBuilder](managerBuilder: A, predicate: URL => Boolean) {
////      def option[B](option: CrawlerOption[A, B], value: B): ConfBuilder[A] = {
////        val f: PartialFunction[URL, A] = {case url if predicate.apply(url) => option.apply(managerBuilder, value)}
////        new ConfBuilder(managerBuilder, f)
////      }
////    }
//  }

//  class NewInitialBranchConfigurationBuilder[A <: CrawlingManager](manager: A, predicate: URL => Boolean) {
//    def set[B](option: Opt[A, B], value: B): NewHostConfigurationBuilder[A] = {
//      val pf: PartialFunction[URL, B] = {case url if predicate.apply(url) => value}
//      option.set(manager, pf)
//      new NewHostConfigurationBuilder(manager, predicate)
//    }
//  }

//  class NewHostConfigurationBuilder[A <: CrawlingManager](manager: A, predicate: URL => Boolean) {
//    def set[B](option: Opt[A, B], value: B): NewHostConfigurationBuilder[A] = {
//      val pf: PartialFunction[URL, B] = {case url if predicate.apply(url) => value}
//      option.set(manager, pf)
//      this
//    }
//    def otherwise(): NewFinalBranchConfigurationBuilder[A] = {
//      new NewFinalBranchConfigurationBuilder(manager)
//    }
//  }

//  class NewFinalBranchConfigurationBuilder[A <: CrawlingManager](manager: A) {
//    def set[B](option: Opt[A, B], value: B): NewFinalBranchConfigurationBuilder[A] = {
//      val pf: PartialFunction[URL, B] = {case _ if true => value}
//      option.set(manager, pf)
//      this
//    }
//    def read(data: mutable.Iterable[String]): InitialBranchBuilder = {
//      val q = new mutable.Queue[String]()
//      q ++= data
//      new InitialBranchBuilder(Context(manager, data.map(s => InputItem(s))))
//    }
//  }

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

  class ExtractorBuilder[A](context: RichContext) {
    def extract(): ForEachBuilder[A] = {
      new ForEachBuilder[A](context)
    }
  }

  class ForEachBuilder[A](context: RichContext) {
    def forEach[A, B](fn: A => B): Unit = {
      context.queue.foreach{
        inItem =>
          inItem.onPickUp()
          try {
            QueueItem(inItem, new URL(inItem.url), 0)
//            val qItem = Try(QueueItem(inItem, new URL(inItem.url), 0)).recover(e => throw new URLParsingException(inItem.url, e)).get
            val f = context.manager.manage(QueueItem(inItem, new URL(inItem.url), 0))
          } catch {
            case exc: CrawlingException => {
              inItem.onFailure(exc)
            }
          }
      }
    }
  }
  class RichStarterBuilder(f: Function[String, Unit], data: mutable.Iterable[String]) {
    def crawl(): Unit = {
      data.foreach(item => f.apply(item))
    }
  }

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
