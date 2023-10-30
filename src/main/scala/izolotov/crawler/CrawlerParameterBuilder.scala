package izolotov.crawler

import java.net.URL

import izolotov.crawler.CrawlerApi.{Direct, ForEachBuilder, Redirect, RichContext}
import izolotov.crawler.CrawlerConf.DefaultBuilder
import izolotov.crawler.CrawlerInput.InputItem
import izolotov.crawler.CrawlerParameterBuilder.Conf

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.{Failure, Success}

object CrawlerParameterBuilder {

  class Branch[DefRaw, DefDoc](conf: Conf[DefDoc], default: DefaultConf[DefRaw, DefDoc]) {
    def when(predicate: URL => Boolean): BranchBuilder[DefRaw, DefDoc] = {
      new BranchBuilder(conf, default, predicate)
    }
    def read(urls: mutable.Iterable[String]): ExtractorBuilder[DefDoc] = {
//      new ForeachBuilder[DefDoc](conf, urls)
      new ExtractorBuilder[DefDoc](conf, urls)
//      new ForEachBuilder[DefDoc](RichContext[DefDoc](new RichCrawler[DefDoc](conf), data.map(s => InputItem(s))))
    }
  }

  class ExtractorBuilder[Doc](conf: Conf[Doc], urls: mutable.Iterable[String]) {
    def extract()(implicit factory: Conf[Doc] => Manager[Future[Doc]]): ForeachBuilder1[Doc] = {

      new ForeachBuilder1[Doc](factory.apply(conf).extract(urls.iterator))
    }
  }

  class ForeachBuilder1[Doc](docs: Iterator[Future[Doc]]) {
    def foreach[Out](fn: Doc => Out): Unit = {
      docs.foreach(
        f =>
          f.onComplete{
            case Success(doc) => fn.apply(doc)
            case Failure(exc) => println(exc)
          }(ExecutionContext.global)
      )
    }
  }

  class ForeachBuilder[Doc](conf: Conf[Doc], urls: mutable.Iterable[String]) {
    def foreach[A, Out](fn: A => Out)(implicit factory: Conf[Doc] => Manager[A]): Unit = {
      val m: Manager[A] = factory.apply(conf)
      val iter: Iterator[String] = urls.iterator
      m.extract(iter).foreach(a => fn.apply(a))
//      m.extract(iter).foreach(println)
      null
    }
  }

  class Conf[Doc](val parallelism: Int,
                  var extractor: PartialFunction[URL, URL => Doc],
                  var delay: PartialFunction[URL, Long],
                  var followRedirectPattern: PartialFunction[URL, URL => Boolean]) {
    def extractor(extractor: PartialFunction[URL, URL => Doc]): Unit = {
      this.extractor = extractor.orElse(this.extractor)
    }
    def delay(delay: PartialFunction[URL, Long]): Unit = {
      this.delay = delay.orElse(this.delay)
    }
  }

  class QueuePlaceholder {
    private var _queue: mutable.Iterable[String] = null
    def queue: mutable.Iterable[String] = _queue
    def queue_=(newValue: mutable.Iterable[String]): Unit = {
      _queue = newValue
    }
  }

  case class DefaultConf[DefRaw, DefDoc](fetcher: URL => DefRaw,
                                         parser: DefRaw => DefDoc,
                                         delay: Long = 0L)

  class ConfBuilder() {
    def default(): DefaultBuilder = {
      new DefaultBuilder()
    }
  }

  class DefaultBuilder() {
    def set[Raw, Doc](
                             fetcher: URL => Raw,
                             parser: Raw => Doc,
//                             writer: Doc => Unit,
                             parallelism: Int = 10,
                             delay: Long = 0L,
                             followRedirectPattern: URL => Boolean
                           ): Branch[Raw, Doc] = {
      val conf = new Conf(
        parallelism,
        {case _ if true => fetcher.andThen(parser)},
        {case _ if true => delay},
        {case _ if true => followRedirectPattern},
      )
      new Branch[Raw, Doc](conf, DefaultConf(fetcher, parser, delay))
    }
  }

  class BranchBuilder[Raw, Doc](conf: Conf[Doc], default: DefaultConf[Raw, Doc], predicate: URL => Boolean) {
    def set(fetcher: URL => Raw = default.fetcher,
            parser: Raw => Doc = default.parser,
//            writer: Doc => Unit = default.writer,
            delay: Long = 0L): Branch[Raw, Doc] = {
      val pf: PartialFunction[URL, URL => Doc] = {
        case url if predicate(url) => fetcher.andThen(parser)
      }
      val pfDelay: PartialFunction[URL, Long] = {
        case url if predicate(url) => delay
      }
      conf.extractor(pf)
      conf.delay(pfDelay)
      new Branch[Raw, Doc](conf, default)
    }


//    def set[X: ClassTag](delay: Long = default.delay): Branch[DefRaw, DefDoc] = {
//      this.setDef(default.fetcher, default.parser, default.writer, delay)
//    }

    private def setDef(fetcher: URL => Raw,
                                 parser: Raw => Doc,
                                 writer: Doc => Unit,
                                 delay: Long = default.delay): Branch[Raw, Doc] = {
      val pf: PartialFunction[URL, URL => Doc] = {
        case url if predicate(url) => fetcher.andThen(parser)
      }
      val pfDelay: PartialFunction[URL, Long] = {
        case url if predicate(url) => delay
      }
      conf.extractor(pf)
//      Some(delay).map(value => {case url if predicate(url) => value})
      conf.delay(pfDelay)
      new Branch[Raw, Doc](conf, default)
    }
    //    def set[Raw, Doc](fetcher: URL => Raw, parser: Raw => Doc, writer: Doc => Unit)
  }

//  case class Branch2[DefRaw, DefDoc](conf: Conf, default: DefaultConf[DefRaw, DefDoc]) extends Branch[DefRaw, DefDoc]
//
//  case class Branch1[DefRaw, DefDoc](conf: Conf, default: DefaultConf[DefRaw, DefDoc]) extends Branch[DefRaw, DefDoc]

}
