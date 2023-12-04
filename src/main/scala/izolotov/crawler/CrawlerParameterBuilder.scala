package izolotov.crawler

import java.io.{BufferedReader, FileReader}
import java.net.URL
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock

import izolotov.CrawlingQueue

import scala.concurrent.Await
import scala.concurrent.duration.Duration

//import izolotov.crawler.CrawlerApi.{Direct, ForEachBuilder, Redirect, RichContext}
import izolotov.crawler.CrawlerConf.DefaultBuilder
import izolotov.crawler.CrawlerInput.InputItem
import izolotov.crawler.CrawlerParameterBuilder.Conf

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

object CrawlerParameterBuilder {

  class Branch[DefRaw, DefDoc](conf: Conf[DefDoc], default: DefaultConf[DefRaw, DefDoc], redirectProcessor: RedirectProcessor) {
    def when(predicate: URL => Boolean): BranchBuilder[DefRaw, DefDoc] = {
      new BranchBuilder(conf, default, predicate, redirectProcessor)
    }
    def read(urls: CrawlingQueue): ExtractorBuilder[DefDoc] = {
//      new ForeachBuilder[DefDoc](conf, urls)
      redirectProcessor.setQueue(urls)
      new ExtractorBuilder[DefDoc](conf, urls, redirectProcessor)
//      new ForEachBuilder[DefDoc](RichContext[DefDoc](new RichCrawler[DefDoc](conf), data.map(s => InputItem(s))))
    }
  }

  class ExtractorBuilder[Doc](conf: Conf[Doc], urls: CrawlingQueue, redirectProcessor: RedirectProcessor) {
    def extract()(implicit factory: Conf[Doc] => Extractor[Doc]): ForeachBuilder1[Doc] = {
      val extractor = factory.apply(conf)
//      val attempts = urls.iterator.map(urlStr => Try(new URL(urlStr)))
//        .map((t: Try[URL]) => t.map(extractor.extract))
//      new ForeachBuilder1[Doc](extractor, attempts, extractor)
//      new RedirectBuilder[Doc](extractor, urls)
      new ForeachBuilder1[Doc](extractor, urls)
    }
//    def followRedirect(): RedirectBuilder[Doc] = {
//      new RedirectBuilder[Doc](conf, urls, redirectProcessor)
//    }
  }

  class Iter() extends Iterator[Future[String]] {
    override def hasNext: Boolean = true

    override def next(): Future[String] = ???
  }

//  class ForeachBuilder1[Doc](extractor: Extractor[Doc], attempts: Iterator[Try[Attempt[Doc]]]) {
  class ForeachBuilder1[Doc](extractor: Extractor[Doc], urls: CrawlingQueue) {
    def foreach[Out, Err](onSuccess: Doc => Out, onErr: Throwable => Err = (exc: Throwable) => throw exc): Unit = {
      implicit val ec = ExecutionContext.global
      val strToUrl: String => URL = str => new URL(str)
      val futures = urls
        .map(item => item.markAsInProgress())
        .map{item =>
          val future = item.apply(strToUrl.andThen(extractor.extract))
          future.onComplete{t =>
            t.map(onSuccess).recover({case e if true => onErr(e)})
            item.markAsProcessed()
          }
          future
        }
      Await.result(Future.sequence(futures), Duration.Inf)
      extractor.close()
    }
//    def foreach1[Out, Err](onSuccess: Doc => Out, onErr: Throwable => Err = (exc: Throwable) => throw exc): Unit = {
//      attempts.foreach(
//        t => {
//          t match {
//            case Success(a) => {
//              a.apply(onSuccess)
//            }
//            case Failure(e) => {
////              throw e
//              try {
//                onErr.apply(e)
//              } catch {
//                case ex: Exception => throw ex
//              }
//            }
//          }
//        }
//      )
//      extractor.close()
//    }
  }

//  class ExtractorBuilder2[Doc](conf: Conf[Doc], urls: mutable.Iterable[String]) {
//    def extract()(implicit f: Conf[Doc] => Manager[Future[Doc]]): ForeachBuilder1[Doc] = {
//      new ForeachBuilder1[Doc](factory.apply(conf).extract(urls.iterator))
//    }
//  }

//  class ForeachBuilder2[Doc](docs: Iterator[Future[Doc]]) {
//    def foreach[Out](fn: Doc => Out): Unit = {
//      docs.foreach(
//        f =>
//          f.onComplete{
//            case Success(doc) => fn.apply(doc)
//            case Failure(exc) => println(exc)
//          }(ExecutionContext.global)
//      )
//    }
//  }

//  class ForeachBuilder[Doc](conf: Conf[Doc], urls: mutable.Iterable[String]) {
//    def foreach[A, Out](fn: A => Out)(implicit factory: Conf[Doc] => Manager[A]): Unit = {
//      val m: Manager[A] = factory.apply(conf)
//      val iter: Iterator[String] = urls.iterator
//      m.extract(iter).foreach(a => fn.apply(a))
////      m.extract(iter).foreach(println)
//      null
//    }
//  }

  class Conf[Doc](val parallelism: Int,
                  var extractor: PartialFunction[URL, URL => Doc],
                  var delay: PartialFunction[URL, Long],
                  var followRedirectPattern: PartialFunction[URL, URL => Boolean]
                 ) {
    def extractor(extractor: PartialFunction[URL, URL => Doc]): Unit = {
      this.extractor = extractor.orElse(this.extractor)
    }
    def delay(delay: PartialFunction[URL, Long]): Unit = {
      this.delay = delay.orElse(this.delay)
    }
  }

  case class DefaultConf[DefRaw, DefDoc](
                                          fetcher: URL => DefRaw,
                                          parser: DefRaw => DefDoc,
                                          delay: Long = 0L,
                                          redirectPattern: URL => Boolean
                                        )

  class ConfBuilder() {
    def default(): DefaultBuilder = {
      new DefaultBuilder(new RedirectProcessor())
    }
  }

  class RedirectProcessor() {
    private var queue: CrawlingQueue = null

    def setQueue(queue: CrawlingQueue): Unit = {
      this.queue = queue
    }

    def process(url: URL): Unit = {
      queue.add(url.toString)
//      val applied = extractor.apply(raw)
//      if (applied.isDefined) {
//        queue.add(applied.get.toString)
//      }
//      raw
    }
  }

  object RedProcessor {
    def process[Raw](raw: Raw)/*(implicit extractor: Raw => Option[URL])*/: Raw = {
      println("Redirect")
      raw
    }
  }

//  object RedirectProcessor {
//    def process[Raw](raw: Raw): Raw = {
//      println("Redirect: " + raw)
//      raw
//    }
//  }

  case class Result()

  class DefaultBuilder(redirectProcessor: RedirectProcessor) {
    def set[Raw, Doc](
                       fetcher: URL => Raw,
                       parser: Raw => Doc,
                       parallelism: Int = 10,
                       delay: Long = 0L,
                       redirectPattern: URL => Boolean = _ => true,
                       outLinkPattern: URL => Boolean = _ => true
                     )(implicit redirectExtractor: Raw => Option[URL], outLinkExtractor: Raw => Iterable[URL]): Branch[Raw, Doc] = {
      val conf = new Conf(
        parallelism,
        {case _ if true =>
          fetcher
            .andThen(raw => {redirectExtractor(raw).foreach(url => if (redirectPattern(url)) redirectProcessor.process(url)); raw})
            .andThen(raw => {outLinkExtractor(raw).foreach(url => if (outLinkPattern(url)) redirectProcessor.process(url)); raw})
            .andThen(parser)
        },
        {case _ if true => delay},
        {case _ if true => redirectPattern},
      )
      new Branch[Raw, Doc](conf, DefaultConf(fetcher, parser, delay, redirectPattern), redirectProcessor)
    }
  }

  class BranchBuilder[Raw, Doc](conf: Conf[Doc], default: DefaultConf[Raw, Doc], predicate: URL => Boolean, redirectProcessor: RedirectProcessor) {
    def set(
             fetcher: URL => Raw = default.fetcher,
             parser: Raw => Doc = default.parser,
             delay: Long = default.delay,
             redirectPattern: URL => Boolean = _ => true
           )(implicit redirectExtractor: Raw => Option[URL]): Branch[Raw, Doc] = {
      val pf: PartialFunction[URL, URL => Doc] = {
        case url if predicate(url) =>
          fetcher
            .andThen(raw => {redirectExtractor(raw).foreach(url => if (redirectPattern(url)) redirectProcessor.process(url)); raw})
            .andThen(parser)
      }
      val pfDelay: PartialFunction[URL, Long] = {
        case url if predicate(url) => delay
      }
      conf.extractor(pf)
      conf.delay(pfDelay)
      new Branch[Raw, Doc](conf, default, redirectProcessor)
    }

  }

//  case class Branch2[DefRaw, DefDoc](conf: Conf, default: DefaultConf[DefRaw, DefDoc]) extends Branch[DefRaw, DefDoc]
//
//  case class Branch1[DefRaw, DefDoc](conf: Conf, default: DefaultConf[DefRaw, DefDoc]) extends Branch[DefRaw, DefDoc]

}
