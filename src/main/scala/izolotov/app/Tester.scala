package izolotov.app

import java.lang.module.Configuration
import java.net.URL
import java.util.concurrent.Executors

import com.google.common.util.concurrent.ThreadFactoryBuilder
import izolotov.FixedDelayModerator
import izolotov.crawler.{Attempt, CrawlerParameterBuilder, Extractor, Manager}
import izolotov.crawler.RichCrawler.HostQueue

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object Tester {
  class Conf[Doc](var extractor: PartialFunction[URL, URL => Doc]) {
    def extractor(extractor: PartialFunction[URL, URL => Doc]): Unit = {
      this.extractor = extractor.orElse(this.extractor)
    }
  }

  class PerHostExtractor[Doc](configuration: Configuration[Doc]) extends Extractor[Doc] {
    val hostMap = collection.mutable.Map[String, HostQueue]()

    val threadFactory = new ThreadFactoryBuilder().setDaemon(false).build
    val sharedEC = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(configuration.parallelism, threadFactory))
    val onCompleteEC = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1, threadFactory))

    private def ec(): ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1, threadFactory))

    val moderator = new FixedDelayModerator(1000L)

    override def extract(url: URL): Attempt[Doc] = {
      val queue = hostMap.getOrElseUpdate(url.getHost, HostQueue(ec(), new FixedDelayModerator(configuration.delayMapping.apply(url))))
      val f = Future{
        val f = Future {
          Thread.sleep(1000L)
          queue.moderator.apply(url, configuration.mapping.apply(url))
        }(sharedEC)
        Await.result(f, Duration.Inf)
      }(queue.ec)
      new FutureAttempt(f)
    }
  }

  class FutureAttempt[Doc](future: Future[Doc]) extends Attempt[Doc] {
    override def apply[Out](f: Doc => Out): Unit = {
      future.onComplete{
        case Success(doc) => f.apply(doc)
        case Failure(exc) => println(exc)
      }(ExecutionContext.global)
    }
  }

  class ExtractionManager[Doc](configuration: Configuration[Doc]) {

    val hostMap = collection.mutable.Map[String, HostQueue]()

    val threadFactory = new ThreadFactoryBuilder().setDaemon(false).build
    val sharedEC = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10, threadFactory))
    val onCompleteEC = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1, threadFactory))

    private def ec(): ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1, threadFactory))

    val moderator = new FixedDelayModerator(1000L)

    def extract(task: Task[Doc]): Result[Doc] = {
      val queue = hostMap.getOrElseUpdate(task.url.getHost, HostQueue(ec(), new FixedDelayModerator(task.delay)))
      val f = Future{
        val f = Future {
          queue.moderator.apply(task.url, task.extractor)
        }(sharedEC)
        Await.result(f, Duration.Inf)
      }(queue.ec)
      Result(task.url, f)
//      val f = Future {
//        moderator.apply(task.url, task.extractor)
//      }(sharedEC)
//      Result(task.url, Await.result(f, Duration.Inf))
    }
  }

//  class Extractor[Doc] {
//    def urlToDoc(url: URL): Doc = {
//
//    }
//  }onComplete
  case class Configuration[Doc](
                                 mapping: PartialFunction[URL, URL => Doc],
                                 delayMapping: PartialFunction[URL, Long],
                                 parallelism: Int
                               ) {
//    def mapping: PartialFunction[URL, URL => String] = {case url if true => url.getHost}
//    def urlToTask(url: URL): Task[Doc] = {
//      Task(url, mapping.apply(url), delayMapping.apply(url))
//    }
  }

  case class Task[Doc](url: URL, extractor: URL => Doc, delay: Long)
  trait Res[Doc] {
    def applyToRes[Out](f: Doc => Out)
  }
  case class Result[Doc](url: URL, future: Future[Doc]) extends Res[Doc] {
    def applyToRes[Out](f: Doc => Out): Unit = {
      future.onComplete{
        case Success(doc) => f.apply(doc)
        case Failure(exc) => println(exc)
      }(ExecutionContext.global)
//      f.apply(doc)
    }
  }
  case class Output[Doc, Out](f: Doc => Out)

  object CrawlerImpl {
    def crawl[Doc, U](urls: Seq[String], conf: Configuration[Doc], forEach: Doc => U, forErr: Throwable => Unit): Unit = {
      val strToUrl: String => Try[URL] = url => Try(new URL(url))
      val extr = new PerHostExtractor[Doc](conf)
      urls.map(strToUrl)
        .map((t: Try[URL]) => t.map(extr.extract))
        .foreach{
          t: Try[Attempt[Doc]] =>
            t match {
              case Success(a) => a.apply(forEach)
              case Failure(e) => forErr.apply(e)
            }
        }
    }
  }
//  def urlToDoc(url: URL): Doc
  def main(args: Array[String]): Unit = {
    val extractorMapping: PartialFunction[URL, URL => String] = {case url if true => url => url.getHost}
    val delayMapping: PartialFunction[URL, Long] = {case url if true => 1000L}
    val conf = new Configuration[String](extractorMapping, delayMapping, 10)
    val extractor = new ExtractionManager[String](conf)
    val strToUrl: String => Try[URL] = url => Try(new URL(url))
//    val resultToOut: Result[String] => String = result => result.doc
    val urlToDoc: URL => String = url => url.getHost
    val forEach: String => Unit = host => println(host)
    val forErr: Throwable => Unit = exc => println(exc)
//    val conf: Conf[String] = new Conf[String]({case url if true => })
//    val pipeline = strToUrl.andThen(conf.urlToTask).andThen(extractor.extract).andThen(res => res.applyToRes(forEach))
    val urls = Seq("malformed", "http://facebook.com", "http://example.com", "http://example.com", "http://example.com", "http://google.com", "http://example.com")
    val extr = new PerHostExtractor[String](conf)

    CrawlerImpl.crawl(urls, conf, forEach, forErr)

//    urls.map(strToUrl)
//        .map((t: Try[URL]) => t.map(extr.extract))
//        .foreach((t: Try[Attempt[String]]))
//    urls.map(strToUrl).groupBy(url => url.map(u => u.getHost))
////      .groupMap(_ => new ExtractionManager[String])(entity => entity._2.flatten)
//      .map(host => (new ExtractionManager[String](conf), host._2))
//      .map(pair => pair._2.map(url => pair._1.extract(conf.urlToTask(url))))
//      .flatten
//      .foreach(res => res.applyToRes(forEach))
//    urls.map(strToUrl).groupBy(url => url.getHost) //groupMap(_ => new ExtractionManager[String])((value: URL) => value)//.flatMap(url => pipeline.apply(url))//foreach(url => pipeline.apply(url))//((key: String, value: Seq[URL]) => "d")//.groupMap((key: Str))
//    val a = urls.toVector
//    a.gr
//    urls.foreach{
//      url =>
//        pipeline.apply(url)
////        println(out)
//    }
//    val out = pipeline.apply("http://example.com")
//    println(out)
  }
}
