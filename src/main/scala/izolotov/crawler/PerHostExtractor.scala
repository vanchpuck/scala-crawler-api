package izolotov.crawler

import java.net.URL
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{Executors, LinkedBlockingDeque, RejectedExecutionException, ThreadPoolExecutor, TimeUnit}

import com.google.common.util.concurrent.ThreadFactoryBuilder
import izolotov.FixedDelayModerator
import izolotov.crawler.PerHostExtractorLegacy.HostQueue
import javax.print.Doc

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}
import PerHostExtractor._

import scala.util.{Failure, Success, Try}

object PerHostExtractor {

  val DefaultCapacity = 20
  val DefaultDelay = 0L

  val DaemonThreadFactory = new ThreadFactoryBuilder().setDaemon(true).build

  class HostQueueIsFullException(cause: String) extends RejectedExecutionException

  class SharedQueueIsFullException(cause: String) extends RejectedExecutionException

  case class Queue(moderator: FixedDelayModerator, ec: ExecutionContextExecutorService)

//  class Queue(delay: Long, length: Int = Int.MaxValue) extends AutoCloseable {
//    val moderator = new FixedDelayModerator(delay)
//
//    val ec = ExecutionContext.fromExecutorService(new ThreadPoolExecutor(
//      1,
//      1,
//      0L,
//      TimeUnit.MILLISECONDS,
//      new LinkedBlockingDeque[Runnable](length),
//      DaemonThreadFactory,
//      (r: Runnable, e: ThreadPoolExecutor) => {
//        throw new HostQueueIsFullException(s"Task ${r.toString()} rejected from ${e.toString()}")
//      }
//    ))
//
//    def extract[Doc](url: URL, f: URL => Doc): Future[Doc] = {
//      Future {
//        moderator.apply(url, f)
//      }(ec)
//    }
//
//    def close(): Unit = {
//      ec.shutdown
//      try
//        if (!ec.awaitTermination(2, TimeUnit.SECONDS)) {
//          ec.shutdownNow
//        }
//      catch {
//        case ie: InterruptedException =>
//          ec.shutdownNow
//          Thread.currentThread.interrupt()
//      }
//    }
//  }

  class ExtractionManager[Doc](
                                parallelism: Int,
                                extract: PartialFunction[URL, URL => Doc],
                                delay: PartialFunction[URL, Long] = {case _ if true => DefaultDelay},
                                processingQueueCapacity: Int = DefaultCapacity,
                                hostQueueCapacity: Int = DefaultCapacity
                              ) extends AutoCloseable {
    val hostMap = collection.mutable.Map[String, Queue]()

    val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(parallelism))

    def extract(url: URL): Future[Doc] = {
//      delay.andThen()
      val queue = hostMap.getOrElseUpdate(url.getHost, Queue(new FixedDelayModerator(delay.apply(url)), newExecutionContext()))
      Future {
        val f = Future {
          queue.moderator.apply(url, extract(url))
        }(ec)
        // TODO add timeout parameter along with delay
        Await.result(f, Duration.Inf)
      }(queue.ec)
//      Future {
//        val f = queue.extract(url, extract(url))
//        Await.result(f, Duration.Inf)
//      }(ec)
    }

    def close(): Unit = {
      shutdown(ec)
      Try(hostMap.foreach(entry => shutdown(entry._2.ec))).recover{case _ => println("Can't close a host queue")}
    }

    private def newExecutionContext(): ExecutionContextExecutorService = ExecutionContext.fromExecutorService(new ThreadPoolExecutor(
      1,
      1,
      0L,
      TimeUnit.MILLISECONDS,
      new LinkedBlockingDeque[Runnable](hostQueueCapacity),
      DaemonThreadFactory,
      (r: Runnable, e: ThreadPoolExecutor) => {
        throw new HostQueueIsFullException(s"Task ${r.toString()} rejected from ${e.toString()}")
      }
    ))
  }

  class RegisteredAttempt[Doc](future: Future[Doc], deregister: () => Unit) extends Attempt[Doc] {
    override def apply[Out, Err](f: Doc => Out, err: Throwable => Err): Unit = {
      future.onComplete{ t =>
        t.map(doc => f.apply(doc)).recover({case exc if true => err.apply(exc)})
        deregister()
      // TODO check execution context
      }(ExecutionContext.global)
    }
  }

  def shutdown(executor: ExecutionContextExecutorService): Unit = {
    executor.shutdown
    try
      if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
        executor.shutdownNow
      }
    catch {
      case _: InterruptedException => {
        executor.shutdownNow
        Thread.currentThread.interrupt()
      }
    }
  }

}

class PerHostExtractor[Doc](
                               parallelism: Int,
                               extract: PartialFunction[URL, URL => Doc],
                               delay: PartialFunction[URL, Long] = {case _ if true => DefaultDelay},
                               processingQueueCapacity: Int = DefaultCapacity,
                               hostQueueCapacity: Int = DefaultCapacity
                             ) extends AutoCloseable with Extractor[Doc] {
  var counter: Int = 0
  val lock = new ReentrantLock()
  val condition = lock.newCondition()
  val extractionManager = new ExtractionManager[Doc](
    parallelism,
    extract,
    delay,
    processingQueueCapacity,
    hostQueueCapacity
  )

  override def extract(url: URL): Attempt[Doc] = {
    try
      new RegisteredAttempt(extractionManager.extract(url), deregisterAttempt)
    finally
      registerAttempt()
  }

  def registerAttempt(): Unit = {
    lock.lock()
    try
      counter+=1
    finally
      lock.unlock()
  }

  def deregisterAttempt(): Unit = {
    lock.lock()
    try {
      counter -= 1
      condition.signal()
    } finally
      lock.unlock()
  }

  override def close(): Unit = {
    lock.lock()
    try {
      while (counter != 0) {
        condition.await()
      }
    } finally {
      extractionManager.close()
      lock.unlock()
    }
  }
}
