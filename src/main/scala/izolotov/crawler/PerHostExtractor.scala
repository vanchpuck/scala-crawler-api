package izolotov.crawler

import java.net.URL
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{Executors, LinkedBlockingDeque, RejectedExecutionException, ThreadPoolExecutor, TimeUnit}

import com.google.common.util.concurrent.ThreadFactoryBuilder
import izolotov.{DelayedApplier, FixedDelayModerator}
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

  class ProcessingQueueIsFullException(cause: String) extends RejectedExecutionException

//  case class Queue(moderator: FixedDelayModerator, ec: ExecutionContextExecutorService)

  class Queue[Doc](capacity: Int = Int.MaxValue)(implicit globalEC: ExecutionContext) extends AutoCloseable {

    private val localEC = ExecutionContext.fromExecutorService(new ThreadPoolExecutor(
      1,
      1,
      0L,
      TimeUnit.MILLISECONDS,
      new LinkedBlockingDeque[Runnable](capacity),
      DaemonThreadFactory,
      (r: Runnable, e: ThreadPoolExecutor) => {
        throw new HostQueueIsFullException(s"Task ${r.toString()} rejected from ${e.toString()}")
      }
    ))

    private val applier = new DelayedApplier

    def extract(url: URL, extract: URL => Doc, delay: Long = 0L): Future[Doc] = {
      Future {
        val f = Future {
          applier.apply(url, extract, delay)
        }(globalEC)
        // TODO add timeout parameter along with delay
        Await.result(f, Duration.Inf)
      }(localEC)
    }

    def close(): Unit = {
      shutdown(localEC)
    }
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
                              delay: PartialFunction[URL, Long],
                              processingQueueCapacity: Int = DefaultCapacity,
                              hostQueueCapacity: Int = DefaultCapacity
                            ) extends AutoCloseable with Extractor[Doc] {
  val hostMap = collection.mutable.Map[String, Queue[Doc]]()

  implicit val ec = ExecutionContext.fromExecutorService(new ThreadPoolExecutor(
    parallelism,
    parallelism,
    0L,
    TimeUnit.MILLISECONDS,
    new LinkedBlockingDeque[Runnable](processingQueueCapacity),
    DaemonThreadFactory,
    (r: Runnable, e: ThreadPoolExecutor) => {
      // TODO block instead of throw an exception
      throw new ProcessingQueueIsFullException(s"Task ${r.toString()} rejected from ${e.toString()}")
    }
  ))

  var counter: Int = 0
  val lock = new ReentrantLock()
  val condition = lock.newCondition()

//  def extract(url: URL): RegisteredAttempt[Doc] = {
//    val queue = hostMap.getOrElseUpdate(url.getHost, new Queue(hostQueueCapacity))
//    try
//      new RegisteredAttempt(queue.extract(url, extract(url), delay(url)), deregisterAttempt)
//    finally
//      registerAttempt()
//  }
  def extract(url: URL): Future[Doc] = {
    val queue = hostMap.getOrElseUpdate(url.getHost, new Queue(hostQueueCapacity))
    queue.extract(url, extract(url), delay(url))
//    try
//      new RegisteredAttempt(queue.extract(url, extract(url), delay(url)), deregisterAttempt)
//    finally
//      registerAttempt()
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
      shutdown(ec)
      //        Try(hostMap.foreach(entry => shutdown(entry._2.ec))).recover{case _ => println("Can't close a host queue")}
      lock.unlock()
    }
  }
}
