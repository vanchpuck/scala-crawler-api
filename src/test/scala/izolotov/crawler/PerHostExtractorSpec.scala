package izolotov.crawler

import java.net.URL
import java.util.concurrent.{CopyOnWriteArrayList, Executors, RejectedExecutionException, TimeUnit}

import collection.mutable.{ArrayBuffer, Buffer, Seq}
import org.scalatest.flatspec.AnyFlatSpec
import PerHostExtractorSpec._
import izolotov.crawler
import izolotov.crawler.PerHostExtractor.{HostQueueIsFullException, SharedQueueIsFullException}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Using

import scala.jdk.CollectionConverters._

object PerHostExtractorSpec {
  val ExampleCom = new URL("http://example.com")
  val ExampleNet = new URL("http://example.net")

  val TIMEOUT_SECONDS = 10L

  def getMinDelay(requests: Seq[Request]): Long = {
    requests.sliding(2, 1).map(slide => slide(1).startTs - slide(0).endTs).min
  }

  case class Request(url: URL, startTs: Long, endTs: Long)

  class ServerMock(break: Long = 0L) {
//    val executor = Executors.newFixedThreadPool(100)
//    var _requests: collection.mutable.Seq[Request] = ArrayBuffer[Request]()
    var _requests = new CopyOnWriteArrayList[Request]()

    //    implicit val ec = ExecutionContext.fromExecutor(executor)

    def requests = _requests

    def call(url: URL): Unit = {
      val startTs = System.currentTimeMillis()
      Thread.sleep(break)
      _requests.add(Request(url, startTs, System.currentTimeMillis()))
    }

//    def close(): Unit = {
//      executor.shutdown
//      try
//        if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
//          executor.shutdownNow
//        }
//      catch {
//        case ie: InterruptedException =>
//          executor.shutdownNow
//          Thread.currentThread.interrupt()
//      }
//    }
  }
}

class PerHostExtractorSpec extends AnyFlatSpec {

  implicit val ec = ExecutionContext.global

  behavior of "a queue"

  it should "should extract all targets" in {
    val delay = 30L
    val server = new ServerMock()
    val queue = new crawler.PerHostExtractor.Queue(delay)
    val futures = Future.sequence(Seq.fill(3)(queue.extract(ExampleCom, server.call)))
    Await.result(futures, Duration.apply(TIMEOUT_SECONDS, TimeUnit.SECONDS))
    queue.close()
    assert(server.requests.size() == 3)
  }

  it should "keep the delay" in {
    val delay = 30L
    val server = new ServerMock()
    val queue = new crawler.PerHostExtractor.Queue(delay)
    val futures = Future.sequence(Seq.fill(3)(queue.extract(ExampleCom, server.call)))
    Await.result(futures, Duration.apply(TIMEOUT_SECONDS, TimeUnit.SECONDS))
    assert(delay <= getMinDelay(server.requests.asScala))
    queue.close()
  }

  it should "throw exception if a queue is full" in {
    val delay = 10L
    val server = new ServerMock(50L)
    val queue = new crawler.PerHostExtractor.Queue(delay, 1)
    val futures = Future.sequence(Seq.fill(3)(queue.extract(ExampleCom, server.call)))
    // 1st call will be handled, 2nd and 3rd will be queued, the last one will raise an Exception
    assertThrows[HostQueueIsFullException](Await.result(futures, Duration.apply(TIMEOUT_SECONDS, TimeUnit.SECONDS)))
    queue.close()
  }

  behavior of "an extraction manager"

  it should "extract all targets in parallel grouped by host" in {
    val breakMillis = 100L
    val server = new ServerMock(breakMillis)
    val manager = new PerHostExtractor[Unit](2, { case _ if true => server.call})

    val startMillis = System.currentTimeMillis()
    val future = Future.sequence(Seq(
      manager.extract(ExampleCom),
      manager.extract(ExampleNet),
      manager.extract(ExampleCom),
      manager.extract(ExampleNet),
      manager.extract(ExampleCom)
    ))
    Await.result(future, Duration.apply(TIMEOUT_SECONDS, TimeUnit.SECONDS))
    val elapsedMillis = System.currentTimeMillis() - startMillis
    assert(server.requests.size() == 5)
    assert(elapsedMillis < breakMillis * 5)
    manager.close()
  }

  it should "keep a delay for a host" in {
    val delay = 20L
    val server = new ServerMock(5L)
    val manager = new PerHostExtractor[Unit](1, { case _ if true => server.call})
    val future = Future.sequence(Seq(
      manager.extract(ExampleCom),
      manager.extract(ExampleNet),
      manager.extract(ExampleCom),
      manager.extract(ExampleNet),
      manager.extract(ExampleCom),
      manager.extract(ExampleNet)
    ))
    Await.result(future, Duration.apply(5, TimeUnit.SECONDS))
    manager.close()
    server.requests.asScala.groupBy(r => r.url.getHost).view.mapValues(getMinDelay).foreach(entry => delay < entry._2)
  }

  it should "throw exception when the shared queue is full" in {
    // 1st call will be handled, 2nd and 3rd will be queued, the last one will raise an Exception
    val server = new ServerMock(30L)
    val manager = new PerHostExtractor[Unit](1, { case _ if true => server.call}, processingQueueCapacity = 2)
    val futures = Future.sequence(Seq(
      manager.extract(ExampleCom),
      manager.extract(ExampleNet),
      manager.extract(ExampleCom),
      manager.extract(ExampleNet)
    ))
    assertThrows[SharedQueueIsFullException](Await.result(futures, Duration.apply(TIMEOUT_SECONDS, TimeUnit.SECONDS)))
    manager.close()
  }

  it should "throw exception when the host queue is full" in {
    // 1st call will be handled, 2nd and 3rd will be queued, the last one will raise an Exception
    val server = new ServerMock(30L)
    val manager = new PerHostExtractor[Unit](3, { case _ if true => server.call}, hostQueueCapacity = 1)
    // 1st call will be handled, 2nd will be queued, the last one will raise an Exception
    val futures = Future.sequence(Seq.fill(3)(manager.extract(ExampleCom)))
    assertThrows[HostQueueIsFullException](Await.result(futures, Duration.apply(TIMEOUT_SECONDS, TimeUnit.SECONDS)))
    manager.close()
  }

  it should "not throw exception when the parallelism level set prevents a queue to be overfilled" in {
    // 1st call will be handled, 2nd and 3rd will be queued, the last one will raise an Exception
    val server = new ServerMock(30L)
    val manager = new PerHostExtractor[Unit](1, { case _ if true => server.call}, hostQueueCapacity = 1)
    // 1st call will be handled, 2nd will be queued, the last one will not be picked while one of the previous don't complete
    val futures = Future.sequence(Seq.fill(3)(manager.extract(ExampleCom)))
    Await.result(futures, Duration.apply(TIMEOUT_SECONDS, TimeUnit.SECONDS))
    manager.close()
  }

}
