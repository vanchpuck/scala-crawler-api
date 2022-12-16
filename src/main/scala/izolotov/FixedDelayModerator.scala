package izolotov

import java.net.URL
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{Executors, TimeUnit}

import scala.concurrent.ExecutionContext

class FixedDelayModerator(delay: Long) {

  private val delayLock = new ReentrantLock
  private val fetcherLock = new ReentrantLock
  private val lockedForTimeout = delayLock.newCondition

  private var prevFetchTime: Long = 0

  private class Delayer(val delay: Long) extends Runnable {
    override def run(): Unit = {
      delayLock.lock()
      try {
        Thread.sleep(delay)
      }
      finally {
        lockedForTimeout.signal()
        delayLock.unlock()
      }
    }
  }

  private implicit val processingContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))

  def apply[A, R](url: A, fn: A => R): R = {
    fetcherLock.lock()
    try {
      delayLock.lock()
      val nextFetchTime = prevFetchTime + delay
      val remainingDelay = nextFetchTime - System.currentTimeMillis
      if (System.currentTimeMillis < nextFetchTime) new Thread(new Delayer(remainingDelay)).start()
      while (System.currentTimeMillis < nextFetchTime) {
        lockedForTimeout.await(remainingDelay, TimeUnit.MILLISECONDS)
      }
      try {
        fn.apply(url)
//        val s: R = fn.apply(url)
//        println(s)
//        s
      } finally {
        prevFetchTime = System.currentTimeMillis
        delayLock.unlock()
      }
    } finally fetcherLock.unlock()
  }

}

