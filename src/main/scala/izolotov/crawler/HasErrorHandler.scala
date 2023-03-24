package izolotov.crawler

import java.net.URL

trait HasErrorHandler {

  // TODO get rid of println
  private var _errHandler: PartialFunction[URL, Throwable => Unit] = {case e if true => e => println(s"Error: ${e}")}

  def errHandler: PartialFunction[URL, Throwable => Unit] = _errHandler

  def errHandler_=(delay: PartialFunction[URL, Throwable => Unit]): Unit = {
    this._errHandler = delay.orElse(_errHandler)
  }

}
