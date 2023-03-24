package izolotov.crawler

import java.net.URL

trait HasSuccessHandler {

  // TODO get rid of println
  private var _successHandler: PartialFunction[URL, () => Unit] = null//{case _ if true => _ => println(1)}

  def successHandler: PartialFunction[URL, () => Unit] = _successHandler

  def successHandler_=(delay: PartialFunction[URL, () => Unit]): Unit = {
    this._successHandler = delay.orElse(_successHandler)
  }

}
