package izolotov.crawler

import java.net.URL

import HasDelay._

object HasDelay {
  val Default = 0L
}

trait HasDelay {

  private var _delay: PartialFunction[URL, Long] = _ => Default

  def delay: PartialFunction[URL, Long] = _delay

  def delay_=(delay: PartialFunction[URL, Long]): Unit = {
    this._delay = delay.orElse(_delay)
  }

}
