package izolotov.crawler

import izolotov.crawler.CrawlerInput.QueueItem

trait CrawlingManager {
  def manage(item: QueueItem, extraction: QueueItem => Unit, onSuccess: () => Unit, onErr: Throwable => Unit): Unit
}
