package izolotov.crawler

import java.net.URL

import izolotov.crawler.CrawlerApi.RedirectAnalyzer
import izolotov.crawler.CrawlerInput.QueueItem

import scala.concurrent.Future

trait RichManager extends HasErrorHandler with HasSuccessHandler {

//  def setFetcher[Raw](fetcher: PartialFunction[URL, URL => Raw])(implicit c: CrawlerApi.RedirectAnalyzer[Raw]): Unit
//
//  def setParser[Raw, Doc](parser: PartialFunction[URL, Raw => Doc])
//
//  def setWriter[Doc](writer: PartialFunction[URL, Doc => Unit])
//
  def setExtractor(extractor: PartialFunction[URL, URL => Unit])

  def manage(url: QueueItem): Unit

}
