package izolotov.crawler

import java.net.URL

import scala.concurrent.Future

trait Extractor[Doc] {
  def extract(url: URL): Future[Doc]
//  def extract(url: URL): Attempt[Doc]
  def close(): Unit
}
