package izolotov.crawler

import java.net.URL

trait Extractor[Doc] {
  def extract(url: URL): Attempt[Doc]
  def close(): Unit
}
