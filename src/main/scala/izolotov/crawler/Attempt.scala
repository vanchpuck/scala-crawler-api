package izolotov.crawler

trait Attempt[Doc] {
  def apply[Out](f: Doc => Out): Unit
}
