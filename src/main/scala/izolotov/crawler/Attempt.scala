package izolotov.crawler

trait Attempt[Doc] {
  // TODO Use Try instead
  def apply[Out, Err](f: Doc => Out, err: Throwable => Err = (err: Throwable) => throw err): Unit
}
