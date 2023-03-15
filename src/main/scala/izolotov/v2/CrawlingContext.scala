package izolotov.v2

import java.net.URL

import izolotov.v2.ErrHandling.CrawlingException

class CrawlingContext[Raw, Doc](
                            fetcher: URL => Raw,
                            parser: Raw => Doc,
                            writer: Doc => Unit,
                            errHandler: CrawlingException => Unit,
                            delay: Long,
                            timeout: Long,
                            followRedirect: Boolean
                     ) {

  import java.net.URL

}
