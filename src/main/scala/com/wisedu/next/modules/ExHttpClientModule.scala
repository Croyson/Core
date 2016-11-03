package com.wisedu.next.modules

import com.twitter.finatra.conversions.time._
import com.twitter.finatra.httpclient.modules.HttpClientModule
import com.twitter.finatra.utils.RetryPolicyUtils._

object ExHttpClientModule extends HttpClientModule {

  private val hostFlag = flag("ex.host", "localhost", "ex hostname")

  override def hostname = hostFlag()
  override val dest = "flag!ex"

  override def retryPolicy = Some(exponentialRetry(
    start = 10.millis,
    multiplier = 2,
    numRetries = 3,
    shouldRetry = Http4xxOr5xxResponses))
}