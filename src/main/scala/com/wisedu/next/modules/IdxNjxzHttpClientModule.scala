package com.wisedu.next.modules

import javax.inject.Singleton

import com.google.inject.Provides
import com.twitter.finagle.Service
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finatra.conversions.time._
import com.twitter.finatra.httpclient.{HttpClient, RichHttpClient}
import com.twitter.finatra.json.FinatraObjectMapper
import com.twitter.finatra.utils.RetryPolicyUtils._
import com.twitter.inject.TwitterModule
import com.wisedu.next.annotations.{IdxNjxzHttpClient, IdxNjxzHttpService}

object IdxNjxzHttpClientModule extends TwitterModule {

  private val hostFlag = flag("idx.njxz.host", "console.njxzc.edu.cn:6060", "idx njxz hostname")

  def hostname = hostFlag()
  val dest = "flag!idx.njxz"

  def retryPolicy = Some(exponentialRetry(
    start = 10.millis,
    multiplier = 2,
    numRetries = 3,
    shouldRetry = Http4xxOr5xxResponses))

  def defaultHeaders: Map[String, String] = Map()

  def sslHostname: Option[String] = None

  @Singleton
  @Provides
  @IdxNjxzHttpClient
  def provideIdxNjxzHttpClient(
                              mapper: FinatraObjectMapper,
                              @IdxNjxzHttpService httpService: Service[Request, Response]): HttpClient = {

    new HttpClient(
      hostname = hostname,
      httpService = httpService,
      retryPolicy = retryPolicy,
      defaultHeaders = defaultHeaders,
      mapper = mapper)
  }

  @Singleton
  @Provides
  @IdxNjxzHttpService
  def provideIdxNjxzHttpService: Service[Request, Response] = {
    sslHostname match {
      case Some(ssl) =>
        RichHttpClient.newSslClientService(
          sslHostname = ssl,
          dest = dest)
      case _ =>
        RichHttpClient.newClientService(
          dest = dest)
    }
  }
}