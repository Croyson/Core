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
import com.wisedu.next.annotations.{JPushHttpClient, JPushHttpService}

object JPushHttpClientModule extends TwitterModule {

  private val jPushHostFlag = flag("jpush.host", "api.jpush.cn", "jpush hostname")

  val dest = "flag!jpush"

  def hostname: String = ""

  def retryPolicy = Some(exponentialRetry(
    start = 10.millis,
    multiplier = 2,
    numRetries = 3,
    shouldRetry = Http4xxOr5xxResponses))

  def defaultHeaders: Map[String, String] = Map()

  def sslHostname: Option[String] = Some(jPushHostFlag())

  @Singleton
  @Provides
  @JPushHttpClient
  def provideJPushHttpClient(
                         mapper: FinatraObjectMapper,
                         @JPushHttpService httpService: Service[Request, Response]): HttpClient = {

    new HttpClient(
      hostname = hostname,
      httpService = httpService,
      retryPolicy = retryPolicy,
      defaultHeaders = defaultHeaders,
      mapper = mapper)
  }

  @Singleton
  @Provides
  @JPushHttpService
  def provideJPushHttpService: Service[Request, Response] = {
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