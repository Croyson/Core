package com.wisedu.next.models

import java.util.{TimeZone, UUID}

import com.twitter.finagle.exp.mysql._
import com.twitter.util.Future
import com.wisedu.next.types.NextTypes.MysqlClient
import org.joda.time.DateTime

case class FeedLike(feedId: UUID, userId: UUID, opType: Int, opTime: DateTime, opMsg: String)

abstract class ConcreteFeedLikes(val client: MysqlClient) {
  val timeValue = new TimestampValue(TimeZone.getDefault, TimeZone.getDefault)

  def getById(userId: UUID, feedId: UUID): Future[Option[FeedLike]] = {
    val query = "SELECT * FROM feedLikes WHERE userId = '" + userId.toString +
      "' and feedId = '" + feedId.toString + "' limit 1 "
    client.select[FeedLike](query) { row =>
      val feedId = row("feedId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val userId = row("userId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(opType) = row("opType").getOrElse("")
      val timeValue(opTime) = row("opTime").getOrElse("")
      val opMsg = row("opMsg").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      FeedLike(UUID.fromString(feedId), UUID.fromString(userId), opType, new DateTime(opTime), opMsg)
    }.map(_.headOption)
  }

  def delById(userId: UUID, feedId: UUID): Future[Result] = {
    val delSql = "delete from feedLikes where userId = '" + userId.toString + "' and feedId = '" + feedId.toString + "'"
    client.query(delSql)
  }

  def insFeedLike(feedLike: FeedLike): Future[Result] = {
    val insertSql = """INSERT INTO feedLikes (feedId, userId, opType, opTime, opMsg) VALUES (?, ?, ?, ?, ?)"""
    val ps = client.prepare(insertSql)
    ps(feedLike.feedId.toString, feedLike.userId.toString, feedLike.opType, feedLike.opTime.toDate, feedLike.opMsg)
  }

  def updFeedLike(feedLike: FeedLike): Future[Result] = {
    val updSql = """update feedLikes set opType = ?, opTime = ?, opMsg = ? where userId = ? and feedId = ?"""
    val ps = client.prepare(updSql)
    ps(feedLike.opType, feedLike.opTime.toDate, feedLike.opMsg, feedLike.userId.toString, feedLike.feedId.toString)
  }

}