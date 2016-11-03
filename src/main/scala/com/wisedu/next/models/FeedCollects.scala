package com.wisedu.next.models

import java.util.{TimeZone, UUID}

import com.twitter.finagle.exp.mysql._
import com.twitter.util.Future
import com.wisedu.next.types.NextTypes.MysqlClient
import org.joda.time.DateTime

case class FeedCollect(feedId: UUID, userId: UUID, cTime: DateTime)

abstract class ConcreteFeedCollects(val client: MysqlClient) {
  val timeValue = new TimestampValue(TimeZone.getDefault, TimeZone.getDefault)

  def getById(userId: UUID, feedId: UUID): Future[Option[FeedCollect]] = {
    val query = "SELECT * FROM feedCollects WHERE userId = '" + userId.toString +
      "' and feedId = '" + feedId.toString + "' limit 1 "
    client.select[FeedCollect](query) { row =>
      val feedId = row("feedId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val userId = row("userId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(cTime) = row("cTime").getOrElse("")
      FeedCollect(UUID.fromString(feedId), UUID.fromString(userId), new DateTime(cTime))
    }.map(_.headOption)
  }

  def delById(userId: UUID, feedId: UUID): Future[Result] = {
    val delSql = "delete from feedCollects where userId = '" + userId.toString + "' and feedId = '" + feedId.toString + "'"
    client.query(delSql)
  }

  def insFeedCollect(feedCollect: FeedCollect): Future[Result] = {
    val insertSql = """INSERT INTO feedCollects (feedId, userId, cTime) VALUES (?, ?, ?)"""
    val ps = client.prepare(insertSql)
    ps(feedCollect.feedId.toString, feedCollect.userId.toString, feedCollect.cTime.toDate)
  }

  def updFeedCollect(feedCollect: FeedCollect): Future[Result] = {
    val updSql = """update feedCollects set cTime = ? where userId = ? and feedId = ?"""
    val ps = client.prepare(updSql)
    ps(feedCollect.cTime.toDate, feedCollect.feedId.toString, feedCollect.userId.toString)
  }

}