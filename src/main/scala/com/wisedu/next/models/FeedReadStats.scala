package com.wisedu.next.models

import java.util.{TimeZone, UUID}

import com.twitter.finagle.exp.mysql._
import com.twitter.util.Future
import com.wisedu.next.types.NextTypes.MysqlClient
import org.joda.time.DateTime

case class FeedReadStat(feedId: UUID, userId: String, frTime: DateTime, lrTime: DateTime, readCount: Int, ipAddr: String)

abstract class ConcreteFeedReadStats(val client: MysqlClient) {
  val timeValue = new TimestampValue(TimeZone.getDefault, TimeZone.getDefault)

  def getById(userId: String, feedId: UUID): Future[Option[FeedReadStat]] = {
    val query = "SELECT * FROM feedReadStats WHERE userId = '" + userId.toString +
      "' and feedId = '" + feedId.toString + "' limit 1 "
    client.select[FeedReadStat](query) { row =>
      val feedId = row("feedId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val userId = row("userId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val ipAddr = row("ipAddr").map {
        case StringValue(str) => str
        case _ => ""
      }.get

      val readCount = row("readCount").map {
        case IntValue(item) => item
        case _ => 0
      }.get
      val timeValue(frTime) = row("frTime").getOrElse("")
      val timeValue(lrTime) = row("lrTime").getOrElse("")
      FeedReadStat(UUID.fromString(feedId), userId, new DateTime(frTime), new DateTime(lrTime), readCount, ipAddr)
    }.map(_.headOption)
  }

  def delById(userId: String, feedId: UUID): Future[Result] = {
    val delSql = "delete from feedReadStats where userId = '" + userId +
      "' and feedId = '" + feedId.toString + "'"
    client.query(delSql)
  }

  def insFeedReadStat(feedReadStat: FeedReadStat): Future[Result] = {
    val insertSql = """INSERT INTO feedReadStats (feedId, userId, frTime, lrTime, readCount,ipAddr) VALUES (?, ?, ?, ?, ?,?)"""
    val ps = client.prepare(insertSql)
    ps(feedReadStat.feedId.toString, feedReadStat.userId.toString, feedReadStat.frTime.toDate,
      feedReadStat.lrTime.toDate, feedReadStat.readCount, feedReadStat.ipAddr)
  }

  def updFeedReadStat(feedReadStat: FeedReadStat): Future[Result] = {
    val updSql = """update feedReadStats set frTime = ?, lrTime = ?, readCount = ?,ipAddr = ? where userId = ? and feedId = ?"""
    val ps = client.prepare(updSql)
    ps(feedReadStat.frTime.toDate, feedReadStat.lrTime.toDate, feedReadStat.readCount, feedReadStat.userId.toString,
      feedReadStat.ipAddr, feedReadStat.feedId.toString)
  }

  def getByIpaddr(ipAddr: String, feedId: UUID): Future[Option[FeedReadStat]] = {
    val query = "SELECT * FROM feedReadStats WHERE ipAddr = '" + ipAddr +
      "' and feedId = '" + feedId.toString + "' limit 1 "
    client.select[FeedReadStat](query) { row =>
      val feedId = row("feedId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val userId = row("userId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val ipAddr = row("ipAddr").map {
        case StringValue(str) => str
        case _ => ""
      }.get

      val readCount = row("readCount").map {
        case IntValue(item) => item
        case _ => 0
      }.get
      val timeValue(frTime) = row("frTime").getOrElse("")
      val timeValue(lrTime) = row("lrTime").getOrElse("")
      FeedReadStat(UUID.fromString(feedId), userId, new DateTime(frTime), new DateTime(lrTime), readCount, ipAddr)
    }.map(_.headOption)
  }

  def updFeedReadStatByIpAddr(feedReadStat: FeedReadStat): Future[Result] = {
    val updSql = """update feedReadStats set frTime = ?, lrTime = ?, readCount = ? where ipAddr = ? and userId = ? and feedId = ?"""
    val ps = client.prepare(updSql)
    ps(feedReadStat.frTime.toDate, feedReadStat.lrTime.toDate, feedReadStat.readCount, feedReadStat.ipAddr,
      feedReadStat.userId.toString, feedReadStat.feedId.toString)
  }

}