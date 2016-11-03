package com.wisedu.next.models

import java.util.{TimeZone, UUID}

import com.twitter.finagle.exp.mysql.{LongValue, Result, StringValue, TimestampValue}
import com.twitter.util.Future
import com.wisedu.next.types.NextTypes.MysqlClient
import org.joda.time.DateTime

case class FeedLotteryDraw(feedId: String, lotteryDrawId: String, lotteryDrawTitle: String, lotteryDrawDescr: String,
                           lotteryDrawUrl: String, lotteryDrawRstUrl: String, cTime: DateTime, finishTime: DateTime)

abstract class ConcreteFeedLotteryDraws(val client: MysqlClient) {
  val timeValue = new TimestampValue(TimeZone.getDefault, TimeZone.getDefault)

  def insFeedLotteryDraw(feedLotteryDraw: FeedLotteryDraw): Future[Result] = {
    val insertSql =
      """INSERT INTO feedLotteryDraw ( lotteryDrawId, lotteryDrawTitle, lotteryDrawDescr,
                                    lotteryDrawUrl, lotteryDrawRstUrl,cTime, finishTime) VALUES ( ?, ?, ?, ?, ?,?,?)"""
    client.prepare(insertSql)(feedLotteryDraw.lotteryDrawId, feedLotteryDraw.lotteryDrawTitle,
      feedLotteryDraw.lotteryDrawDescr, feedLotteryDraw.lotteryDrawUrl, feedLotteryDraw.lotteryDrawRstUrl,
      feedLotteryDraw.cTime.toDate, feedLotteryDraw.finishTime.toDate)
  }

  def updFeedLotteryDraw(feedLotteryDraw: FeedLotteryDraw): Future[Result] = {
    val updSql =
      """update feedLotteryDraw set lotteryDrawTitle = ? , lotteryDrawDescr = ?,
                                    lotteryDrawUrl = ?, lotteryDrawRstUrl = ?,finishTime=? where lotteryDrawId = ? """
    client.prepare(updSql)(feedLotteryDraw.lotteryDrawTitle, feedLotteryDraw.lotteryDrawDescr,
      feedLotteryDraw.lotteryDrawUrl, feedLotteryDraw.lotteryDrawRstUrl, feedLotteryDraw.finishTime.toDate,
      feedLotteryDraw.lotteryDrawId)
  }

  def updFeedId(lotteryDrawId: String, feedId: String): Future[Result] = {
    val updSql =
      """update feedLotteryDraw set feedId = ?  where lotteryDrawId = ? """
    client.prepare(updSql)(feedId.toString, lotteryDrawId)
  }


  def getByFeedId(feedId: String): Future[Option[FeedLotteryDraw]] = {
    val query = "select * from feedLotteryDraw  where feedId = '" + feedId + "' limit 1"
    client.select[FeedLotteryDraw](query) { row =>
      val feedId = row("feedId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val lotteryDrawId = row("lotteryDrawId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val lotteryDrawTitle = row("lotteryDrawTitle").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val lotteryDrawDescr = row("lotteryDrawDescr").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val lotteryDrawUrl = row("lotteryDrawUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val lotteryDrawRstUrl = row("lotteryDrawRstUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
      val timeValue(finishTime) = row("finishTime").getOrElse("0000-00-00 00:00:00")
      FeedLotteryDraw(feedId, lotteryDrawId, lotteryDrawTitle, lotteryDrawDescr, lotteryDrawUrl, lotteryDrawRstUrl,
        new DateTime(cTime), new DateTime(finishTime))
    }.map(_.headOption)
  }

  def getById(lotteryDrawId: String): Future[Option[FeedLotteryDraw]] = {
    val query = "select * from feedLotteryDraw  where lotteryDrawId = '" + lotteryDrawId.toString + "' limit 1"
    client.select[FeedLotteryDraw](query) { row =>
      val feedId = row("feedId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val lotteryDrawId = row("lotteryDrawId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val lotteryDrawTitle = row("lotteryDrawTitle").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val lotteryDrawDescr = row("lotteryDrawDescr").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val lotteryDrawUrl = row("lotteryDrawUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val lotteryDrawRstUrl = row("lotteryDrawRstUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
      val timeValue(finishTime) = row("finishTime").getOrElse("0000-00-00 00:00:00")
      FeedLotteryDraw(feedId, lotteryDrawId, lotteryDrawTitle, lotteryDrawDescr, lotteryDrawUrl, lotteryDrawRstUrl,
        new DateTime(cTime), new DateTime(finishTime))
    }.map(_.headOption)
  }

  def delById(lotteryDrawId: String): Future[Result] = {
    val delSql = "delete from feedLotteryDraw  where lotteryDrawId = '" + lotteryDrawId + "'"
    client.query(delSql)
  }

  def delByFeedId(feedId: UUID): Future[Result] = {
    val delSql = "delete from feedLotteryDraw  where feedId = '" + feedId.toString + "'"
    client.query(delSql)
  }

  def collFeedLotteryDraws(limit: Int, offset: Int): Future[Seq[FeedLotteryDraw]] = {
    val query = "select * from feedLotteryDraw order by cTime desc limit  " + offset + "," + limit
    client.select[FeedLotteryDraw](query) { row =>
      val feedId = row("feedId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val lotteryDrawId = row("lotteryDrawId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val lotteryDrawTitle = row("lotteryDrawTitle").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val lotteryDrawDescr = row("lotteryDrawDescr").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val lotteryDrawUrl = row("lotteryDrawUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val lotteryDrawRstUrl = row("lotteryDrawRstUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
      val timeValue(finishTime) = row("finishTime").getOrElse("0000-00-00 00:00:00")
      FeedLotteryDraw(feedId, lotteryDrawId, lotteryDrawTitle, lotteryDrawDescr, lotteryDrawUrl, lotteryDrawRstUrl,
        new DateTime(cTime), new DateTime(finishTime))
    }
  }

  def collFeedLotteryDrawSize(): Future[Int] = {
    val query = "SELECT count(1) as value from  feedLotteryDraw  "

    client.select(query) { row =>
      val LongValue(value) = row("value").getOrElse("0")
      value.toInt
    }.map(_.head)
  }


}