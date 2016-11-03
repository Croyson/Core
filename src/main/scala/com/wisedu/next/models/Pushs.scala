package com.wisedu.next.models

import java.util.TimeZone

import com.twitter.finagle.exp.mysql._
import com.twitter.util.Future
import com.wisedu.next.types.NextTypes.MysqlClient
import org.joda.time.DateTime

case class Push(pushId: String, platform: String, audienceType: String, tags: String, alias: String, alterContent: String,
                title: String, feedId: String, feedSubParam: String, pushAdmin: String, cTime: DateTime, pushTime: DateTime, status: Int)

abstract class ConcretePushs(val client: MysqlClient) {
  val timeValue = new TimestampValue(TimeZone.getDefault, TimeZone.getDefault)

  def insPush(push: Push): Future[Result] = {
    val sql =
      """INSERT INTO pushs (pushId, platform, audienceType, tags,alias,alterContent,
         title, feedId, feedSubParam, pushAdmin, cTime, pushTime, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    val ps = client.prepare(sql)
    ps(push.pushId, push.platform, push.audienceType, push.tags, push.alias, push.alterContent,
      push.title, push.feedId, push.feedSubParam, push.pushAdmin, push.cTime.toDate, push.pushTime.toDate, push.status)
  }

  def upStatus(pushId: String, status: Int): Future[Result] = {
    val sql = "update pushs set status = ?,pushTime = now() where pushId = ?"
    client.prepare(sql)(status, pushId)
  }

  def updPush(push: Push): Future[Result] = {
    val sql =
      """update pushs set platform = ?, audienceType = ?, tags = ?,alias = ?,alterContent = ?,
         title = ? , feedId = ?, feedSubParam = ?, pushAdmin = ? where pushId = ?"""
    val ps = client.prepare(sql)
    ps(push.platform, push.audienceType, push.tags, push.alias, push.alterContent,
      push.title, push.feedId, push.feedSubParam, push.pushAdmin, push.pushId)
  }

  def delPush(pushId: String): Future[Result] = {
    val sql =
      """delete from  pushs  where pushId = ?"""
    client.prepare(sql)(pushId)
  }

  def getById(pushId: String): Future[Option[Push]] = {
    val sql = "select * from pushs where pushId = ? limit 1"
    client.prepare(sql).select[Push](pushId) { row =>
      val pushId = row("pushId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val platform = row("platform").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val audienceType = row("audienceType").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val tags = row("tags").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val alias = row("alias").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val alterContent = row("alterContent").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val title = row("title").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val feedId = row("feedId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val pushAdmin = row("pushAdmin").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val feedSubParam = row("feedSubParam").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
      val timeValue(pushTime) = row("pushTime").getOrElse("0000-00-00 00:00:00")
      val IntValue(status) = row("status").getOrElse("")
      Push(pushId, platform, audienceType, tags, alias, alterContent, title, feedId, feedSubParam, pushAdmin,
        new DateTime(cTime), new DateTime(pushTime), status)
    }.map(_.headOption)
  }

  def collTags(status: String, alterContent: String, limit: Int, offset: Int): Future[Seq[Push]] = {
    var sql = "select * from pushs where 1=1 "
    if (!alterContent.isEmpty) {
      sql += " and alterContent like '%" + alterContent + "%'"
    }
    if (!status.isEmpty) {
      sql += " and status = '" + status + "'"
    }
    sql += " order by cTime desc   limit " + offset + "," + limit

    client.select[Push](sql) { row =>
      val pushId = row("pushId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val platform = row("platform").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val audienceType = row("audienceType").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val tags = row("tags").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val alias = row("alias").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val alterContent = row("alterContent").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val title = row("title").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val feedId = row("feedId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val pushAdmin = row("pushAdmin").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val feedSubParam = row("feedSubParam").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
      val timeValue(pushTime) = row("pushTime").getOrElse("0000-00-00 00:00:00")
      val IntValue(status) = row("status").getOrElse("")
      Push(pushId, platform, audienceType, tags, alias, alterContent, title, feedId, feedSubParam, pushAdmin,
        new DateTime(cTime), new DateTime(pushTime), status)
    }
  }

  def collTagsSize(status: String, alterContent: String): Future[Int] = {
    var query = "SELECT count(1) as value from  pushs where 1=1 "
    if (!alterContent.isEmpty) {
      query += " and alterContent like '%" + alterContent + "%'"
    }
    if (!status.isEmpty) {
      query += " and status = '" + status + "'"
    }
    client.select(query) { row =>
      val LongValue(value) = row("value").getOrElse("0")
      value.toInt
    }.map(_.head)
  }
}
