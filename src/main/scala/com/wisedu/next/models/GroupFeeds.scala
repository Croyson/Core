package com.wisedu.next.models

import java.util.TimeZone

import com.twitter.finagle.exp.mysql._
import com.twitter.util.Future
import com.wisedu.next.types.NextTypes.MysqlClient
import org.joda.time.DateTime

case class GroupFeed(groupId: String, feedId: String, cTime: DateTime, cUser: String, auditStatus: Int)

abstract class ConcreteGroupFeeds(val client: MysqlClient) {
  val timeValue = new TimestampValue(TimeZone.getDefault, TimeZone.getDefault)

  def insGroupFeed(groupFeed: GroupFeed): Future[Result] = {
    val sql = "insert into  groupFeeds(groupId,feedId,cTime,cUser,auditStatus) values(?,?,?,?,?)"
    client.prepare(sql)(groupFeed.groupId,groupFeed.feedId,groupFeed.cTime.toDate,groupFeed.cUser,groupFeed.auditStatus)
  }


  def getById(groupId: String, feedId: String): Future[Option[GroupFeed]] = {
    val query = "SELECT * FROM groupFeeds WHERE groupId = ?  and feedId = ? limit 1 "
    client.prepare(query).select[GroupFeed](groupId,feedId){row =>
         val groupId = row("groupId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val feedId = row("feedId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val cUser = row("cUser").map {
          case StringValue(str) => str
          case _ => ""
        }.get

        val timeValue(cTime) = row("cTime").getOrElse("")
        val IntValue(auditStatus) = row("auditStatus").getOrElse("")
       GroupFeed(groupId,feedId,new DateTime(cTime),cUser,auditStatus)
    }.map(_.headOption)
  }

  def delById(groupId: String, feedId: String): Future[Result] = {
    val delSql = "delete from groupFeeds WHERE groupId = ? and feedId = ? "
    client.prepare(delSql)(groupId, feedId)
  }

  def delByFeedId(feedId: String): Future[Result] = {
    val delSql = "delete from groupFeeds WHERE feedId = ? "
    client.prepare(delSql)(feedId)
  }

  def delByGroupId(groupId: String): Future[Result] = {
    val delSql = "delete from groupFeeds WHERE groupId = ?"
     client.prepare(delSql)(groupId)
  }

  def updGroupFeed(groupId: String, feedId: String, status: Int): Future[Result] = {
    val sql = """ update groupFeeds  set  status = ? WHERE groupId = ? and feedId = ? """
    client.prepare(sql)(status, groupId, feedId)
  }

  def collGroupFeeds(groupId: String, limits: Int, offset: Int): Future[Seq[GroupFeed]] = {
    var query = "SELECT * FROM groupFeeds WHERE 1 = 1 "
    if (!groupId.isEmpty) {
      query += " and groupId = '" + groupId + "' "
    }
    query += "order by cTime desc limit " + offset + ", " + limits
    client.select[GroupFeed](query){row =>
         val groupId = row("groupId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val feedId = row("feedId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val cUser = row("cUser").map {
          case StringValue(str) => str
          case _ => ""
        }.get

        val timeValue(cTime) = row("cTime").getOrElse("")
        val IntValue(auditStatus) = row("auditStatus").getOrElse("")
       GroupFeed(groupId,feedId,new DateTime(cTime),cUser,auditStatus)
    }
  }

  def collGroupFeedSize(groupId: String): Future[Int] = {
    var query = "SELECT count(1) as value FROM groupFeeds WHERE 1 = 1 "
    if (!groupId.isEmpty) {
      query += " and groupId = '" + groupId + "' "
    }
    client.select(query) { row =>
      val LongValue(value) = row("value").getOrElse("0")
      value.toInt
    }.map(_.head)
  }

  def getGroupFeedsByFeedId(feedId: String): Future[Seq[GroupFeed]] = {
    val query = "SELECT * FROM groupFeeds WHERE feedId = ? order by cTime desc "
    client.prepare(query).select[GroupFeed](feedId){row =>
         val groupId = row("groupId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val feedId = row("feedId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val cUser = row("cUser").map {
          case StringValue(str) => str
          case _ => ""
        }.get

        val timeValue(cTime) = row("cTime").getOrElse("")
        val IntValue(auditStatus) = row("auditStatus").getOrElse("")
       GroupFeed(groupId,feedId,new DateTime(cTime),cUser,auditStatus)
    }
  }

}