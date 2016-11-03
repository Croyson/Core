package com.wisedu.next.models

import java.util.UUID

import com.twitter.finagle.exp.mysql._
import com.twitter.util.Future
import com.wisedu.next.types.NextTypes.MysqlClient

case class FeedPermission(feedId: UUID, collegeId: String, permissionType: Int)

abstract class ConcreteFeedPermissions(val client: MysqlClient) {

  def getById(feedId: UUID, collegeId: String): Future[Option[FeedPermission]] = {
    val query = "SELECT * FROM feedPermissions WHERE feedId = '" + feedId.toString +
      "' and collegeId = '" + collegeId.toString + "' limit 1 "
    client.select[FeedPermission](query) { row =>
      val feedId = row("feedId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val collegeId = row("collegeId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(permissionType) = row("permissionType").getOrElse("")
      FeedPermission(UUID.fromString(feedId), collegeId, permissionType)
    }.map(_.headOption)
  }

  def delById(feedId: UUID, collegeId: String): Future[Result] = {
    val delSql = "delete from feedPermissions where feedId = '" + feedId.toString +
      "' and collegeId = '" + collegeId.toString + "'"
    client.query(delSql)
  }

  def delByFeedId(feedId: UUID): Future[Result] = {
    val delSql = "delete from feedPermissions where feedId = '" + feedId.toString +"'"
    client.query(delSql)
  }

  def insFeedPermission(feedPermission: FeedPermission): Future[Result] = {
    val insertSql = """INSERT INTO feedPermissions (feedId, collegeId, permissionType) VALUES (?, ?, ?)"""
    val ps = client.prepare(insertSql)
    ps(feedPermission.feedId.toString, feedPermission.collegeId.toString, feedPermission.permissionType)
  }

  def updFeedPermission(feedPermission: FeedPermission): Future[Result] = {
    val updSql = """update feedPermissions set permissionType = ? where feedId = ? and collegeId = ?"""
    val ps = client.prepare(updSql)
    ps(feedPermission.feedId.toString, feedPermission.collegeId.toString, feedPermission.permissionType)
  }

  def getByFeedId(feedId: UUID): Future[Seq[FeedPermission]] = {
    val query = "SELECT * FROM feedPermissions WHERE feedId = '" + feedId.toString +"'"
    client.select[FeedPermission](query) { row =>
      val feedId = row("feedId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val collegeId = row("collegeId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(permissionType) = row("permissionType").getOrElse("")
      FeedPermission(UUID.fromString(feedId), collegeId, permissionType)
    }
  }

}