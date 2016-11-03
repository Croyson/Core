package com.wisedu.next.models

import java.util.UUID

import com.twitter.finagle.exp.mysql._
import com.twitter.util.Future
import com.wisedu.next.types.NextTypes.MysqlClient

case class UserTag(userId: UUID, tagId: String, tagRank: Int, errorValue: Int)

abstract class ConcreteUserTags(val client: MysqlClient) {

  def getById(userId: UUID, tagId: String): Future[Option[UserTag]] = {
    val query = "SELECT * FROM userTags WHERE userId = '" + userId.toString +
      "' and tagId = '" + tagId + "' limit 1 "
    client.select[UserTag](query) { row =>
      val userId = row("userId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val tagId = row("tagId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(tagRank) = row("tagRank").getOrElse("")
      val IntValue(errorValue) = row("errorValue").getOrElse("")
      UserTag(UUID.fromString(userId), tagId, tagRank, errorValue)
    }.map(_.headOption)
  }

  def delById(userId: UUID, tagId: String): Future[Result] = {
    val delSql = "delete from userTags where userId = '" + userId.toString + "' and tagId = '" + tagId.toString + "'"
    client.query(delSql)
  }

  def insUserTag(userTag: UserTag): Future[Result] = {
    val insertSql = """INSERT INTO userTags (userId, tagId, tagRank, errorValue) VALUES (?, ?, ?, ?)"""
    val ps = client.prepare(insertSql)
    ps(userTag.userId.toString, userTag.tagId, userTag.tagRank, userTag.errorValue)
  }

  def updUserTag(userTag: UserTag): Future[Result] = {
    val updSql = """update userTags set tagRank = ?, errorValue = ? where tagId = ? and userId = ?"""
    val ps = client.prepare(updSql)
    ps(userTag.tagRank, userTag.errorValue, userTag.tagId, userTag.userId.toString)
  }

  def getUserIdByTagId(tagId: String): Future[Seq[String]] = {
    val query = "SELECT userId FROM userTags WHERE tagId = '" + tagId + "'"
    client.select[String](query) { row =>
      val StringValue(userId) = row("userId").getOrElse("")
      userId
    }
  }

  def getUserTagsByUserId(userId: UUID): Future[Seq[UserTag]] = {
    val query = "SELECT * FROM userTags  WHERE userId = '" + userId.toString +
      "' and userId = '" + userId.toString + "'"
    client.select[UserTag](query) { row =>
      val userId = row("userId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val tagId = row("tagId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(tagRank) = row("tagRank").getOrElse("")
      val IntValue(errorValue) = row("errorValue").getOrElse("")
      UserTag(UUID.fromString(userId), tagId, tagRank, errorValue)
    }
  }

  def delUserTagsByUserId(userId: UUID): Future[Result] = {
    val delSql = "delete FROM userTags WHERE userId = '" + userId.toString + "'"
    client.query(delSql)
  }

  def getUserIdByTagIds(tagIds: String, limits: Int, offset: Int): Future[Seq[String]] = {
    val query = "SELECT userId FROM userTags WHERE tagId in ('" + tagIds.replaceAll(",", "','") + "') limit " + offset + ", " + limits
    client.select[String](query) { row =>
      val StringValue(userId) = row("userId").getOrElse("")
      userId
    }
  }


  def sysUserTags(userId: UUID): Future[Result] = {
    val sql =
      """
            INSERT INTO userTags(
            	userId,
            	tagId,
            	tagRank,
            	errorValue
            )SELECT
            	?,
            	tagId,
            	0,
            	0
            FROM
            	tags
            WHERE
            	EXISTS(
            		SELECT
            			*
            		FROM
            			userTagKeys
            		WHERE
            			tags.tagId = userTagKeys.tagKey
            		AND(
            			userTagKeys.userKey =(
            				SELECT
            					sex
            				FROM
            					users
            				WHERE
            					userId = ?
            			)
            			OR userTagKeys.userKey =(
            				SELECT
            					collegeId
            				FROM
            					users
            				WHERE
            					userId = ?
            			)
            		)
            	)
      """
    client.prepare(sql)(userId.toString,userId.toString,userId.toString)
  }
}