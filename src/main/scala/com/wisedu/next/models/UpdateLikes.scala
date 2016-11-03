package com.wisedu.next.models

import java.util.{TimeZone, UUID}

import com.twitter.finagle.exp.mysql._
import com.twitter.util.Future
import com.wisedu.next.types.NextTypes.MysqlClient
import org.joda.time.DateTime

case class UpdateLike(updateId: UUID, userId: UUID, opType: Int, opTime: DateTime)

abstract class ConcreteUpdateLikes(val client: MysqlClient) {
  val timeValue = new TimestampValue(TimeZone.getDefault, TimeZone.getDefault)

  def getById(userId: UUID, updateId: UUID): Future[Option[UpdateLike]] = {
    val query = "SELECT * FROM updateLikes WHERE userId = '" + userId.toString +
      "' and updateId = '" + updateId.toString + "' limit 1 "
    client.select[UpdateLike](query) { row =>
      val updateId = row("updateId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val userId = row("userId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(opType) = row("opType").getOrElse("")
      val timeValue(opTime) = row("opTime").getOrElse("")
      UpdateLike(UUID.fromString(updateId), UUID.fromString(userId), opType, new DateTime(opTime))
    }.map(_.headOption)
  }

  def delById(userId: UUID, updateId: UUID): Future[Result] = {
    val delSql = "delete from updateLikes where userId = '" + userId.toString + "' and feedId = '" + updateId.toString + "'"
    client.query(delSql)
  }

  def insUpdateLike(updateLike: UpdateLike): Future[Result] = {
    val insertSql = """INSERT INTO updateLikes (updateId, userId, opType, opTime) VALUES (?, ?, ?, ?)"""
    val ps = client.prepare(insertSql)
    ps(updateLike.updateId.toString, updateLike.userId.toString, updateLike.opType, updateLike.opTime.toDate)
  }

  def updUpdateLike(updateLike: UpdateLike): Future[Result] = {
    val updSql = """update updateLikes set opType = ?, opTime = ? where updateId = ? and userId = ?"""
    val ps = client.prepare(updSql)
    ps(updateLike.opType, updateLike.opTime.toDate, updateLike.updateId.toString, updateLike.userId.toString)
  }

  def getUpdateLikeSize(updateId: String, op_type:Int): Future[Int] = {
    val query = "select count(*) as value from updateLikes where updateId='"+ updateId +"' and optype= "+ op_type
    client.select(query) { row =>
      val LongValue(value) = row("value").getOrElse("")
      value.toInt
    }.map(_.head)
  }

  def getLikeById(userId: UUID, updateId: UUID, opType: Int): Future[Option[UpdateLike]] = {
    val query = "SELECT * FROM updateLikes WHERE userId = '" + userId.toString +
      "' and updateId = '" + updateId.toString + "' and opType = " + opType + " limit 1 "
    client.select[UpdateLike](query) { row =>
      val updateId = row("updateId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val userId = row("userId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(opType) = row("opType").getOrElse("")
      val timeValue(opTime) = row("opTime").getOrElse("")
      UpdateLike(UUID.fromString(updateId), UUID.fromString(userId), opType, new DateTime(opTime))
    }.map(_.headOption)
  }


}