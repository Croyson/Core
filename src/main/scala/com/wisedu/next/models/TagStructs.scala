package com.wisedu.next.models

import java.util.TimeZone

import com.twitter.finagle.exp.mysql._
import com.twitter.util.Future
import com.wisedu.next.types.NextTypes.MysqlClient
import org.joda.time.DateTime

case class TagStruct(tagId: String, userId: String, cTime: DateTime)

abstract class ConcreteTagStructs(val client: MysqlClient) {
  val timeValue = new TimestampValue(TimeZone.getDefault, TimeZone.getDefault)

  def getById(tagId: String): Future[Option[TagStruct]] = {
    val query = "select * from tagStructs where tagId = '" + tagId + "' limit 1"
    client.select[TagStruct](query) { row =>
      val tagId = row("tagId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val userId = row("userId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
     val timeValue(cTime) = row("cTime").getOrElse("")
      TagStruct(tagId,userId,new DateTime(cTime))
    }.map(_.headOption)
  }

  def delById(tagId: String): Future[Result] = {
    val delSql = "delete from tagStructs where tagId = '" + tagId + "'"
    client.query(delSql)
  }

  def insTagStruct(tagStruct: TagStruct): Future[Result] = {
    val insertSql =
      """INSERT INTO tagStructs (tagId, userId, cTime) VALUES (?, ?, ?)"""
    val ps = client.prepare(insertSql)
    ps(tagStruct.tagId,tagStruct.userId,tagStruct.cTime.toDate)
  }
}
