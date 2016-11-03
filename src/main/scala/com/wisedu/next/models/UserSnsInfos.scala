package com.wisedu.next.models

import java.util.{TimeZone, UUID}

import com.twitter.finagle.exp.mysql.{IntValue, Result, StringValue, TimestampValue}
import com.twitter.util.Future
import com.wisedu.next.types.NextTypes._
import org.joda.time.DateTime

case class UserSnsInfo(userId: UUID, openId: String, snsCode: String, snsAlias: String, snsType: Int, cTime: DateTime)

abstract class ConcreteUserSnsInfos(val client: MysqlClient) {
  val timeValue = new TimestampValue(TimeZone.getDefault, TimeZone.getDefault)

  def insUserSnsInfo(userSnsInfo: UserSnsInfo): Future[Result] = {
    val insertSQL = """INSERT INTO userSnsInfos (userId, openId, snsCode, snsAlias, snsType, cTime)
        VALUES (?, ?, ?, ?, ?, now())"""
    client.prepare(insertSQL)(userSnsInfo.userId.toString, userSnsInfo.openId, userSnsInfo.snsCode,
      userSnsInfo.snsAlias, userSnsInfo.snsType)
  }

  def delUserSnsInfo(userId: UUID, openId: String, snsType: Int): Future[Result] = {
    val delSql = "delete from userSnsInfos where userId = '" + userId.toString + "'" +  " and openId = '" + openId +
      "' and snsType = " + snsType
    client.query(delSql)
  }

  def getSnsByOpenId(openId: String, snsType: Int): Future[Option[UserSnsInfo]] = {
    val query = "select * from userSnsInfos where openId = '" + openId + "' and snsType = " + snsType + " limit 1 "
    client.select[UserSnsInfo](query) { row =>
      val userId = row("userId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val openId = row("openId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val snsCode = row("snsCode").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val snsAlias = row("snsAlias").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(snsType) = row("snsType").getOrElse("")
      val timeValue(cTime) = row("cTime").getOrElse("")
      UserSnsInfo(UUID.fromString(userId), openId, snsCode, snsAlias, snsType, new DateTime(cTime))
    }.map(_.headOption)
  }

  def getSnsByUserId(userId:UUID): Future[Seq[UserSnsInfo]] = {
    val query = "select * from userSnsInfos where userId = '" + userId.toString + "' order by cTime desc "
    client.select[UserSnsInfo](query) { row =>
      val userId = row("userId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val openId = row("openId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val snsCode = row("snsCode").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val snsAlias = row("snsAlias").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(snsType) = row("snsType").getOrElse("")
      val timeValue(cTime) = row("cTime").getOrElse("")
      UserSnsInfo(UUID.fromString(userId), openId, snsCode, snsAlias, snsType, new DateTime(cTime))
    }
  }
}