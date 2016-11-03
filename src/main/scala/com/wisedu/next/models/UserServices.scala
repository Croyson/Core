package com.wisedu.next.models

import java.util.{TimeZone, UUID}

import com.twitter.finagle.exp.mysql._
import com.twitter.util.Future
import com.wisedu.next.types.NextTypes.MysqlClient
import org.joda.time.DateTime

case class UserService(userId: UUID, serId: String, cTime: DateTime)

abstract class ConcreteUserServices(val client: MysqlClient) {
  val timeValue = new TimestampValue(TimeZone.getDefault, TimeZone.getDefault)

  def getById(userId: UUID, serId: String): Future[Option[UserService]] = {
    val query = "SELECT * FROM userServices WHERE userId = '" + userId.toString +
      "' and serId = '" + serId + "' limit 1 "
    client.select[UserService](query) { row =>
      val userId = row("userId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val serId = row("serId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(cTime) = row("cTime").getOrElse("")
      UserService(UUID.fromString(userId), serId, new DateTime(cTime))
    }.map(_.headOption)
  }

  def delById(userId: UUID, serId: String): Future[Result] = {
    val delSql = "delete from userServices where userId = '" + userId.toString + "' and serId = '" + serId + "'"
    client.query(delSql)
  }

  def insUserService(userService: UserService): Future[Result] = {
    val insertSql = """INSERT INTO userServices (userId, serId, cTime) VALUES (?, ?, ?)"""
    val ps = client.prepare(insertSql)
    ps(userService.userId.toString, userService.serId.toString, userService.cTime.toDate)
  }

  def getSerByUser(userId: UUID, limits: Int, offset: Int): Future[Seq[Service]]={
    val query = "select * from services where serId in (SELECT serId FROM userServices WHERE userId = '" + userId.toString +"') order by sortNo desc,mtime desc limit "+ offset +", "+ limits

    client.select[Service](query) { row =>
      val serId = row("serId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val name = row("name").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(orgType) = row("orgType").getOrElse("")
      val srcId = row("srcId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(srcType) = row("srcType").getOrElse("")
      val imgUrl = row("imgUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val backImgUrl = row("backImgUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val depict = row("depict").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
      val timeValue(mTime) = row("mTime").getOrElse("0000-00-00 00:00:00")
      val collegeId = row("collegeId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(sortNo) = row("sortNo").getOrElse("")
      val IntValue(isDisplay ) = row("isDisplay").getOrElse("")
      Service(serId, name, orgType, srcId, srcType, imgUrl, backImgUrl, depict,
        new DateTime(cTime), new DateTime(mTime),collegeId,sortNo,isDisplay)
    }
  }

  def collUserServiceSize(userId: String, serId:String): Future[Int]={
    var query = "select count(1) as value from userServices f where 1 = 1 "
    if(!userId.isEmpty)
      query += " and userId = '" + userId + "'"
    if(!serId.isEmpty)
      query += " and serId = '" + serId + "'"

    client.select(query) { row =>
      val LongValue(value) = row("value").getOrElse("0")
      value.toInt
    }.map(_.head)
  }

}