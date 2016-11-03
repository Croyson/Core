package com.wisedu.next.models

import java.util.{TimeZone, UUID}
import com.twitter.finagle.exp.mysql._
import com.twitter.finagle.redis.Client
import com.twitter.util.Future
import com.wisedu.next.types.NextTypes.MysqlClient
import org.joda.time.DateTime

case class UserRelation(userId: UUID,followId:UUID,rtype:Int,cTime:DateTime)

abstract class ConcreteUserRelations(val client: MysqlClient, val redisClient: Client) {
  val timeValue = new TimestampValue(TimeZone.getDefault, TimeZone.getDefault)

  def insUserRelation(userRelation: UserRelation): Future[Result] = {
    val insertSql =
      """INSERT INTO userRelations (userId, followId, rtype,cTime)
          VALUES (?,?,?,now())"""
    client.prepare(insertSql)(userRelation.userId.toString,userRelation.followId.toString,userRelation.rtype)
  }

  def delUserRelationByUserId(userId: UUID,followId:UUID,rtype:Int): Future[Result] = {
    val delSql = "delete from userRelations where userId = ? and followId = ? and rtype = ?"
    client.prepare(delSql)(userId.toString,followId.toString,rtype)
  }

  def getById(userId: UUID,followId:UUID,rtype:Int): Future[Option[UserRelation]] = {
    val sql = "SELECT * FROM userRelations where userId = ? and followId = ? and rtype = ? limit 1 "
    client.prepare(sql).select[UserRelation](userId.toString,followId.toString,rtype){ row =>
      val userId = row("userId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val followId = row("followId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val rtype = row("rtype").map {
        case IntValue(str) => str
        case _ => 0
      }.get
       val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
      UserRelation(UUID.fromString(userId),UUID.fromString(followId),rtype,new DateTime(cTime))
    }.map(_.headOption)
  }

  def getUserRelations(userId:UUID,rtype:Int,offSet: Int, limits: Int): Future[Seq[UserRelation]] = {
    val sql = "SELECT * FROM userRelations where userId = ? and rtype = ?   order by cTime desc limit " + offSet + "," + limits
    client.prepare(sql).select[UserRelation](userId.toString,rtype) { row =>
      val userId = row("userId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val followId = row("followId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val rtype = row("rtype").map {
        case IntValue(str) => str
        case _ => 0
      }.get
       val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
      UserRelation(UUID.fromString(userId),UUID.fromString(followId),rtype,new DateTime(cTime))
    }
  }

  // 获取关注用户或粉丝用户ID
  def getUserIds(userId: UUID, offSet: Int, limits: Int, method: Int): Future[Seq[String]] = {
    var sql = ""
    // 0--关注者 1--粉丝
    if(method==0){
      sql = "SELECT followId as id from userRelations where userId = ? order by cTime desc limit " + offSet + "," + limits
    }else{
      sql = "SELECT userId as id from userRelations where followId = ? order by cTime desc limit " + offSet + "," + limits
    }
    client.prepare(sql).select[String](userId.toString) { row =>
      val id = row("id").map{
        case StringValue(str) => str
        case _ => ""
      }.get
      id
    }
  }
}


