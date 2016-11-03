package com.wisedu.next.models

import com.twitter.finagle.exp.mysql._
import com.twitter.finagle.redis.Client
import com.twitter.util.Future
import com.wisedu.next.types.NextTypes.MysqlClient

case class CirclePermission(circleId: String, collegeId: String, isDefaultDisPlay: Int, isDefaultSave: Int)

abstract class ConcreteCirclePermissions(val client: MysqlClient, val redisClient: Client) {

  def getByCircleId(circleId: String): Future[Seq[CirclePermission]] = {
    val query = "SELECT * FROM circlePermissions WHERE CircleId = '" + circleId + "' "
    client.select[CirclePermission](query) { row =>
      val circleId = row("circleId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val collegeId = row("collegeId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val isDefaultDisPlay = row("isDefaultDisPlay").map {
        case IntValue(str) => str
        case _ => 0
      }.get
      val isDefaultSave = row("isDefaultSave").map {
        case IntValue(str) => str
        case _ => 0
      }.get
      CirclePermission(circleId, collegeId,isDefaultDisPlay,isDefaultSave)
    }
  }

  def getByCollegeId(collegeId: String,isDefaultDisPlay:String,isDefaultSave:String): Future[Seq[CirclePermission]] = {
    var query = "SELECT * FROM circlePermissions WHERE collegeId = '" + collegeId + "' "

    if(isDefaultDisPlay.nonEmpty){
      query += " and isDefaultDisPlay = '" +isDefaultDisPlay+ "'"
    }

    if(isDefaultSave.nonEmpty){
      query += " and isDefaultSave = '" +isDefaultSave+ "'"
    }

    client.select[CirclePermission](query) { row =>
      val circleId = row("circleId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val collegeId = row("collegeId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val isDefaultDisPlay = row("isDefaultDisPlay").map {
        case IntValue(str) => str
        case _ => 0
      }.get
      val isDefaultSave = row("isDefaultSave").map {
        case IntValue(str) => str
        case _ => 0
      }.get
      CirclePermission(circleId, collegeId,isDefaultDisPlay,isDefaultSave)
    }
  }

  def delById(circleId: String): Future[Result] = {
    val delSql = "delete from circlePermissions where CircleId = '" + circleId + "'"
    client.query(delSql)
  }

  def insCirclePermission(circlePermission: CirclePermission): Future[Result] = {
    val insertSQL =
      """INSERT INTO circlePermissions (circleId, collegeId,isDefaultDisPlay,isDefaultSave
             ) VALUES (?, ?, ?, ?)"""
    client.prepare(insertSQL)(
      circlePermission.circleId, circlePermission.collegeId, circlePermission.isDefaultDisPlay, circlePermission.isDefaultSave
    )
  }

}