package com.wisedu.next.models

import com.twitter.finagle.exp.mysql._
import com.twitter.util.Future
import com.wisedu.next.types.NextTypes.MysqlClient

case class GroupPermission(groupId: String, collegeId: String, permissionType: Int)

abstract class ConcreteGroupPermissions(val client: MysqlClient) {

  def getByGroupId(groupId: String): Future[Seq[GroupPermission]] = {
    val query = "SELECT * FROM groupPermissions WHERE groupId = '" + groupId + "' "
    client.select[GroupPermission](query) { row =>
      val groupId = row("groupId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val collegeId = row("collegeId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val permissionType = row("permissionType").map {
        case IntValue(str) => str
        case _ => 0
      }.get
      GroupPermission(groupId, collegeId, permissionType)
    }
  }

  def getByCollegeId(collegeId: String): Future[Seq[GroupPermission]] = {
    val query = "SELECT * FROM groupPermissions WHERE collegeId = '" + collegeId.toString + "' "
    client.select[GroupPermission](query) { row =>
      val groupId = row("groupId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val collegeId = row("collegeId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val permissionType = row("permissionType").map {
        case IntValue(str) => str
        case _ => 0
      }.get
      GroupPermission(groupId,collegeId, permissionType)
    }
  }

  def delById(groupId: String): Future[Result] = {
    val delSql = "delete from groupPermissions where groupId = '" + groupId + "'"
    client.query(delSql)
  }

  def insGroupPermission(groupPermission: GroupPermission): Future[Result] = {
    val insertSQL =
      """INSERT INTO groupPermissions (groupId, collegeId,permissionType
             ) VALUES (?, ?, ?)"""
    client.prepare(insertSQL)(groupPermission.groupId,groupPermission.collegeId.toString,groupPermission.permissionType)
  }

}