package com.wisedu.next.models

import java.util.TimeZone

import com.twitter.finagle.exp.mysql._
import com.twitter.util.Future
import com.wisedu.next.types.NextTypes.MysqlClient
import org.joda.time.DateTime

case class Group(groupId: String, groupName: String, description: String, feedTypeCL: String, tagCL: String,
                 groupType: Int, iconUrl: String, backImgUrl: String, sTime: DateTime, eTime: DateTime,
                 sortNo: Int, isSumGroup: Int, groupParam: String, strutsType: Int,permissionsLimit:Int,
                 posterGroupId:String, isAnonymous:Int)

abstract class ConcreteGroups(val client: MysqlClient) {
  val timeValue = new TimestampValue(TimeZone.getDefault, TimeZone.getDefault)

  def getById(groupId: String): Future[Option[Group]] = {
    val query = "SELECT * FROM groups WHERE groupId = '" + groupId.toString + "' limit 1 "
    client.select[Group](query) { row =>
      val groupId = row("groupId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val groupName = row("groupName").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val description = row("description").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val feedTypeCL = row("feedTypeCL").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val tagCL = row("tagCL").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(groupType) = row("groupType").getOrElse("")
      val iconUrl = row("iconUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val backImgUrl = row("backImgUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(sTime) = row("sTime").getOrElse("")
      val timeValue(eTime) = row("eTime").getOrElse("")
      val IntValue(sortNo) = row("sortNo").getOrElse(0)
      val IntValue(isSumGroup) = row("isSumGroup").getOrElse(0)
      val groupParam = row("groupParam").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val posterGroupId = row("posterGroupId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(strutsType) = row("strutsType").getOrElse(0)
      val IntValue(permissionsLimit) = row("permissionsLimit").getOrElse(0)
      val IntValue(isAnonymous) = row("isAnonymous").getOrElse(0)
      Group(groupId, groupName, description, feedTypeCL, tagCL, groupType, iconUrl, backImgUrl,
        new DateTime(sTime), new DateTime(eTime), sortNo, isSumGroup, groupParam,strutsType,permissionsLimit,posterGroupId, isAnonymous)
    }.map(_.headOption)
  }

  def delById(groupId: String): Future[Result] = {
    val delSql = "delete from groups where groupId = '" + groupId.toString + "'"
    client.query(delSql)
  }

  def insGroup(group: Group): Future[Result] = {
    val insertSQL =
      """INSERT INTO groups (groupId, groupName, description, feedTypeCL, tagCL, groupType, iconUrl,
          backImgUrl, sTime, eTime,sortNo,isSumGroup,groupParam,strutsType,permissionsLimit,posterGroupId,isAnonymous) VALUES
          (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?)"""
    client.prepare(insertSQL)(group.groupId.toString, group.groupName, group.description, group.feedTypeCL,
      group.tagCL, group.groupType, group.iconUrl,
      group.backImgUrl, group.sTime.toDate, group.eTime.toDate, group.sortNo, group.isSumGroup, group.groupParam,
      group.strutsType,group.permissionsLimit,group.posterGroupId,group.isAnonymous)
  }

  def updGroup(group: Group): Future[Result] = {
    val insertSQL =
      """update groups  set groupName = ?, description = ?, feedTypeCL = ?, tagCL = ?, groupType = ?,
                       iconUrl = ?, backImgUrl = ?, sTime = ?,eTime = ?,sortNo = ?,isSumGroup = ?,groupParam = ?,
                       strutsType = ?,permissionsLimit=?,posterGroupId = ?, isAnonymous = ?
                       where groupId = ? """
    client.prepare(insertSQL)(group.groupName, group.description, group.feedTypeCL, group.tagCL, group.groupType,
      group.iconUrl, group.backImgUrl, group.sTime.toDate, group.eTime.toDate, group.sortNo, group.isSumGroup,
      group.groupParam, group.strutsType,group.permissionsLimit,group.posterGroupId,group.isAnonymous,group.groupId.toString)
  }

  def collGroups(collegeId:String,groupName: String, groupType: String, limit: Int, offSet: Int): Future[Seq[Group]] = {
    var query = "SELECT * FROM groups where 1=1 "
    if (!groupName.isEmpty) {
      query += " and groupName like '%" + groupName + "%'"
    }

    if (!groupType.isEmpty) {
      query += " and groupType in ('" + groupType.replaceAll(",", "','") + "')"
    }

    if (!collegeId.isEmpty) {
      query += " and (permissionsLimit = 0 or  EXISTS (select groupId from groupPermissions where groupPermissions.groupId = groups.groupId and collegeId = '"+collegeId +"' ))"
    }
    query += " order by sortNo asc limit " + offSet + "," + limit

    client.select[Group](query) { row =>
      val groupId = row("groupId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val groupName = row("groupName").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val description = row("description").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val feedTypeCL = row("feedTypeCL").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val tagCL = row("tagCL").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(groupType) = row("groupType").getOrElse("")
      val iconUrl = row("iconUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val backImgUrl = row("backImgUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(sTime) = row("sTime").getOrElse("")
      val timeValue(eTime) = row("eTime").getOrElse("")
      val IntValue(sortNo) = row("sortNo").getOrElse(0)
      val IntValue(isSumGroup) = row("isSumGroup").getOrElse(0)
      val groupParam = row("groupParam").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val posterGroupId = row("posterGroupId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(strutsType) = row("strutsType").getOrElse(0)
      val IntValue(permissionsLimit) = row("permissionsLimit").getOrElse(0)
      val IntValue(isAnonymous) = row("isAnonymous").getOrElse(0)
      Group(groupId, groupName, description, feedTypeCL, tagCL, groupType, iconUrl, backImgUrl,
        new DateTime(sTime), new DateTime(eTime), sortNo, isSumGroup, groupParam,strutsType,permissionsLimit,posterGroupId, isAnonymous)
    }
  }

  def collGroupsSize(collegeId:String,groupName: String, groupType: String): Future[Int] = {
    var query = "SELECT count(1) as value FROM groups where 1=1 "
    if (!groupName.isEmpty) {
      query += " and groupName like '%" + groupName + "%'"
    }

    if (!groupType.isEmpty) {
      query += " and groupType = '" + groupType + "'"
    }
    if (!collegeId.isEmpty) {
      query += " and ( permissionsLimit = 0 or EXISTS (select groupId from groupPermissions where groupPermissions.groupId = groups.groupId and collegeId = '"+collegeId +"' ))"
    }
    client.select(query) { row =>
      val LongValue(value) = row("value").getOrElse("0")
      value.toInt
    }.map(_.head)
  }

  def getPosterGroupById(channel_id: String,types:Int): Future[Option[Group]] = {
    var query = "select * from groups where groupId = "
    if(types==0){
      query += "(select posterGroupId from groups where groupId='"+ channel_id +"')"
    }else{
      query += "(select posterGroupId from circles where circleId='"+ channel_id +"')"
    }

    client.select[Group](query) { row =>
      val groupId = row("groupId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val groupName = row("groupName").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val description = row("description").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val feedTypeCL = row("feedTypeCL").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val tagCL = row("tagCL").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(groupType) = row("groupType").getOrElse("")
      val iconUrl = row("iconUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val backImgUrl = row("backImgUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(sTime) = row("sTime").getOrElse("")
      val timeValue(eTime) = row("eTime").getOrElse("")
      val IntValue(sortNo) = row("sortNo").getOrElse(0)
      val IntValue(isSumGroup) = row("isSumGroup").getOrElse(0)
      val groupParam = row("groupParam").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val posterGroupId = row("posterGroupId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(strutsType) = row("strutsType").getOrElse(0)
      val IntValue(permissionsLimit) = row("permissionsLimit").getOrElse(0)
      val IntValue(isAnonymous) = row("isAnonymous").getOrElse(0)
      Group(groupId, groupName, description, feedTypeCL, tagCL, groupType, iconUrl, backImgUrl,
        new DateTime(sTime), new DateTime(eTime), sortNo, isSumGroup, groupParam,strutsType,permissionsLimit,posterGroupId, isAnonymous)
    }.map(_.headOption)
  }
}