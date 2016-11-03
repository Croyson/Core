package com.wisedu.next.services

import javax.inject.{Inject, Singleton}

import com.twitter.finagle.exp.mysql.Result
import com.twitter.util.Future
import com.wisedu.next.annotations.NewSqlDatabase
import com.wisedu.next.models._

@Singleton
class GroupBaseService {

  @Inject
  @NewSqlDatabase var appDatabase: AppDatabase = _

  def getGroupById(groupId: String): Future[Option[Group]] = {
    appDatabase.groups.getById(groupId)
  }

  def delGroupById(groupId: String): Future[Result] = {
    appDatabase.groups.delById(groupId)
  }

  def collGroup(collegeId:String,groupName: String, groupType: String, limit: Int, offSet: Int): Future[Seq[Group]] = {
    appDatabase.groups.collGroups(collegeId,groupName, groupType, limit, offSet)
  }

  def collGroupsSize(collegeId:String,groupName: String, groupType: String): Future[Int] = {
    appDatabase.groups.collGroupsSize(collegeId,groupName, groupType)
  }


  def collGroupPageList(collegeId:String,groupName: String, groupType: String, limit: Int, offSet: Int): Future[(Seq[Group], Int)] = {
    val groupsF = appDatabase.groups.collGroups(collegeId,groupName, groupType, limit, offSet)
    val groupsSizeF = appDatabase.groups.collGroupsSize(collegeId,groupName, groupType)
    for {
      groups <- groupsF
      groupsSize <- groupsSizeF
    } yield (groups, groupsSize)
  }


  def addGroup(group: Group): Future[Result] = {
    appDatabase.groups.insGroup(group)
  }

  def updGroup(group: Group): Future[Result] = {
    appDatabase.groups.updGroup(group)
  }

  def getPermissionsByGroupId(groupId: String): Future[Seq[GroupPermission]] = {
    appDatabase.groupPermissions.getByGroupId(groupId)
  }

  def getPermissionsByCollegeId(collegeId: String): Future[Seq[GroupPermission]] = {
    appDatabase.groupPermissions.getByCollegeId(collegeId)
  }

  def delPermissionsByGroupId(groupId: String): Future[Result] = {
    appDatabase.groupPermissions.delById(groupId)
  }

  def addGroupPermission(groupPermission: GroupPermission): Future[Result] = {
    appDatabase.groupPermissions.insGroupPermission(groupPermission)
  }

  def getGroupByCollegeId(collegeId: String, groupType: String, limits: Int, offset: Int): Future[Option[Group]] = {
    appDatabase.groups.collGroups(collegeId,"",groupType,limits, offset).map(_.headOption)
  }

  def getPosterGroupById(channel_id: String,types:Int): Future[Option[Group]] = {
    appDatabase.groups.getPosterGroupById(channel_id,types)
  }

}