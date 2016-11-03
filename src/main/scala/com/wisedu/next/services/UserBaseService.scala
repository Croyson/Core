package com.wisedu.next.services

import java.util.UUID
import javax.inject.{Inject, Singleton}

import com.twitter.finagle.exp.mysql.Result
import com.twitter.util.Future
import com.websudos.phantom.dsl.ResultSet
import com.wisedu.next.annotations.NewSqlDatabase
import com.wisedu.next.models._
import org.joda.time.DateTime

@Singleton
class UserBaseService {

  @Inject
  @NewSqlDatabase var appDatabase: AppDatabase = _

  def getUserById(userId: UUID): Future[Option[User]] = {
    appDatabase.users.getUserByIdWithCache(userId.toString).flatMap {
      case Some(user) => Future(Some(user))
      case None => appDatabase.users.getById(userId).map {
        user => user.map { item =>
          getUserStatById(item.userId).map {
            //查询的时候插入用户状态,解决老用户的问题
            case None => insUserStat(UserStat(item.userId, 0, 0, 0))
          }
          appDatabase.users.setUserByIdWithCache(item)
          item
        }
      }
    }
  }


  def getUserByPhoneNo(phoneNo: String): Future[Option[User]] = {
    appDatabase.users.getByPhoneNo(phoneNo)
  }

  def getUserByAlias(alias: String): Future[Option[User]] = {
    appDatabase.users.getByAlias(alias)
  }

  def getUserByToken(tokenS: String): Future[Option[User]] = {
    appDatabase.users.getByToken(tokenS)
  }

  def getSnsByOpenId(openId: String, snsType: Int): Future[Option[UserSnsInfo]] = {
    appDatabase.userSnsInfos.getSnsByOpenId(openId, snsType)
  }


  def getSnsByUserId(userId: UUID): Future[Seq[UserSnsInfo]] = {
    appDatabase.userSnsInfos.getSnsByUserId(userId)
  }

  def insAuthCode(authCode: AuthCode): Future[ResultSet] = {
    appDatabase.authCodes.insAuthCode(authCode)
  }

  def getAuthCodeById(phoneNo: String): Future[Option[AuthCode]] = {
    appDatabase.authCodes.getById(phoneNo)
  }

  def insUser(user: User): Future[Result] = {
    appDatabase.users.setUserByIdWithCache(user)
    appDatabase.users.insUser(user)
  }

  def insUserSnsInfo(userSnsInfo: UserSnsInfo): Future[Result] = {
    appDatabase.userSnsInfos.insUserSnsInfo(userSnsInfo)
  }

  def delUserSnsInfo(userId: UUID, openId: String, snsType: Int): Future[Result] = {
    appDatabase.userSnsInfos.delUserSnsInfo(userId, openId, snsType)
  }

  def updUser(user: User): Future[Result] = {
    appDatabase.users.setUserByIdWithCache(user)
    appDatabase.users.updUser(user)
  }

  def updUserToken(userId: UUID, tokenS: String): Future[Unit] = {
    appDatabase.users.updToken(userId, tokenS)
    appDatabase.users.getById(userId).map {
      user => user.map { item =>
        appDatabase.users.setUserByIdWithCache(item)
        item
      }
    }
  }

  def insUserService(userService: UserService): Future[Result] = {
    appDatabase.userServices.insUserService(userService)
  }

  def delUserService(userId: UUID, serId: String): Future[Result] = {
    appDatabase.userServices.delById(userId, serId)
  }

  def checkUserLookService(userId: UUID, serId: String): Future[String] = {
    appDatabase.userServices.getById(userId, serId).map {
      case Some(item) => "1"
      case None => "0"
    }
  }

  //获取唯一的昵称,每次发现重复后面加入两位随机数
  def getUniqueAlice(alice: String): Future[String] = {
    appDatabase.users.getByAlias(alice).flatMap {
      case Some(user) => getUniqueAlice(alice + BaseFunctions.getRandomString(2))
      case None => Future(alice)
    }
  }

  def getByDeviceId(deviceId: String): Future[Seq[User]] = {
    appDatabase.users.collByDeviceId(deviceId)
  }

  def collUsers(name: String, isAnonymousUser: String, status: String, tagId: String,
    limit: Int, offset: Int): Future[Seq[User]] ={
    appDatabase.users.collUsers(name, isAnonymousUser, status, tagId, offset,limit)
  }

  def  collUsersSize(name: String, isAnonymousUser: String, status: String, tagId: String):Future[Int]={
    appDatabase.users.collUsersSize(name, isAnonymousUser, status, tagId)
  }

  def collUserPageList(name: String, isAnonymousUser: String, status: String, tagId: String,
                       limit: Int, offset: Int): Future[(Seq[User], Int)] = {
    val usersF = appDatabase.users.collUsers(name, isAnonymousUser, status, tagId, offset,limit)
    val usersSizeF = appDatabase.users.collUsersSize(name, isAnonymousUser, status, tagId)

    for {
      users <- usersF
      usersSize <- usersSizeF
    } yield (users, usersSize)
  }

  //获取用户标签
  def getUserTagsByUserId(userId: UUID): Future[Seq[UserTag]] = {
    appDatabase.userTags.getUserTagsByUserId(userId)
  }

  //删除用户标签
  def delUserTagsByUserId(userId: UUID): Future[Result] = {
    appDatabase.userTags.delUserTagsByUserId(userId)
  }

  //添加用户标签
  def insUserTag(userTag: UserTag): Future[Result] = {
    appDatabase.userTags.insUserTag(userTag)
  }

  //获取用户属性 和 tagid 对应关系信息
  def getUserTagKeyInfo(userKey: String, keyType: Int): Future[Option[UserTagKey]] = {
    appDatabase.userTagKeys.getTagIdById(userKey, keyType)
  }

  //同步用户 性别  和  学校的标签
  def sysUserTags(userId: UUID): Future[Result] = {
    appDatabase.userTags.delUserTagsByUserId(userId).flatMap {
      rst => appDatabase.userTags.sysUserTags(userId)
    }
  }

  def getSerByUser(userId: UUID, limits: Int, offset: Int): Future[Seq[Service]] = {
    appDatabase.userServices.getSerByUser(userId, limits, offset)
  }

  // 更新用户学校信息
  def updUserCollege(collegeId: String, collegeName: String, userId: String): Future[Result] = {
    appDatabase.users.delUserByIdWithCache(userId)
    appDatabase.users.updUserCollege(collegeId, collegeName, userId)
  }

  //修改密码
  def updUserPassword(userId: UUID, password: String): Future[Result] = {
    appDatabase.users.delUserByIdWithCache(userId.toString)
    appDatabase.users.updUserPassword(userId, password)
  }

  //修改背景图
  def updBackgroundImg(userId: UUID, backgroundImg: String): Future[Result] = {
    appDatabase.users.delUserByIdWithCache(userId.toString)
    appDatabase.users.updBackgroundImg(userId, backgroundImg)
  }

  // 根据
  def getUsersByUpdateId(updateId: String, limits: Int, offset: Int): Future[Seq[User]] = {
    appDatabase.users.getUsersByUpdateId(updateId, limits, offset)
  }

  // 更新用户头像
  def updUserImg(userId: UUID, imgUrl: String): Future[Result] = {
    appDatabase.users.delUserByIdWithCache(userId.toString)
    appDatabase.users.updImgUrl(userId, imgUrl)
  }

  // 根据用户ID获取所有用户信息
  def collUsersByIds(userIds: String): Future[Seq[User]] = {
    appDatabase.users.collUsersByIds(userIds)
  }


  def updUserStatus(deviceId: String, userId: String): Future[Result] = {
    appDatabase.users.delUserByIdWithCache(userId)
    appDatabase.users.updStatusByDeviceId(deviceId, 0).flatMap {
      rst => appDatabase.users.updStatusByUserId(userId, 1)
    }
  }


  def getDefaultUser(): User = {
    User(UUID.randomUUID(), "匿名用户", "匿名用户",
      " ", " ", 1, " ", " ", " ", " ", " ", " ", " ", "", "", "",
      DateTime.now, "", " ", "", "", "", DateTime.now, "", "", 0, 0, "", "", 1)
  }

  // 获取用户关注的媒体数 或是媒体下用户订阅数
  def collUserServiceSize(userId: String, serId: String): Future[Int] = {
    appDatabase.userServices.collUserServiceSize(userId, serId)
  }

  // 根据资讯ID获取点赞用户相关信息
  def getUsersByFeedId(feedId: String, limits: Int, offset: Int): Future[Seq[User]] = {
    appDatabase.users.getUsersByFeedId(feedId, limits, offset)
  }

  //插入用户关系
  def insUserRelation(userRelation: UserRelation): Future[Result] = {
    appDatabase.userRelations.insUserRelation(userRelation)
  }

  //删除用户关系
  def delUserRelationByUserId(userId: UUID, followId: UUID, rtype: Int): Future[Result] = {
    appDatabase.userRelations.delUserRelationByUserId(userId, followId, rtype)
  }

  //获取一个用户关系
  def getRelationById(userId: UUID, followId: UUID, rtype: Int): Future[Option[UserRelation]] = {
    appDatabase.userRelations.getById(userId, followId, rtype)
  }

  //获取一个用户的关系 rtype 0 粉丝 1关注
  def getUserRelations(userId: UUID, rtype: Int, offSet: Int, limits: Int): Future[Seq[UserRelation]] = {
    appDatabase.userRelations.getUserRelations(userId, rtype, offSet, limits)
  }

  //获取用户的状态值  粉丝数  关注数
  def getUserStatById(userId: UUID): Future[Option[UserStat]] = {
    appDatabase.userStats.getById(userId)
  }

  //插入用户状态
  def insUserStat(userStat: UserStat): Future[ResultSet] = {
    appDatabase.userStats.insUserStat(userStat)
  }

  //增加粉丝数
  def incUserFansCount(userId: UUID): Future[ResultSet] = {
    getUserStatById(userId).map {
      case None => insUserStat(UserStat(userId, 0, 0, 0))
    }
    appDatabase.userStats.incUserFansCount(userId)
  }

  //减少粉丝数
  def decUserFansCount(userId: UUID): Future[ResultSet] = {
    getUserStatById(userId).map {
      case None => insUserStat(UserStat(userId, 0, 0, 0))
    }
    appDatabase.userStats.decUserFansCount(userId)
  }

  //增加关注数
  def incFollowCount(userId: UUID): Future[ResultSet] = {
    getUserStatById(userId).map {
      case None => insUserStat(UserStat(userId, 0, 0, 0))
    }
    appDatabase.userStats.incFollowCount(userId)
  }

  //减少关注数
  def decFollowCount(userId: UUID): Future[ResultSet] = {
    getUserStatById(userId).map {
      case None => insUserStat(UserStat(userId, 0, 0, 0))
    }
    appDatabase.userStats.decFollowCount(userId)
  }

  def incMsgCount(userId: UUID): Future[ResultSet] = {
    getUserStatById(userId).map {
      case None => insUserStat(UserStat(userId, 0, 0, 0))
    }
    appDatabase.userStats.incMsgCount(userId)
  }

  def decMsgCount(userId: UUID): Future[ResultSet] = {
    getUserStatById(userId).map {
      case None => insUserStat(UserStat(userId, 0, 0, 0))
    }
    appDatabase.userStats.decMsgCount(userId)
  }

  // 获取关注用户或粉丝用户ID
  def getUserIds(userId: UUID, offSet: Int, limits: Int, method: Int): Future[Seq[String]] = {
    appDatabase.userRelations.getUserIds(userId, offSet, limits, method)
  }

  //获取用户消息数量
  def getUserMsgCount(userId: UUID): Future[Long] = {
    appDatabase.userStats.getUserMsgCount(userId)
  }
  //增加用户消息数量
  def incUserMsgCount(userId: UUID): Future[Long] = {
    appDatabase.userStats.incUserMsgCount(userId)
  }

  //重置用户消息数量
  def resetUserMsgCount(userId: UUID): Future[Unit] = {
    appDatabase.userStats.resetUserMsgCount(userId)
  }
  //获取上一次用户查看消息列表时间
  def getUserMsgListLastTime(userId: UUID): Future[DateTime] = {
    appDatabase.userStats.getUserMsgListLastTime(userId).map{
      case Some(rst) => rst
      case None =>  DateTime.now
    }
  }
  //获取重置用户查看消息列表时间
  def resetUserMsgListLastTime(userId: UUID): Future[Unit] = {
    appDatabase.userStats.resetUserMsgListLastTime(userId)
  }

}