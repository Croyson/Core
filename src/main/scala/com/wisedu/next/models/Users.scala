package com.wisedu.next.models

import _root_.java.lang.{Long => JLong}
import java.util.{TimeZone, UUID}

import com.twitter.finagle.exp.mysql._
import com.twitter.finagle.redis.Client
import com.twitter.finagle.redis.util.StringToChannelBuffer
import com.twitter.util.Future
import com.wisedu.next.consts.UserKeys
import com.wisedu.next.services.BaseFunctions
import com.wisedu.next.types.NextTypes.MysqlClient
import org.joda.time.DateTime

case class User(userId: UUID, alias: String, name: String, password: String, imgUrl: String, sex: Int,
                phoneNo: String, mSchool: String, collegeId: String, collegeName: String, depart: String,
                cla: String, majors: String, dorm: String, degree: String, freshDate: String, birthDate: DateTime,
                home: String, loveSt: String, sign: String, interests: String, actives: String, cTime: DateTime,
                tokenS: String, deviceId: String, isAnonymousUser: Int, status: Int,backgroundImg:String,descr:String,
                 role:Int)

abstract class ConcreteUsers(val client: MysqlClient, val redisClient: Client) {
  val timeValue = new TimestampValue(TimeZone.getDefault, TimeZone.getDefault)

  def getUserByIdWithCache(userId: String): Future[Option[User]] = {
    val key = UserKeys.userId.format(userId)
    redisClient.get(StringToChannelBuffer(key)).map {
      result => result.map(item => BaseFunctions.channelBuffer2Object[User](item))
    }
  }

  def setUserByIdWithCache(user: User): Future[Unit] = {
    val key = UserKeys.userId.format(user.userId)
    redisClient.setEx(StringToChannelBuffer(key), 60 * 24 * 7, BaseFunctions.object2ChannelBuffer(user))
  }

  def delUserByIdWithCache(userId: String): Future[JLong] = {
    val key = UserKeys.userId.format(userId)
    redisClient.del(Seq(StringToChannelBuffer(key)))
  }

  def insUser(user: User): Future[Result] = {

    val insertSql =
      """INSERT INTO users (userId, alias, name, password, imgUrl, sex, phoneNo, mSchool, collegeId,
      collegeName, depart, cla, majors, dorm, degree, freshDate, birthDate, home, loveSt, sign, interests, actives,
      cTime, tokenS, deviceId, isAnonymousUser,status,backgroundImg,descr,role) VALUES (?,?, ? , ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?)"""
    val ps = client.prepare(insertSql)
    ps(user.userId.toString, user.alias, user.name, user.password, user.imgUrl, user.sex, user.phoneNo, user.mSchool,
      user.collegeId.toString, user.collegeName, user.depart, user.cla, user.majors, user.dorm, user.degree, user.freshDate,
      user.birthDate.toDate, user.home, user.loveSt, user.sign, user.interests, user.actives, user.cTime.toDate, user.tokenS, user.deviceId,
      user.isAnonymousUser, user.status,user.backgroundImg,user.descr,user.role)
  }

  def delUser(userId: UUID): Future[Result] = {
    val delSql = "delete from users where userId = '" + userId.toString + "'"
    client.query(delSql)
  }

  def updUser(user: User): Future[Result] = {
    val updSql =
      """update users set alias = ?, name = ?, imgUrl = ?, sex = ?, phoneNo = ?, mSchool = ?,
      collegeId = ?, collegeName = ?, depart = ?, cla = ?, majors = ?, dorm = ?, degree = ?, freshDate = ?,
      birthDate = ?, home = ?, loveSt = ?, sign = ?, interests = ?, actives = ?, tokenS = ?, deviceId = ?,
      isAnonymousUser = ?,status = ? , backgroundImg = ?,descr=?,role=?
      where userId = ?"""
    val ps = client.prepare(updSql)
    ps(user.alias, user.name, user.imgUrl, user.sex, user.phoneNo, user.mSchool, user.collegeId.toString,
      user.collegeName, user.depart, user.cla, user.majors, user.dorm, user.degree, user.freshDate,
      user.birthDate.toDate, user.home, user.loveSt, user.sign, user.interests, user.actives, user.tokenS,
      user.deviceId, user.isAnonymousUser, user.status,user.backgroundImg, user.descr,user.role,user.userId.toString)
  }

  def updUserPassword(userId:UUID,password:String): Future[Result] = {
    val sql = "update users set password = ? where userId = ?"
    client.prepare(sql)(password,userId.toString)
  }

  def getById(userId: UUID): Future[Option[User]] = {
    val query = "SELECT * FROM users WHERE  userId = '" + userId.toString + "' limit 1 "
    client.select[User](query) { row =>
      val userId = row("userId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val alias = row("alias").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val name = row("name").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val password = row("password").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val imgUrl = row("imgUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(sex) = row("sex").getOrElse("1")
      val phoneNo = row("phoneNo").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val mSchool = row("mSchool").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val collegeId = row("collegeId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val collegeName = row("collegeName").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val depart = row("depart").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val cla = row("cla").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val majors = row("majors").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val dorm = row("dorm").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val degree = row("degree").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val freshDate = row("freshDate").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(birthDate) = row("birthDate").getOrElse("0000-00-00 00:00:00")
      val home = row("home").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val loveSt = row("loveSt").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val sign = row("sign").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val interests = row("interests").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val actives = row("actives").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
      val tokenS = row("tokenS").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val deviceId = row("deviceId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(isAnonymousUser) = row("isAnonymousUser").getOrElse("")
      val IntValue(status) = row("status").getOrElse("")

      val backgroundImg = row("backgroundImg").map {
        case StringValue(str) => str
        case _ => ""
      }.get

      val descr = row("descr").map {
        case StringValue(str) => str
        case _ => ""
      }.get

      val IntValue(role) = row("role").getOrElse(1)
      User(UUID.fromString(userId), alias, name, password, imgUrl, sex, phoneNo, mSchool,collegeId,
        collegeName, depart, cla, majors, dorm, degree, freshDate, new DateTime(birthDate), home, loveSt,
        sign, interests, actives, new DateTime(cTime), tokenS, deviceId,
         isAnonymousUser,status,backgroundImg,descr,role)
    }.map(_.headOption)
  }

  def collUsers(offSet: Int, limits: Int): Future[Seq[User]] = {
    val query = "SELECT * FROM users order by cTime desc limit " + offSet + "," + limits
    client.select[User](query) { row =>
      val userId = row("userId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val alias = row("alias").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val name = row("name").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val password = row("password").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val imgUrl = row("imgUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(sex) = row("sex").getOrElse("1")
      val phoneNo = row("phoneNo").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val mSchool = row("mSchool").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val collegeId = row("collegeId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val collegeName = row("collegeName").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val depart = row("depart").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val cla = row("cla").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val majors = row("majors").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val dorm = row("dorm").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val degree = row("degree").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val freshDate = row("freshDate").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(birthDate) = row("birthDate").getOrElse("0000-00-00 00:00:00")
      val home = row("home").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val loveSt = row("loveSt").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val sign = row("sign").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val interests = row("interests").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val actives = row("actives").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
      val tokenS = row("tokenS").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val deviceId = row("deviceId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(isAnonymousUser) = row("isAnonymousUser").getOrElse("")
      val IntValue(status) = row("status").getOrElse("")
      val backgroundImg = row("backgroundImg").map {
        case StringValue(str) => str
        case _ => ""
      }.get
       val descr = row("descr").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(role) = row("role").getOrElse(1)
      User(UUID.fromString(userId), alias, name, password, imgUrl, sex, phoneNo, mSchool,collegeId,
        collegeName, depart, cla, majors, dorm, degree, freshDate, new DateTime(birthDate), home, loveSt,
        sign, interests, actives, new DateTime(cTime), tokenS, deviceId,
         isAnonymousUser,status,backgroundImg,descr,role)
    }
  }

  def updImgUrl(userId: UUID, imgUrl: String): Future[Result] = {
    val updSql = "update users set imgUrl = ? where userId = ? "
    client.prepare(updSql)(imgUrl, userId.toString)
  }

  def updBackgroundImg(userId: UUID, backgroundImg: String): Future[Result] = {
    val updSql = "update users set backgroundImg = ? where userId = ? "
    client.prepare(updSql)(backgroundImg, userId.toString)
  }

  def updToken(userId: UUID, tokenS: String): Future[Result] = {
    val updSql = "update users set tokenS = ? where userId = ? "
    client.prepare(updSql)(tokenS, userId.toString)
  }

  def updStatusByDeviceId(deviceId: String,status:Int): Future[Result] = {
    val updSql = "update users set status = ? where deviceId = ? "
    client.prepare(updSql)(status,deviceId)
  }

  def updStatusByUserId(userId: String,status:Int): Future[Result] = {
    val updSql = "update users set status = ? where  userId = ? "
    client.prepare(updSql)(status,userId)
  }

  def getByPhoneNo(phoneNo: String): Future[Option[User]] = {
    val query = "SELECT * FROM users WHERE phoneNo = '" + phoneNo + "' limit 1 "
    client.select[User](query) { row =>
      val userId = row("userId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val alias = row("alias").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val name = row("name").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val password = row("password").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val imgUrl = row("imgUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(sex) = row("sex").getOrElse("1")
      val phoneNo = row("phoneNo").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val mSchool = row("mSchool").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val collegeId = row("collegeId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val collegeName = row("collegeName").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val depart = row("depart").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val cla = row("cla").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val majors = row("majors").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val dorm = row("dorm").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val degree = row("degree").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val freshDate = row("freshDate").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(birthDate) = row("birthDate").getOrElse("0000-00-00 00:00:00")
      val home = row("home").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val loveSt = row("loveSt").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val sign = row("sign").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val interests = row("interests").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val actives = row("actives").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
      val tokenS = row("tokenS").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val deviceId = row("deviceId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(isAnonymousUser) = row("isAnonymousUser").getOrElse("")
      val IntValue(status) = row("status").getOrElse("")
     val backgroundImg = row("backgroundImg").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val descr = row("descr").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(role) = row("role").getOrElse(1)
      User(UUID.fromString(userId), alias, name, password, imgUrl, sex, phoneNo, mSchool,collegeId,
        collegeName, depart, cla, majors, dorm, degree, freshDate, new DateTime(birthDate), home, loveSt,
        sign, interests, actives, new DateTime(cTime), tokenS, deviceId,
         isAnonymousUser,status,backgroundImg,descr,role)
    }.map(_.headOption)
  }

  def getByAlias(alias: String): Future[Option[User]] = {
    val query = "SELECT * FROM users WHERE alias = '" + alias + "' limit 1 "
    client.select[User](query) { row =>
      val userId = row("userId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val alias = row("alias").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val name = row("name").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val password = row("password").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val imgUrl = row("imgUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(sex) = row("sex").getOrElse("1")
      val phoneNo = row("phoneNo").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val mSchool = row("mSchool").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val collegeId = row("collegeId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val collegeName = row("collegeName").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val depart = row("depart").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val cla = row("cla").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val majors = row("majors").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val dorm = row("dorm").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val degree = row("degree").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val freshDate = row("freshDate").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(birthDate) = row("birthDate").getOrElse("0000-00-00 00:00:00")
      val home = row("home").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val loveSt = row("loveSt").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val sign = row("sign").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val interests = row("interests").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val actives = row("actives").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
      val tokenS = row("tokenS").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val deviceId = row("deviceId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(isAnonymousUser) = row("isAnonymousUser").getOrElse("")
      val IntValue(status) = row("status").getOrElse("")
      val backgroundImg = row("backgroundImg").map {
        case StringValue(str) => str
        case _ => ""
      }.get
       val descr = row("descr").map {
        case StringValue(str) => str
        case _ => ""
      }.get
     val IntValue(role) = row("role").getOrElse(1)
      User(UUID.fromString(userId), alias, name, password, imgUrl, sex, phoneNo, mSchool,collegeId,
        collegeName, depart, cla, majors, dorm, degree, freshDate, new DateTime(birthDate), home, loveSt,
        sign, interests, actives, new DateTime(cTime), tokenS, deviceId,
         isAnonymousUser,status,backgroundImg,descr,role)
    }.map(_.headOption)
  }

  def getByToken(tokenS: String): Future[Option[User]] = {
    val query = "SELECT * FROM users WHERE tokenS = '" + tokenS + "' limit 1 "
    client.select[User](query) { row =>
      val userId = row("userId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val alias = row("alias").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val name = row("name").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val password = row("password").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val imgUrl = row("imgUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(sex) = row("sex").getOrElse("1")
      val phoneNo = row("phoneNo").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val mSchool = row("mSchool").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val collegeId = row("collegeId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val collegeName = row("collegeName").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val depart = row("depart").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val cla = row("cla").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val majors = row("majors").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val dorm = row("dorm").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val degree = row("degree").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val freshDate = row("freshDate").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(birthDate) = row("birthDate").getOrElse("0000-00-00 00:00:00")
      val home = row("home").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val loveSt = row("loveSt").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val sign = row("sign").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val interests = row("interests").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val actives = row("actives").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
      val tokenS = row("tokenS").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val deviceId = row("deviceId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(isAnonymousUser) = row("isAnonymousUser").getOrElse("")
      val IntValue(status) = row("status").getOrElse("")
     val backgroundImg = row("backgroundImg").map {
        case StringValue(str) => str
        case _ => ""
      }.get
       val descr = row("descr").map {
        case StringValue(str) => str
        case _ => ""
      }.get
     val IntValue(role) = row("role").getOrElse(1)
      User(UUID.fromString(userId), alias, name, password, imgUrl, sex, phoneNo, mSchool,collegeId,
        collegeName, depart, cla, majors, dorm, degree, freshDate, new DateTime(birthDate), home, loveSt,
        sign, interests, actives, new DateTime(cTime), tokenS, deviceId,
         isAnonymousUser,status,backgroundImg,descr,role)
     }.map(_.headOption)
  }

  def collByDeviceId(deviceId: String): Future[Seq[User]] = {
    val query = "SELECT * FROM users WHERE deviceId = '" + deviceId + "'"
    client.select[User](query) { row =>
      val userId = row("userId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val alias = row("alias").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val name = row("name").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val password = row("password").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val imgUrl = row("imgUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(sex) = row("sex").getOrElse("1")
      val phoneNo = row("phoneNo").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val mSchool = row("mSchool").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val collegeId = row("collegeId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val collegeName = row("collegeName").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val depart = row("depart").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val cla = row("cla").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val majors = row("majors").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val dorm = row("dorm").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val degree = row("degree").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val freshDate = row("freshDate").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(birthDate) = row("birthDate").getOrElse("0000-00-00 00:00:00")
      val home = row("home").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val loveSt = row("loveSt").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val sign = row("sign").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val interests = row("interests").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val actives = row("actives").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
      val tokenS = row("tokenS").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val deviceId = row("deviceId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(isAnonymousUser) = row("isAnonymousUser").getOrElse("")
      val IntValue(status) = row("status").getOrElse("")
      val backgroundImg = row("backgroundImg").map {
        case StringValue(str) => str
        case _ => ""
      }.get
       val descr = row("descr").map {
        case StringValue(str) => str
        case _ => ""
      }.get
     val IntValue(role) = row("role").getOrElse(1)
      User(UUID.fromString(userId), alias, name, password, imgUrl, sex, phoneNo, mSchool,collegeId,
        collegeName, depart, cla, majors, dorm, degree, freshDate, new DateTime(birthDate), home, loveSt,
        sign, interests, actives, new DateTime(cTime), tokenS, deviceId,
         isAnonymousUser,status,backgroundImg,descr,role)
      }
  }


  def collUsers(name: String, isAnonymousUser: String, status: String, tagId: String, offSet: Int, limits: Int): Future[Seq[User]] = {
    var query = "SELECT * FROM users where  1=1"

    if (!name.isEmpty) {
      query += " and alias like '%" + name + "%'"
    }

    if (!isAnonymousUser.isEmpty) {
      query += " and isAnonymousUser = '" + isAnonymousUser + "'"
    }

    if (!status.isEmpty) {
      query += " and status = '" + status + "'"
    }

    if (!tagId.isEmpty) {
      query += " and EXISTS (select userId from userTags where userTags.userId = users.userId and userTags.tagId = '" + tagId + "')"
    }

    query += " order by cTime desc limit " + offSet + "," + limits


    client.select[User](query) { row =>
      val userId = row("userId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val alias = row("alias").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val name = row("name").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val password = row("password").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val imgUrl = row("imgUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(sex) = row("sex").getOrElse("1")
      val phoneNo = row("phoneNo").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val mSchool = row("mSchool").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val collegeId = row("collegeId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val collegeName = row("collegeName").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val depart = row("depart").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val cla = row("cla").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val majors = row("majors").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val dorm = row("dorm").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val degree = row("degree").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val freshDate = row("freshDate").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(birthDate) = row("birthDate").getOrElse("0000-00-00 00:00:00")
      val home = row("home").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val loveSt = row("loveSt").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val sign = row("sign").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val interests = row("interests").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val actives = row("actives").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
      val tokenS = row("tokenS").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val deviceId = row("deviceId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(isAnonymousUser) = row("isAnonymousUser").getOrElse("")
      val IntValue(status) = row("status").getOrElse("")
      val backgroundImg = row("backgroundImg").map {
        case StringValue(str) => str
        case _ => ""
      }.get
       val descr = row("descr").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(role) = row("role").getOrElse(1)
      User(UUID.fromString(userId), alias, name, password, imgUrl, sex, phoneNo, mSchool,collegeId,
        collegeName, depart, cla, majors, dorm, degree, freshDate, new DateTime(birthDate), home, loveSt,
        sign, interests, actives, new DateTime(cTime), tokenS, deviceId,
         isAnonymousUser,status,backgroundImg,descr,role)
         }
  }


  def collUsersSize(name: String, isAnonymousUser: String, status: String, tagId: String): Future[Int] = {
    var query = "SELECT count(1) as value  FROM users where  1=1"

    if (!name.isEmpty) {
      query += " and alias like '%" + name + "%'"
    }

    if (!isAnonymousUser.isEmpty) {
      query += " and isAnonymousUser = '" + isAnonymousUser + "'"
    }

    if (!status.isEmpty) {
      query += " and status = '" + status + "'"
    }

    if (!tagId.isEmpty) {
      query += " and EXISTS (select userId from userTags where userTags.userId = users.userId and userTags.tagId = '" + tagId + "')"
    }


    client.select(query) { row =>
      val LongValue(value) = row("value").getOrElse("0")
      value.toInt
    }.map(_.head)
  }

  def updUserCollege(collegeId: String, collegeName: String, userId: String): Future[Result] = {
    val updSql =
      """update users set collegeId = ?, collegeName = ? where userId = ?"""
    val ps = client.prepare(updSql)
    ps(collegeId, collegeName, userId)
  }

  def getUsersByUpdateId(updateId: String,limits: Int, offset: Int): Future[Seq[User]] = {
    val query = "select * from users where userId in (select userId  from (select userId from updateLikes where updateId='" + updateId + "'  and opType=1 order by optime desc limit " + offset + "," + limits +" ) t)"
    client.select[User](query) { row =>
      val userId = row("userId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val alias = row("alias").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val name = row("name").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val password = row("password").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val imgUrl = row("imgUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(sex) = row("sex").getOrElse("1")
      val phoneNo = row("phoneNo").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val mSchool = row("mSchool").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val collegeId = row("collegeId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val collegeName = row("collegeName").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val depart = row("depart").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val cla = row("cla").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val majors = row("majors").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val dorm = row("dorm").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val degree = row("degree").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val freshDate = row("freshDate").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(birthDate) = row("birthDate").getOrElse("0000-00-00 00:00:00")
      val home = row("home").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val loveSt = row("loveSt").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val sign = row("sign").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val interests = row("interests").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val actives = row("actives").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
      val tokenS = row("tokenS").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val deviceId = row("deviceId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(isAnonymousUser) = row("isAnonymousUser").getOrElse("")
      val IntValue(status) = row("status").getOrElse("")
      val backgroundImg = row("backgroundImg").map {
        case StringValue(str) => str
        case _ => ""
      }.get
       val descr = row("descr").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(role) = row("role").getOrElse(1)
      User(UUID.fromString(userId), alias, name, password, imgUrl, sex, phoneNo, mSchool,collegeId,
        collegeName, depart, cla, majors, dorm, degree, freshDate, new DateTime(birthDate), home, loveSt,
        sign, interests, actives, new DateTime(cTime), tokenS, deviceId,
         isAnonymousUser,status,backgroundImg,descr,role)
       }

  }

  // 根据用户ID获取所有用户信息
  def collUsersByIds(userIds: String): Future[Seq[User]] = {
    val query = "SELECT * FROM users WHERE userId in ('" + userIds.replaceAll(",", "','") + "')"
    client.select[User](query) { row =>
      val userId = row("userId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val alias = row("alias").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val name = row("name").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val password = row("password").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val imgUrl = row("imgUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(sex) = row("sex").getOrElse("1")
      val phoneNo = row("phoneNo").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val mSchool = row("mSchool").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val collegeId = row("collegeId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val collegeName = row("collegeName").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val depart = row("depart").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val cla = row("cla").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val majors = row("majors").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val dorm = row("dorm").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val degree = row("degree").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val freshDate = row("freshDate").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(birthDate) = row("birthDate").getOrElse("0000-00-00 00:00:00")
      val home = row("home").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val loveSt = row("loveSt").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val sign = row("sign").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val interests = row("interests").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val actives = row("actives").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
      val tokenS = row("tokenS").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val deviceId = row("deviceId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(isAnonymousUser) = row("isAnonymousUser").getOrElse("")
      val IntValue(status) = row("status").getOrElse("")
      val backgroundImg = row("backgroundImg").map {
        case StringValue(str) => str
        case _ => ""
      }.get
       val descr = row("descr").map {
        case StringValue(str) => str
        case _ => ""
      }.get
     val IntValue(role) = row("role").getOrElse(1)
      User(UUID.fromString(userId), alias, name, password, imgUrl, sex, phoneNo, mSchool,collegeId,
        collegeName, depart, cla, majors, dorm, degree, freshDate, new DateTime(birthDate), home, loveSt,
        sign, interests, actives, new DateTime(cTime), tokenS, deviceId,
         isAnonymousUser,status,backgroundImg,descr,role)
         }
  }

  // 根据资讯ID获取点赞用户相关信息
  def getUsersByFeedId(feedId: String,limits: Int, offset: Int): Future[Seq[User]] = {
    val query = "select * from users where userId in (select userId  from ( select userId from feedLikes where feedId='" + feedId + "' and opType=1 order by optime desc limit " + offset + "," + limits +" ) t)"
    client.select[User](query) { row =>
      val userId = row("userId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val alias = row("alias").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val name = row("name").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val password = row("password").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val imgUrl = row("imgUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(sex) = row("sex").getOrElse("1")
      val phoneNo = row("phoneNo").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val mSchool = row("mSchool").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val collegeId = row("collegeId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val collegeName = row("collegeName").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val depart = row("depart").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val cla = row("cla").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val majors = row("majors").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val dorm = row("dorm").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val degree = row("degree").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val freshDate = row("freshDate").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(birthDate) = row("birthDate").getOrElse("0000-00-00 00:00:00")
      val home = row("home").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val loveSt = row("loveSt").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val sign = row("sign").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val interests = row("interests").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val actives = row("actives").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
      val tokenS = row("tokenS").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val deviceId = row("deviceId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(isAnonymousUser) = row("isAnonymousUser").getOrElse("")
      val IntValue(status) = row("status").getOrElse("")
      val backgroundImg = row("backgroundImg").map {
        case StringValue(str) => str
        case _ => ""
      }.get
       val descr = row("descr").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(role) = row("role").getOrElse(1)
      User(UUID.fromString(userId), alias, name, password, imgUrl, sex, phoneNo, mSchool,collegeId,
        collegeName, depart, cla, majors, dorm, degree, freshDate, new DateTime(birthDate), home, loveSt,
        sign, interests, actives, new DateTime(cTime), tokenS, deviceId,
         isAnonymousUser,status,backgroundImg,descr,role)
      }

  }
}


