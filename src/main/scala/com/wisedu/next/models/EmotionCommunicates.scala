package com.wisedu.next.models

import java.util.{UUID, TimeZone}

import com.twitter.finagle.exp.mysql._
import com.twitter.util.Future
import com.wisedu.next.types.NextTypes.MysqlClient
import org.joda.time.DateTime

case class EmotionCommunicate(emotionId: String, feedId: String, updateId: String, modelId: String, userId: String,
                              selectId: String, cTime: DateTime)

case class EmotionTypeStats(modelId: String, selectId: String, count: Int)

abstract class ConcreteEmotionCommunicates(val client: MysqlClient) {
  val timeValue = new TimestampValue(TimeZone.getDefault, TimeZone.getDefault)

  def getByFeedId(feedId: String): Future[Seq[EmotionCommunicate]] = {
    val sql = "select * from emotionCommunicates where feedId = ? order by cTime desc "
    client.prepare(sql).select[EmotionCommunicate](feedId) { row =>
     val emotionId = row("emotionId").map {
        case StringValue(str) => str
        case _ => ""
      }.get

      val feedId = row("feedId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
       val updateId = row("updateId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
     val  modelId = row("modelId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val  userId = row("userId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val  selectId = row("selectId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
      EmotionCommunicate(emotionId, feedId, updateId, modelId,userId,
                              selectId,new DateTime(cTime))
      }
  }

  def getByUpdateId(updateId: String): Future[Seq[EmotionCommunicate]] = {
    val sql = "select * from emotionCommunicates where updateId = ? order by cTime desc "
    client.prepare(sql).select[EmotionCommunicate](updateId) { row =>
     val emotionId = row("emotionId").map {
        case StringValue(str) => str
        case _ => ""
      }.get

      val feedId = row("feedId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
       val updateId = row("updateId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
     val  modelId = row("modelId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val  userId = row("userId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val  selectId = row("selectId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
      EmotionCommunicate(emotionId, feedId, updateId, modelId,userId,
                              selectId,new DateTime(cTime))
      }
  }

  def getByUpdateIdFeedIdUserId(updateId: String, feedId: String, userId: String): Future[Option[EmotionCommunicate]] = {
    val sql = "select * from emotionCommunicates where updateId = ? and feedId = ? and userId = ?"
    client.prepare(sql).select[EmotionCommunicate](updateId, feedId, userId) { row =>
      val emotionId = row("emotionId").map {
        case StringValue(str) => str
        case _ => ""
      }.get

      val feedId = row("feedId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val updateId = row("updateId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val  modelId = row("modelId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val  userId = row("userId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val  selectId = row("selectId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
      EmotionCommunicate(emotionId, feedId, updateId, modelId,userId,
        selectId,new DateTime(cTime))
    }.map(_.headOption)
  }

  def insEmotionCommunicate(emotionCommunicate: EmotionCommunicate): Future[Result] = {
    val sql =
      """INSERT INTO emotionCommunicates(emotionId, feedId, updateId, modelId, userId,
                                      selectId,cTime) VALUES (?, ?, ?, ?,?, ?, ?)"""
    val ps = client.prepare(sql)
    ps(emotionCommunicate.emotionId, emotionCommunicate.feedId, emotionCommunicate.updateId, emotionCommunicate.modelId
      , emotionCommunicate.userId, emotionCommunicate.selectId, emotionCommunicate.cTime.toDate)
  }

  def collEmotionSize(id: String, method: String, model_id: String): Future[Seq[EmotionTypeStats]] = {
    var query = "select distinct modelId, selectId, count(*) as value from emotionCommunicates where modelId= '"+ model_id + "'"
    if (method == "0")
      query += "and feedId = '"+ id +"'"
    if(method == "1")
      query += "and updateId = '" + id +"'"
    query += " GROUP BY modelId, selectId"

    client.select[EmotionTypeStats](query) { row =>
      val LongValue(value) = row("value").getOrElse("0")
      val  modelId = row("modelId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val  selectId = row("selectId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      EmotionTypeStats(modelId, selectId, value.toInt)
    }
  }

  def collectUserById(modelId: String, selectId: String, method: String, id: String,limits: Int, offset: Int): Future[Seq[User]] = {
    var query = "select * from users where userId in (select userId  from (select userId from emotionCommunicates f where modelId = '"+modelId+"' and selectId = '"+ selectId+"'"
    if (method == "0")
      query += " and feedId = '"+ id +"'"
    if (method == "1")
      query += " and updateId = '"+ id +"'"
    query +="  order by ctime desc limit " + offset + "," + limits +")t)"


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


  def getByContentUserId(id: String,userId:String,opType:Int): Future[Option[EmotionCommunicate]] = {
    var sql = "select * from emotionCommunicates where userId =  '" + userId + "' "
    if(opType == 0){
      sql += " and feedId = '" + id +"'"
    }else{
      sql += " and updateId = '" + id +"'"
    }
     sql += " limit 1 "
    client.select[EmotionCommunicate](sql) { row =>
     val emotionId = row("emotionId").map {
        case StringValue(str) => str
        case _ => ""
      }.get

      val feedId = row("feedId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
       val updateId = row("updateId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
     val  modelId = row("modelId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val  userId = row("userId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val  selectId = row("selectId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
      EmotionCommunicate(emotionId, feedId, updateId, modelId,userId,
                              selectId,new DateTime(cTime))
      }
  }.map(_.headOption)



}