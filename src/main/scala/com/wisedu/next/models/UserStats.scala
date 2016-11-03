package com.wisedu.next.models

import _root_.java.lang.{Long => JLong}
import java.util.{TimeZone, UUID}

import com.twitter.finagle.exp.mysql.TimestampValue
import com.twitter.finagle.redis.Client
import com.twitter.finagle.redis.util.{CBToString, StringToChannelBuffer}
import com.twitter.util.Future
import com.websudos.phantom.CassandraTable
import com.websudos.phantom.dsl._
import com.wisedu.next.consts.UserKeys
import com.wisedu.next.services.BaseFunctions
import com.wisedu.next.types.NextTypes._
import org.jboss.netty.buffer.ChannelBuffer
import org.joda.time.DateTime

case class UserStat(userId: UUID, fansCount: Long, followCount: Long, msgCount: Long)


class UserStats extends CassandraTable[UserStats, UserStat] {

  object userId extends UUIDColumn(this) with PrimaryKey[UUID]

  object fansCount extends CounterColumn(this)

  object followCount extends CounterColumn(this)

  object msgCount extends CounterColumn(this)

  def fromRow(row: Row): UserStat = {
    UserStat(
      userId(row), fansCount(row), followCount(row), msgCount(row)
    )
  }

}

abstract class ConcreteUserStats(val client: MysqlClient, val redisClient: Client) extends UserStats with RootConnector {
  val timeValue = new TimestampValue(TimeZone.getDefault, TimeZone.getDefault)

  implicit def cb2s(cb: ChannelBuffer): String = CBToString(cb)

  def getUserMsgCount(userId: UUID): Future[Long] = {
    val key = UserKeys.userMsgCount.format(userId.toString)
    redisClient.get(StringToChannelBuffer(key)).map {
      case Some(result) => cb2s(result).toLong
      case None => 0L
    }
  }

  def incUserMsgCount(userId: UUID): Future[Long] = {
    val key = UserKeys.userMsgCount.format(userId.toString)
    redisClient.incr(StringToChannelBuffer(key)).map {
      rst => rst
    }
  }

  def resetUserMsgCount(userId: UUID): Future[Unit] = {
    val key = UserKeys.userMsgCount.format(userId.toString)
    redisClient.set(StringToChannelBuffer(key), StringToChannelBuffer("0"))
  }


  def resetUserMsgListLastTime(userId: UUID): Future[Unit] = {
    val key = UserKeys.userMsgListLastTime.format(userId.toString)
    redisClient.set(StringToChannelBuffer(key), BaseFunctions.object2ChannelBuffer(DateTime.now()))
  }


  def getUserMsgListLastTime(userId: UUID): Future[Option[DateTime]] = {
    val key = UserKeys.userMsgListLastTime.format(userId.toString)
    redisClient.get(StringToChannelBuffer(key)).map {
      result => result.map(item => BaseFunctions.channelBuffer2Object[DateTime](item))
    }
  }


  def getById(userId: UUID): Future[Option[UserStat]] = {
    select.where(_.userId eqs userId).get()
  }

  def insUserStat(userStat: UserStat): Future[ResultSet] = {
    update.where(_.userId eqs userStat.userId)
      .modify(_.fansCount increment userStat.fansCount)
      .and(_.followCount increment userStat.followCount)
      .and(_.msgCount increment userStat.msgCount)
      .execute()
  }

  def incUserFansCount(userId: UUID): Future[ResultSet] = {
    update.where(_.userId eqs userId).modify(_.fansCount increment 1).execute()
  }

  def decUserFansCount(userId: UUID): Future[ResultSet] = {
    update.where(_.userId eqs userId).modify(_.fansCount increment -1).execute()
  }

  def incFollowCount(userId: UUID): Future[ResultSet] = {
    update.where(_.userId eqs userId).modify(_.followCount increment 1).execute()
  }

  def decFollowCount(userId: UUID): Future[ResultSet] = {
    update.where(_.userId eqs userId).modify(_.followCount increment -1).execute()
  }

  def incMsgCount(userId: UUID): Future[ResultSet] = {
    update.where(_.userId eqs userId).modify(_.msgCount increment 1).execute()
  }

  def decMsgCount(userId: UUID): Future[ResultSet] = {
    update.where(_.userId eqs userId).modify(_.msgCount increment -1).execute()
  }

}


