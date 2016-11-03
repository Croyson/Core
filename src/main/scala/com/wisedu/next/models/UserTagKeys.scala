package com.wisedu.next.models

import com.twitter.finagle.exp.mysql._
import com.twitter.util.Future
import com.wisedu.next.types.NextTypes.MysqlClient

case class UserTagKey(userKey: String, keyType: Int, tagKey: String)

abstract class ConcreteUserTagKeys(val client: MysqlClient) {

  def getTagIdById(userKey: String, keyType: Int): Future[Option[UserTagKey]] = {
    val query = "select * from userTagKeys where userKey = ?  and keyType = ? limit 1 "
    client.prepare(query).select[UserTagKey](userKey,keyType) { row =>
      val userKey = row("userKey").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(keyType) = row("keyType").getOrElse("")
      val tagKey = row("tagKey").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      UserTagKey(userKey,keyType, tagKey)
    }.map(_.headOption)
  }


}