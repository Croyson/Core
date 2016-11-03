package com.wisedu.next.modules

import javax.inject.Singleton

import com.google.inject.Provides
import com.twitter.finagle.exp.Mysql
import com.twitter.finagle.redis.Client
import com.twitter.inject.TwitterModule
import com.websudos.phantom.connectors.ContactPoints
import com.wisedu.next.annotations.NewSqlDatabase
import com.wisedu.next.models.AppDatabase

object DataBaseModule extends TwitterModule {

  val dscHostsFlag = flag("dsc.hosts", "localhost", "cassandra 's hosts")
  val dscKeySpaceFlag = flag("dsc.keyspace", "cpdailyspace", "cassandra 's keyspace")
  val dscPortFlag = flag("dsc.port", "9042", "cassandra 's port")

  val mysqlHostFlag = flag("mysql.host", "localhost", "mysql server address host")
  val mysqlPortFlag = flag("mysql.port", "3306", "mysql server address port")
  val mysqlUsernameFlag = flag("mysql.username", "root", "mysql username")
  val mysqlPasswordFlag = flag("mysql.password", "123456", "mysql password")
  val mysqlDbNameFlag = flag("mysql.database", "cpdailyspace", "default database to connect to")
  val liveGroupFlag = flag("live.group", "zbpd", "default live group")
  val redisHostFlag = flag("redis.host", "172.16.2.131", "redis server address host")
  val redisPortFlag = flag("redis.port", "6379", "redis server address port")

  @Singleton
  @Provides
  @NewSqlDatabase
  def providesAppDatabase: AppDatabase = {
    val connector = ContactPoints(dscHostsFlag().split(",").toSeq, dscPortFlag().toInt).keySpace(dscKeySpaceFlag())
    val client = Mysql.client
      .withCredentials(mysqlUsernameFlag(), mysqlPasswordFlag())
      .withDatabase(mysqlDbNameFlag())
      .newRichClient("%s:%s".format(mysqlHostFlag(), mysqlPortFlag()))
    val redisClient = Client("%s:%s".format(redisHostFlag(), redisPortFlag()))
    redisClient.select(9)
    new AppDatabase(connector, client,redisClient)
  }

}