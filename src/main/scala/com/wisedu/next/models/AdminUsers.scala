package com.wisedu.next.models

import com.twitter.util.Future
import com.websudos.phantom.CassandraTable
import com.websudos.phantom.dsl._

case class AdminUser(userId: String, userName: String, password: String, cTime: DateTime)

class AdminUsers extends CassandraTable[AdminUsers, AdminUser] {
  object userId extends StringColumn(this) with PrimaryKey[String]
  object userName extends StringColumn(this)
  object password extends StringColumn(this)
  object cTime extends DateTimeColumn(this)

  def fromRow(row: Row): AdminUser = {
    AdminUser(
      userId(row), userName(row), password(row), cTime(row)
    )
  }
}

abstract class ConcreteAdminUsers extends AdminUsers with RootConnector {

  def getById(userId: String): Future[Option[AdminUser]] = {
    select.where(_.userId eqs userId).get()
  }

  def insAdminUser(adminUser: AdminUser): Future[ResultSet] = {
    insert.value(_.userId, adminUser.userId)
      .value(_.userName, adminUser.userName)
      .value(_.password, adminUser.password)
      .value(_.cTime, adminUser.cTime)
      .execute()
  }

  def delAdminUser(userId: String): Future[ResultSet] = {
    delete.where(_.userId eqs userId).execute()
  }

}

case class AdminToken(tokens: String, userId: String, cTime: DateTime, exTime: DateTime)

class AdminTokens extends CassandraTable[AdminTokens, AdminToken] {
  object tokens extends StringColumn(this) with PrimaryKey[String]
  object userId extends StringColumn(this) with Index[String]
  object cTime extends DateTimeColumn(this)
  object exTime extends DateTimeColumn(this)

  def fromRow(row: Row): AdminToken = {
    AdminToken(
      tokens(row), userId(row), cTime(row), exTime(row)
    )
  }
}

abstract class ConcreteAdminTokens extends AdminTokens with RootConnector {

  def getByTokens(tokens: String): Future[Option[AdminToken]] = {
    select.where(_.tokens eqs tokens).get()
  }

  def getByUserId(userId: String): Future[Option[AdminToken]] = {
    select.where(_.userId eqs userId).get()
  }

  def insAdminAuth(adminToken: AdminToken): Future[ResultSet] = {
    insert.value(_.userId, adminToken.userId)
      .value(_.tokens, adminToken.tokens)
      .value(_.cTime, adminToken.cTime)
      .value(_.exTime, adminToken.exTime)
      .execute()
  }

  def delAdminAuth(userId: String): Future[ResultSet] = {
    delete.where(_.userId eqs userId).execute()
  }

}
