package com.wisedu.next.models

import com.twitter.util.Future
import com.websudos.phantom.dsl._
import com.websudos.phantom.CassandraTable

case class AuthCode(phoneNo: String, authCode: String, cTime: DateTime, exTime: DateTime)

class AuthCodes extends CassandraTable[AuthCodes, AuthCode] {
  object phoneNo extends StringColumn(this) with PrimaryKey[String]
  object authCode extends StringColumn(this)
  object cTime extends DateTimeColumn(this)
  object exTime extends DateTimeColumn(this)

  def fromRow(row: Row): AuthCode = {
    AuthCode(
      phoneNo(row), authCode(row), cTime(row), exTime(row)
    )
  }
}

abstract class ConcreteAuthCodes extends AuthCodes with RootConnector {

  def getById(phoneNo: String): Future[Option[AuthCode]] = {
    select.where(_.phoneNo eqs phoneNo).get()
  }

  def insAuthCode(authCode: AuthCode): Future[ResultSet] = {
    insert.value(_.phoneNo, authCode.phoneNo)
      .value(_.authCode, authCode.authCode)
      .value(_.cTime, authCode.cTime)
      .value(_.exTime, authCode.exTime)
      .execute()
  }

  def delAuthCode(phoneNum: String): Future[ResultSet] = {
    delete.where(_.phoneNo eqs phoneNum).execute()
  }

}
