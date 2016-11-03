package com.wisedu.next.services

import java.util.UUID
import javax.inject.{Inject, Singleton}

import com.twitter.util.Future
import com.wisedu.next.annotations.NewSqlDatabase
import com.wisedu.next.models._
import org.joda.time.DateTime

@Singleton
class AdminUserBaseService {

  @Inject @NewSqlDatabase var appDatabase: AppDatabase = _
  def verifyLoginInfo(userId:String,password:String): Future[Boolean] ={
    appDatabase.adminUsers.getById(userId).map{
      case Some(item) => if(item.password.equals(password)){
        true
      }else{
        false
      }
      case None => false
    }
  }

  def getToken(userId:String):Future[String]={
    appDatabase.adminTokens.getByUserId(userId).map{
      case Some(token) => token.tokens
      case None => UUID.randomUUID().toString
    }.map{
      token => {
        appDatabase.adminTokens.insAdminAuth(AdminToken(token, userId, DateTime.now, DateTime.now.plusHours(4)))
        token
      }
    }
  }

  def getByTokens(tokens:String):Future[Option[AdminToken]]={
    appDatabase.adminTokens.getByTokens(tokens)
  }

}