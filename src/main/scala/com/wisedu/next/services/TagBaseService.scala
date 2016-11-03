package com.wisedu.next.services

import javax.inject.{Inject, Singleton}

import com.twitter.finagle.exp.mysql.Result
import com.twitter.util.Future
import com.wisedu.next.annotations.NewSqlDatabase
import com.wisedu.next.models.{TagStruct, AppDatabase, Tag}

/**
 * Version: 1.1
 * Author: pattywgm
 * Time: 16/5/20 下午2:25
 * Desc:
 */

@Singleton
class TagBaseService {

  @Inject
  @NewSqlDatabase var appDatabase: AppDatabase = _

  def getTagByName(tagName: String): Future[Option[Boolean]] = {
    appDatabase.tags.getByName(tagName)
  }

  def getTagById(tagId: String): Future[Option[Tag]] = {
    appDatabase.tags.getById(tagId)
  }

  def delById(tagId: String): Future[Result] = {
    appDatabase.tags.delById(tagId)
  }

  def insTag(tag: Tag): Future[Result] = {
    appDatabase.tags.insTag(tag)
  }

  def updTag(tag: Tag): Future[Result] = {
    appDatabase.tags.updTag(tag)
  }

  def collTags(tagDomain: Int, limit: Int, offset: Int): Future[Seq[Tag]] = {
    appDatabase.tags.collTags(tagDomain, limit, offset)
  }

  def collTagSize(tagDomain: Int): Future[Int] = {
    appDatabase.tags.collTagSize(tagDomain)
  }

  def collTagPageList(tagDomain: Int, limit: Int, offset: Int): Future[(Seq[Tag], Int)] = {
    val tagsF = appDatabase.tags.collTags(tagDomain, limit, offset)
    val tagsSizeF = appDatabase.tags.collTagSize(tagDomain)

    for {
      tags <- tagsF
      tagSize <- tagsSizeF
    } yield (tags, tagSize)
  }

  def collDomainEnabledTag(tagDomain: Int): Future[Seq[Tag]] = {
    appDatabase.tags.collDomainEnabledTag(tagDomain)
  }

  def getTagStructById(tagId: String): Future[Option[TagStruct]] = {
    appDatabase.tagStructs.getById(tagId)
  }

  def delTagStructById(tagId: String): Future[Result] = {
    appDatabase.tagStructs.delById(tagId)
  }

  def insTagStruct(tagStruct: TagStruct): Future[Result] = {
    appDatabase.tagStructs.insTagStruct(tagStruct)
  }



}
