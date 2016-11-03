package com.wisedu.next.services

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.security.MessageDigest
import java.util.{Date, Random, UUID}

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

object BaseFunctions {

  def isUUID(str: String): Boolean = {
    try {
      UUID.fromString(str)
      true
    } catch {
      case _: Exception => false
    }
  }

  def getRandomString(len: Int, name: String): String = {
    val random = new Random()
    var idx = 0
    val str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    var rst = new Date().getTime.toString
    while (idx < len) {
      val num = random.nextInt(62)
      rst = rst + str(num)
      idx = idx + 1
    }
    if (name.contains(".")) {
      val suffix = name.substring(name.lastIndexOf("."))
      rst + suffix
    }
    else {
      rst
    }
  }

  def getRandomStringWithWH(len: Int, name: String, width: Int, height: Int): String = {
    val random = new Random()
    var idx = 0
    val str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    var rst = new Date().getTime.toString
    while (idx < len) {
      val num = random.nextInt(62)
      rst = rst + str(num)
      idx = idx + 1
    }
    if (name.contains(".")) {
      val suffix = name.substring(name.lastIndexOf("."))
      rst + "_" + width + "_" + height + suffix
    }
    else {
      rst + "_" + width + "_" + height
    }
  }

  def getRandomString(len: Int): String = {
    val random = new Random()
    var idx = 0
    val str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    var rst = ""
    while (idx < len) {
      val num = random.nextInt(62)
      rst = rst + str(num)
      idx = idx + 1
    }
    rst
  }

  def getIdFromName(name: String): String = {
    if (name.contains(".")) {
      val idx = name.lastIndexOf(".")
      val suffix = name.substring(idx)
      val n = name.substring(0, idx)
      val nId = UUID.nameUUIDFromBytes(n.getBytes("UTF-8")).toString + suffix
      nId
    } else {
      val nId = UUID.nameUUIDFromBytes(name.getBytes("UTF-8")).toString
      nId
    }

  }

  def object2ChannelBuffer(obj: Any): ChannelBuffer = {
    val bufout = new ByteArrayOutputStream()
    val obout = new ObjectOutputStream(bufout)
    obout.writeObject(obj)
    val channelBuffer = ChannelBuffers.copiedBuffer(bufout.toByteArray)
    obout.close()
    bufout.close()
    channelBuffer
  }

  def channelBuffer2Object[T](channelBuffer: ChannelBuffer): T = {
    val bufout = new ByteArrayOutputStream()
    val obout = new ObjectOutputStream(bufout)
    val bufin = new ByteArrayInputStream(channelBuffer.array())
    val obin = new ObjectInputStream(bufin)
    val item = obin.readObject().asInstanceOf[T]
    obin.close()
    bufin.close()
    obout.close()
    bufout.close()
    item
  }

  def byte2hex(bytes: Array[Byte]): String = {
    val sign = new StringBuilder()
    for (byte <- bytes) {
      val hex = Integer.toHexString(byte & 0xFF)
      if (hex.length() == 1) {
        sign.append("0")
      }
      sign.append(hex.toUpperCase)
    }
    sign.toString()
  }

  //MD5加密
  def md5Encryption(text: String, secret: String): String = {
    val md = MessageDigest.getInstance("MD5")
    val query = new StringBuilder()
    query.append(text).append(secret)
    BaseFunctions.byte2hex(md.digest(query.toString().getBytes("utf-8")))
  }

}