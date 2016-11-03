package com.wisedu.next.services

import java.awt.image.BufferedImage
import java.net.URL
import java.util.UUID
import javax.imageio.ImageIO
import javax.inject.{Inject, Singleton}

import com.jhlabs.image.{GaussianFilter, ScaleFilter}
import com.twitter.finatra.annotations.Flag
import com.twitter.finatra.utils.FuturePools
import com.twitter.util.Future
import org.apache.commons.io.FileUtils
import org.joda.time.DateTime

import scala.reflect.io.File

@Singleton
class StaticBaseService {

  @Inject
  @Flag("local.doc.root") var filePath: String = _
  val vFilePath = "/v2/statics/asserts"
  private val futurePool = FuturePools.unboundedPool("CallbackConverter")

  def putBucket(bucketId: String): Future[Boolean] = {
    futurePool {
      val realFilePath = filePath + "/asserts"
      File(realFilePath + "/" + bucketId).createDirectory()
      true
    }
  }

  def delBucket(bucketId: String): Future[Boolean] = {
    futurePool {
      val realFilePath = filePath + "/asserts"
      File(realFilePath + "/" + bucketId).deleteRecursively()
    }
  }

  def putObject(bucketId: String, objectId: String, cont: Array[Byte]): Future[String] = {
    futurePool {
      val realFilePath = filePath + "/asserts"

      File(realFilePath + "/" + bucketId + "/" + objectId).createFile()
      val output = File(realFilePath + "/" + bucketId + "/" + objectId).outputStream()
      output.write(cont)
      output.close()
      vFilePath + "/" + bucketId + "/" + objectId
    }
  }

  def delObject(bucketId: String, objectId: String): Future[Boolean] = {
    futurePool {
      val realFilePath = filePath + "/asserts"
      File(realFilePath + "/" + bucketId + "/" + objectId).delete()
    }
  }

  def getObject(bucketId: String, objectId: String): Future[java.io.InputStream] = {
    futurePool {
      val realFilePath = filePath + "/asserts"
      File(realFilePath + "/" + bucketId + "/" + objectId).inputStream()
    }
  }

  def getObjectVUrl(bucketId: String, objectId: String): String = {
    vFilePath + "/" + bucketId + "/" + objectId
  }

  def fuzzyUpdateImgs(imgs: String): Future[Seq[String]] = {
    Future.collect(imgs.split(",").map {
      imgUrl => {
        futurePool {
          if (imgUrl.indexOf("http") > -1) {
            val httpUrl = new URL(imgUrl)
            val realFilePath = filePath + "/asserts"
            val imgPath = DateTime.now.toString("yyyyMMdd") + "/" + UUID.randomUUID().toString + ".jpeg"
            val file = new java.io.File(realFilePath + "/" + imgPath)
            FileUtils.copyURLToFile(httpUrl, file)
            val img = ImageIO.read(file)
            val gf = new GaussianFilter()
            gf.setRadius(150)
            gf.setUseAlpha(true)
            gf.setPremultiplyAlpha(true)
            ImageIO.write(gf.filter(img, null), "jpeg", file)
            vFilePath + "/" + imgPath
          } else {
            val imgPath = filePath + "/asserts/" + imgUrl.replace(vFilePath, "")
            val img = ImageIO.read(new java.io.File(imgPath))
            val gf = new GaussianFilter()
            val newId = UUID.randomUUID().toString
            gf.setRadius(150)
            gf.setUseAlpha(true)
            gf.setPremultiplyAlpha(true)
            var imgType = imgPath.substring(if (imgPath.lastIndexOf(".") > -1) imgPath.lastIndexOf(".") + 1 else imgPath.length, imgPath.length)
            imgType = if (imgType.isEmpty) "jpeg" else imgType
            val fuzzyImgUrl = imgPath.substring(0, imgPath.lastIndexOf("/")) + newId + ".jpeg"
            ImageIO.write(gf.filter(img, null), imgType, new java.io.File(fuzzyImgUrl))
            imgUrl.substring(0, imgUrl.lastIndexOf("/")) + newId + ".jpeg"
          }
        }
      }
    })
  }

  //图片压缩
  def compressionUpdateImgs(imgs: String, width: Int = 150, height: Int = 150, sizeName: String): Future[Seq[String]] = {
    Future.collect(imgs.split(",").map {
      imgUrl => {
        futurePool {
          if (imgUrl.indexOf("http") > -1) {
            val httpUrl = new URL(imgUrl)
            val realFilePath = filePath + "/asserts"
            var imgType = imgUrl.substring(if (imgUrl.lastIndexOf(".") > -1) imgUrl.lastIndexOf(".") + 1 else imgUrl.length, imgUrl.length)
            imgType = if (imgType.isEmpty) "jpg" else imgType
            val imgPath = DateTime.now.toString("yyyyMMdd") + "/" + UUID.randomUUID().toString + "_" + sizeName + "." + imgType
            val file = new java.io.File(realFilePath + "/" + imgPath)
            FileUtils.copyURLToFile(httpUrl, file)
            val img = ImageIO.read(new java.io.File(imgPath))
            val sf = getScaleFilter(img, width, height)
            ImageIO.write(sf.filter(img, null), imgType, file)
            vFilePath + "/" + imgPath

          } else {
            val imgPath = filePath + "/asserts/" + imgUrl.replace(vFilePath, "")
            val file = new java.io.File(imgPath)
            val img = ImageIO.read(file)
            val sf = getScaleFilter(img, width, height)
            val imgType = imgPath.substring(if (imgPath.lastIndexOf(".") > -1) imgPath.lastIndexOf(".")  else imgPath.length, imgPath.length)
            val mImgUrl = imgPath.substring(0, if (imgPath.lastIndexOf(".") > -1) imgPath.lastIndexOf(".") else imgPath.length) + "_" + sizeName  + imgType
            ImageIO.write(sf.filter(img, null),if(imgType.nonEmpty) imgType.substring(1) else getImgFormatType(file), new java.io.File(mImgUrl))
            imgUrl.substring(0, if (imgUrl.lastIndexOf(".") > -1) imgUrl.lastIndexOf(".") else imgUrl.length) + "_" + sizeName  + imgType
          }
        }
      }
    })
  }


  def getImgFormatType(o: Object): String = {
    val iis = ImageIO.createImageInputStream(o)
    val imgReaders = ImageIO.getImageReaders(iis)
    iis.close()
    if (!imgReaders.hasNext) {
      ""
    } else {
      val reader = imgReaders.next()
      reader.getFormatName
    }
  }

  def getScaleFilter(img: BufferedImage, width: Int, height: Int): ScaleFilter = {
    var newWidth = 200
    var newHeight = 200
    if (img == null || width == 0 || height == 0) {
      new ScaleFilter(newWidth, newHeight)
    } else {
      if (img.getWidth / img.getHeight >= width / height) {
        if (img.getWidth > width) {
          newWidth = width
          newHeight = (img.getHeight * width) / img.getWidth
        } else {
          newWidth = img.getWidth
          newHeight = img.getHeight
        }
      } else {
        if (img.getHeight > height) {
          newHeight = height
          newWidth = (img.getWidth * height) / img.getHeight
        } else {
          newWidth = img.getWidth
          newHeight = img.getHeight
        }
      }
      new ScaleFilter(newWidth, newHeight)
    }
  }

  def fetchUrl(url: String, bucketId: String, objectId: String): Future[String] = {
    futurePool {
      val httpUrl = new URL(url)
      val realFilePath = filePath + "/asserts"
      val file = new java.io.File(realFilePath + "/" + bucketId + "/" + objectId)
      FileUtils.copyURLToFile(httpUrl, file)
      vFilePath + "/" + bucketId + "/" + objectId
    }
  }

}