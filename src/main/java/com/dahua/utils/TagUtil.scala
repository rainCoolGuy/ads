package com.dahua.utils

import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

object TagUtil {


	val tagUserIdFilterParam: String =
		"""
		  |imei != "" or imeimd5 != "" or imeisha1 != "" or
		  |idfa != "" or idfamd5 != "" or idfasha1 != "" or
		  |mac != "" or macmd5 != "" or macsha1 != "" or
		  |androidid != "" or androididmd5 != "" or androididsha1 != "" or
		  |openudid != "" or openudidmd5 != "" or openudidsha1 != ""
   """.stripMargin

	def getUserId(v: Row): ListBuffer[String] = {

		val userId: ListBuffer[String] = ListBuffer[String]()
		if(v.getAs[String]("imei").nonEmpty){userId.append("IM:"+v.getAs[String]("imei").toUpperCase)}
		if(v.getAs[String]("imeimd5").nonEmpty){userId.append("IMD:"+v.getAs[String]("imeimd5").toUpperCase)}
		if(v.getAs[String]("imeisha1").nonEmpty){userId.append("IMS:"+v.getAs[String]("imeisha1").toUpperCase)}
		if(v.getAs[String]("idfa").nonEmpty){userId.append("ID:"+v.getAs[String]("idfa").toUpperCase)}
		if(v.getAs[String]("idfamd5").nonEmpty){userId.append("IDM:"+v.getAs[String]("idfamd5").toUpperCase)}
		if(v.getAs[String]("idfasha1").nonEmpty){userId.append("IDS:"+v.getAs[String]("idfasha1").toUpperCase)}
		if(v.getAs[String]("mac").nonEmpty){userId.append("MAC:"+v.getAs[String]("mac").toUpperCase)}
		if(v.getAs[String]("macmd5").nonEmpty){userId.append("MACM:"+v.getAs[String]("macmd5").toUpperCase)}
		if(v.getAs[String]("macsha1").nonEmpty){userId.append("MACS:"+v.getAs[String]("macsha1").toUpperCase)}
		if(v.getAs[String]("androidid").nonEmpty){userId.append("AD:"+v.getAs[String]("androidid").toUpperCase)}
		if(v.getAs[String]("androididmd5").nonEmpty){userId.append("ADM:"+v.getAs[String]("androididmd5").toUpperCase)}
		if(v.getAs[String]("androididsha1").nonEmpty){userId.append("IMS:"+v.getAs[String]("androididsha1").toUpperCase)}
		if(v.getAs[String]("openudid").nonEmpty){userId.append("OP:"+v.getAs[String]("openudid").toUpperCase)}
		if(v.getAs[String]("openudidmd5").nonEmpty){userId.append("OPM:"+v.getAs[String]("openudidmd5").toUpperCase)}
		if(v.getAs[String]("openudidsha1").nonEmpty){userId.append("OPS:"+v.getAs[String]("openudidsha1").toUpperCase)}
		userId
	}

}
