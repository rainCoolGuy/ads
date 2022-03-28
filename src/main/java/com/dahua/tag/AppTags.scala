package com.dahua.tag

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object AppTags extends TagTrait {
	override def makeTags(args: Any*): Map[String, Int] = {
		// 设定返回值类型
		var map = Map[String, Int]()
		val row: Row = args(0).asInstanceOf[Row]
		// 广播变量接收
		val broadcast: Map[String, String] = args(1).asInstanceOf[Map[String, String]]

		val appName: String = row.getAs[String]("appname")
		val appId: String = row.getAs[String]("appid")

		if (StringUtils.isEmpty(appName)) {
			broadcast.contains(appId) match {
				case true => map += "APP" + appName
				case false =>
				case _ =>
			}
		}
		map
	}
}
