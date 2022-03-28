package com.dahua.tag

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object AppTags extends TagTrait {
	override def makeTags(args: Any*): Map[String, Int] = {
		// TODO 设定返回值类型
		var map:Map[String, Int] = Map[String, Int]()
		val row: Row = args(0).asInstanceOf[Row]
		// 广播变量接收
		val broadcast: Map[String, String] = args(1).asInstanceOf[Map[String, String]]

		val appName: String = row.getAs[String]("appname")
		val appId: String = row.getAs[String]("appid")

		// 渠道标签
		val adPlaFormProviDerid: Int = row.getAs[Int]("adplatformproviderid")

		if (StringUtils.isEmpty(appName)) {
			if (broadcast.contains(appId)) {
				map += "APP" + broadcast.getOrElse(appId, "未知") -> 1
			}
		}

		map
	}
}
