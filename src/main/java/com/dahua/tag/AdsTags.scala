package com.dahua.tag

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/*
* 广告位类型标签
* */
object AdsTags extends TagTrait {
	override def makeTags(args: Any*): Map[String, Int] = {
		var map: Map[String, Int] = Map[String, Int]()
		val row: Row = args(0).asInstanceOf[Row]

		// 广告位类型
		val adsPaceType: Int = row.getAs[Int]("asdpacetype")

		if (adsPaceType > 9) {
			map += "LC" + adsPaceType -> 1
		} else {
			map += "LC0" + adsPaceType -> 1
		}

		// 广告位名称
		val adsPaceTypeName: String = row.getAs[String]("adspacetypename")
		if (StringUtils.isNotEmpty(adsPaceTypeName)) {
			map += "LN" + adsPaceTypeName -> 1
		}


		map
	}
}
