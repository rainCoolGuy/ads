package com.dahua.tag

import com.dahua.tools.SNTools
import org.apache.spark.sql.Row

object BusinessDistrictTag extends TagTrait {
	override def makeTags(args: Any*): Map[String, Int] = {
		var map: Map[String, Int] = Map[String, Int]()
		val row: Row = args(0).asInstanceOf[Row]

		val longitude: String = row.getAs[String]("long")
		val dimension: String = row.getAs[String]("lat")


		val bs: String = SNTools.getBusiness(s"${dimension},${longitude}")

		if (bs != "") {
			println(s"商圈: ${bs}")
			println(s"经度: ${longitude} 维度: ${dimension}")
			map += "SQ" + bs -> 1
		}
		map
	}
}
