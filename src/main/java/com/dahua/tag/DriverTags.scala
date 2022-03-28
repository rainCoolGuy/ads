package com.dahua.tag

import org.apache.spark.sql.Row

object DriverTags extends TagTrait {
	override def makeTags(args: Any*): Map[String, Int] = {

		var map: Map[String, Int] = Map[String, Int]()
		val row: Row = args(0).asInstanceOf[Row]
		val client: Int = row.getAs[Int]("client")

		client match {
			case 1 => map += "D00010001" -> 1
			case 2 => map += "D00010002" -> 1
			case 3 => map += "D00010003" -> 1
			case _ => map += "D00010004" -> 1
		}

		map
	}
}
