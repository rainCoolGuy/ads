package com.dahua.utils

object NumFormat {

	def toInt(field: String): Int = {
		try {
			field.toInt
		} catch  {
			case _: Exception => 0
		}
	}

	def toDouble(field: String): Double = {
		try {
			field.toDouble
		} catch  {
			case _: Exception => 0D
		}
	}
}
