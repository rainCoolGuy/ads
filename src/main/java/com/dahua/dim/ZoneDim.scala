package com.dahua.dim

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ZoneDim {

	def main(args: Array[String]): Unit = {

		if (args.length > 2) {
			println(
				"""
				  |缺少参数
				  |inputPath outputPath
				  |""".stripMargin)
			sys.exit(0)
		}

		// 创建 SparkSession对象
		val conf: SparkConf = new SparkConf()
		  .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

		val spark: SparkSession = SparkSession
		  .builder
		  .config(conf)
		  .appName("Log2Parquet")
		  .master("local[1]")
		  .getOrCreate


		val Array(inputPath, outputPath) = args

		val df: DataFrame = spark.read.parquet(inputPath)

		df.createTempView("dim")

		val sql: String =
			"""
			  |select
			  |		provincename,
			  |  	cityname,
			  |   	sum(case when requestmode = 1 and processnode >= 1 then 1 else 0 end) as ysqq,
			  |from dim
			  |group by
			  |provincename,
			  |cityname
			  |""".stripMargin

		spark.sql(sql).show()

		spark.stop()
	}
}
