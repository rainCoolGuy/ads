package com.dahua.analyse

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object ProCityAnalyseSQL {

	def main(args: Array[String]): Unit = {

		if (args.length > 1) {
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

		// val Array(inputPath) = args

		val inputPath = "/Users/angel/study/大华大数据/项目/互联网广告/互联网广告第一天/output1"

		// 读取数据源
		val df: DataFrame = spark.read.parquet(inputPath)

		// 创建临时视图
		df.createTempView("log")

		spark.sql(
			"""
			  | select
			  | 	provincename,
			  |  	cityname,
			  |   	row_number() over(partition by provincename order by cityname)
			  | from log group by provincename, cityname
			  |""".stripMargin)
		  .show(50)

		spark.close
	}
}
