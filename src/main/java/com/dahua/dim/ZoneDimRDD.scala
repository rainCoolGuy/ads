package com.dahua.dim

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object ZoneDimRDD {

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
		  .master("local[*]")
		  .getOrCreate


		val Array(inputPath, outputPath) = args

		val df: DataFrame = spark.read.parquet(inputPath)

//		df.map(
//			row => {
//				val requestmode: Int = row.getAs[Int]("requestmode")
//				requestmode
//			}
//		)


		spark.stop()
	}
}
