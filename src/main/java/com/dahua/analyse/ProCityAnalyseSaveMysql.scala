package com.dahua.analyse

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object ProCityAnalyseSaveMysql {

	def main(args: Array[String]): Unit = {

		//		if (args.length != 1) {
		//			println(
		//				"""
		//				  |缺少参数
		//				  |inputPath outputPath
		//				  |""".stripMargin)
		//			sys.exit(0)
		//		}

		// 创建 SparkSession对象
		val conf: SparkConf = new SparkConf()
		  .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

		val spark: SparkSession = SparkSession
		  .builder
		  .config(conf)
		  .appName("Log2Parquet")
		  .master("local[1]")
		  .getOrCreate

		val sc: SparkContext = spark.sparkContext
		import spark.implicits._
		//		val Array(inputPath, outputPath): Array[String] = args
		val df: DataFrame = spark.read.parquet("/Users/angel/study/大华大数据/项目/互联网广告/互联网广告第一天/output1")

		df.createTempView("log")

		// 编写sql语句
		val sql: String =
			"""
			  |select
			  |	 provincename province,
			  |  cityname city,
			  |  count(1) as pcCnt
			  |from log
			  |group by provincename, cityname
			  |""".stripMargin

		val resDF: DataFrame = spark.sql(sql)

		// resDF.show()
		val props = new Properties()
		props.setProperty("user", "root")
		props.setProperty("password", "wangfengxu")
		resDF
		  .write
		  .mode("append")
		  .jdbc("jdbc:mysql://localhost:3306/emp",
			  "dept1", props)
//		resDF
//		  .write
//		  .format("jdbc")
//		  .option("url", "jdbc:mysql://localhost:3306/emp")
//		  .option("driver", "com.mysql.cj.jdbc.Driver")
//		  .option("user", "root")
//		  .option("password", "123456")
//		  .option("dbtable", "dept1")
//		  // .mode(SaveMode.Append)
//		  .save()


		sc.stop
		spark.close
	}
}
