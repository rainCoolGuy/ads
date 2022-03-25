package com.dahua.tools

import com.dahua.bean.LogBean
import com.dahua.utils.{LogSchema, NumFormat}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object Log2Parquet2 {

	def main(args: Array[String]): Unit = {

		if (args.length != 2) {
			println(
				"""
				  |缺少参数
				  |""".stripMargin)
			sys.exit(0)
		}

		// 创建 SparkSession对象
		val conf: SparkConf = new SparkConf()
		  .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

		// 将自定义对象进行kryo序列化
		conf.registerKryoClasses(Array(classOf[LogBean]))
		val spark: SparkSession = SparkSession
		  .builder
		  .config(conf)
		  .appName("Log2Parquet")
		  .master("local[*]")
		  .getOrCreate
		import spark.implicits._
		val sc: SparkContext = spark.sparkContext
		val Array(inputPath, outputPath): Array[String] = args

		val line: RDD[String] = sc.textFile(inputPath)

		val rdd: RDD[Array[String]] = line
		  .map(_.split(",", -1))
		  .filter(_.length >= 85)
		val rddLogBean: RDD[LogBean] = rdd.map(LogBean(_))

		val df: DataFrame = spark.createDataFrame(rddLogBean)

		df.write.parquet(outputPath)

		sc.stop
		spark.close

	}
}
