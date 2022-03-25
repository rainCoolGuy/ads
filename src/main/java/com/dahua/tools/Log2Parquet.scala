package com.dahua.tools

import com.dahua.utils.{LogSchema, NumFormat}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Log2Parquet {

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

		val spark: SparkSession = SparkSession
		  .builder
		  .config(conf)
		  .appName("Log2Parquet")
		  .master("local[*]")
		  .getOrCreate

		val sc: SparkContext = spark.sparkContext
		import spark.implicits._
		val Array(inputPath, outputPath): Array[String] = args

		val line: RDD[String] = sc.textFile(inputPath)

		val logData: RDD[Array[String]] = line
		  .map(_.split(",", -1))
		  .filter(_.length >= 85)

		val row: RDD[Row] = logData.map(
			line => {
				Row(
					line(0),
					NumFormat.toInt(line(1)),
					NumFormat.toInt(line(2)),
					NumFormat.toInt(line(3)),
					NumFormat.toInt(line(4)),
					line(5),
					line(6),
					NumFormat.toInt(line(7)),
					NumFormat.toInt(line(8)),
					NumFormat.toDouble(line(9)),
					NumFormat.toDouble(line(10)),
					line(11),
					line(12),
					line(13),
					line(14),
					line(15),
					line(16),
					NumFormat.toInt(line(17)),
					line(18),
					line(19),
					NumFormat.toInt(line(20)),
					NumFormat.toInt(line(21)),
					line(22),
					line(23),
					line(24),
					line(25),
					NumFormat.toInt(line(26)),
					line(27),
					NumFormat.toInt(line(28)),
					line(29),
					NumFormat.toInt(line(30)),
					NumFormat.toInt(line(31)),
					NumFormat.toInt(line(32)),
					line(33),
					NumFormat.toInt(line(34)),
					NumFormat.toInt(line(35)),
					NumFormat.toInt(line(36)),
					line(37),
					NumFormat.toInt(line(38)),
					NumFormat.toInt(line(39)),
					NumFormat.toDouble(line(40)),
					NumFormat.toDouble(line(41)),
					NumFormat.toInt(line(42)),
					line(43),
					NumFormat.toDouble(line(44)),
					NumFormat.toDouble(line(45)),
					line(46),
					line(47),
					line(48),
					line(49),
					line(50),
					line(51),
					line(52),
					line(53),
					line(54),
					line(55),
					line(56),
					NumFormat.toInt(line(57)),
					NumFormat.toDouble(line(58)),
					NumFormat.toInt(line(59)),
					NumFormat.toInt(line(60)),
					line(61),
					line(62),
					line(63),
					line(64),
					line(65),
					line(66),
					line(67),
					line(68),
					line(69),
					line(70),
					line(71),
					line(72),
					NumFormat.toInt(line(73)),
					NumFormat.toDouble(line(74)),
					NumFormat.toDouble(line(75)),
					NumFormat.toDouble(line(76)),
					NumFormat.toDouble(line(77)),
					NumFormat.toDouble(line(78)),
					line(79),
					line(80),
					line(81),
					line(82),
					line(83),
					NumFormat.toInt(line(84))
				)
			}
		)

		val df: DataFrame = spark.createDataFrame(row, LogSchema.structType)

		df.write.parquet(outputPath)

		sc.stop
		spark.close

	}
}
