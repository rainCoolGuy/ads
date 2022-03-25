package com.dahua.analyse

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object ProCityAnalyseForRDD {
	val conf: SparkConf = new SparkConf()
	  .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
	// 创建 SparkSession对象
	val spark: SparkSession = SparkSession
	  .builder
	  .config(conf)
	  .appName("Log2Parquet")
	  .master("local[*]")
	  .getOrCreate

	val sc: SparkContext = spark.sparkContext

	def main(args: Array[String]): Unit = {

		if (args.length != 2) {
			println(
				"""
				  |缺少参数
				  |inputPath outputPath
				  |""".stripMargin)
			sys.exit(0)
		}

		val line: RDD[String] = sc.textFile(args(0))

		val field: RDD[Array[String]] = line.map(_.split(",", -1))

		val proCityRDD: RDD[((String, String), Int)] = field.filter(_.length >= 85)
		  .map(
			  arr => {
				  ((arr(24), arr(25)), 1)
			  }
		  )
		val reduceRDD: RDD[((String, String), Int)] = proCityRDD
		  .reduceByKey(_ + _)

		reduceRDD.cache()

		val index: Int = reduceRDD.map(
			_._1._1
		).distinct().count().toInt


		reduceRDD
		  .sortBy(_._2)
		  .partitionBy(new MyPartition(index))
		  .saveAsTextFile("data/output")

		sc.stop
		spark.close
	}

	private val partAcc = new MyAcc
	sc.register(partAcc, "part")

	class MyPartition(val count: Int) extends Partitioner {

		override def numPartitions: Int = count

		override def getPartition(key: Any): Int = {
			val value: String = key.toString
			val str: String = value.substring(1, value.indexOf(","))

			partAcc add str
			partAcc.value(str)

		}
	}


	class MyAcc extends AccumulatorV2[String, mutable.Map[String, Int]] {
		var index = 0

		private val map: mutable.Map[String, Int] = mutable.Map[String, Int]()


		override def isZero: Boolean = map.isEmpty

		override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
			new MyAcc
		}

		override def reset(): Unit = {
			map.clear()
		}

		override def add(v: String): Unit = {
			if (!map.contains(v)) {
				map.put(v, index)
				index += 1
			}
		}

		override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {

			val map1: mutable.Map[String, Int] = this.map
			val map2: mutable.Map[String, Int] = other.value
			map2.foreach {
				case (k, v) => {
					if (!map1.contains(k)) {
						map.put(k, v)
					} else {
						map2(k) = map(k)
					}
				}
			}
		}

		override def value: mutable.Map[String, Int] = map

	}
}