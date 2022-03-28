package com.dahua.dim

import com.dahua.bean.LogBean
import com.dahua.utils.RedisUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

object ZoneDimForRDDV2RedisCache {

	def main(args: Array[String]): Unit = {
		if (args.length != 2) {
			println(
				"""
				  |缺少参数
				  |inputpath  outputpath
				  |""".stripMargin)
			sys.exit()
		}

		val conf: SparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

		val spark: SparkSession = SparkSession.builder().config(conf).appName("Log2Parquet").master("local[1]").getOrCreate()

		val sc: SparkContext = spark.sparkContext

		import spark.implicits._

		// 接收参数
		var Array(inputPath, outputPath) = args


		sc
		  .textFile(inputPath)
		  .map(_.split(",", -1))
		  .filter(_.length >= 85)
		  .map(LogBean(_))
		  .filter(
			  t => {
				  t.appid.nonEmpty
			  }
		  )
		  .mapPartitions(
			  iter => {
				  var appName: String = ""
				  var ysqqs: List[Double] = Nil
				  val redis: Jedis = RedisUtils.getJedis
				  val map: Iterator[(String, List[Double])] = iter.map(
					  x => {
						  appName = x.appname
						  if (appName == "" || appName.isEmpty) {
							  if (x.appid == "") appName = "不明确"
							  else appName = redis.get(x.appid)
						  }
						  ysqqs = DIMZhibiao.qqsRtp(x.requestmode, x.processnode)

						  (appName, ysqqs)
					  }
				  )
				  redis.close()
				  map
			  }
		  )
		  .reduceByKey(
			  (list1, list2) => {
				  list1.zip(list2)
					.map(
						tup => {
							tup._1 + tup._2
						}
					)
			  }
		  )
		  .foreach(println)

		sc.stop()
		spark.stop()
	}

}
