package com.dahua.dim

import com.dahua.logBean.LogBean
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object ZoneDimForRDDV2RedisCache {

	def main(args: Array[String]): Unit = {
		if (args.length != 3) {
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
		var Array(inputPath, appMapping, outputPath) = args

		// 先读映射文件: appmapping
		val appMap: Map[String, String] = sc
		  .textFile(appMapping)
		  .map(
			  line => {
				  val arr: Array[String] = line.split("[:]", -1)
				  (arr(0), arr(1))
			  }
		  )
		  .collect()
		  .toMap

		// 使用广播变量, 进行广播
		val appBroadcast: Broadcast[Map[String, String]] = sc.broadcast(appMap)

		val log: RDD[String] = sc.textFile(inputPath)
		val logRDD: RDD[LogBean] = log
		  .map(_.split(",", -1))
		  .filter(_.length >= 85)
		  .map(LogBean(_))
		  .filter(
			  t => {
				  t.appid.nonEmpty
			  }
		  )
		logRDD
		  .map(
			  log => {
				  var appname: String = log.appname
				  if (appname == "" || appname.isEmpty) {
					  appname = appBroadcast.value.getOrElse(log.appid, "不明确")
				  }

				  val ysqqs: List[Double] = DIMZhibiao.qqsRtp(log.requestmode, log.processnode)

				  (appname, ysqqs)
			  }
		  )
		  .reduceByKey(
			  (list1, list2) => {
				  list1.zip(list2)
					.map(
						x => {
							x._1 + x._2
						}
					)
			  }
		  )

		sc.stop()


	}

}
