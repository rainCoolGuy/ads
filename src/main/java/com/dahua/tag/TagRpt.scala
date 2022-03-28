package com.dahua.tag

import com.dahua.bean.LogBean
import com.dahua.utils.TagUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.util.UUID

object TagRpt {

	def main(args: Array[String]): Unit = {

		if (args.length != 4) {
			println(
				"""
				  |缺少参数
				  |inputPath, appMapping, stopWords, outputPath
				  |""".stripMargin)
			sys.exit()
		}

		// 创建sparksession对象
		val conf: SparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

		val spark: SparkSession = SparkSession.builder().config(conf).appName("Log2Parquet").master("local[1]").getOrCreate()

		val sc: SparkContext = spark.sparkContext

		import spark.implicits._

		// 接收参数
		var Array(inputPath, appMapping, stopWords, outputPath) = args

		// 读取appMapping广播变量
		val appMap: Map[String, String] = sc
		  .textFile(appMapping)
		  .map(
			  line => {
				  val str: Array[String] = line.split("[:]", -1)
				  (str(0), str(1))
			  }
		  )
		  .collect.toMap
		// 广播app变量
		val broadcastAppMap: Broadcast[Map[String, String]] = sc.broadcast(appMap)

		val stopWordsMap: Map[String, Int] = sc
		  .textFile(stopWords)
		  .map((_, 0))
		  .collect
		  .toMap
		// 停用词广播变量
		val broadcastStopWord: Broadcast[Map[String, Int]] = sc.broadcast(stopWordsMap)

		// 读取数据源, 打数据标签
		val df: DataFrame = spark.read.parquet(inputPath)

		val tagDS: Dataset[(String, List[(String, Int)])] = df
		  .where(TagUtil.tagUserIdFilterParam)
		  .map(
			  row => {
				  // 广告标签
				  val adsMap: Map[String, Int] = AdsTags.makeTags(row)

				  // app 标签
				  val appMaps: Map[String, Int] = AppTags.makeTags(row, broadcastAppMap.value)

				  // 驱动标签
				  val driverMap: Map[String, Int] = DriverTags.makeTags(row)

				  // 关键字标签
				  val keyMap: Map[String, Int] = KeysTags.makeTags(row, broadcastStopWord.value)

				  // 地域标签
				  val pcMap: Map[String, Int] = PCTags.makeTags(row)

				  // 商圈标签
				  val businessMap: Map[String, Int] = BusinessDistrictTag.makeTags(row)

				  // 获取用户ID
				  val headId: String = TagUtil.getUserId(row).head
				  val resultId: String = if (headId == "" || headId == null) UUID.randomUUID().toString.substring(0, 6)
				  else headId
				  (resultId, (adsMap ++ appMaps ++ driverMap ++ keyMap ++ pcMap ++ businessMap).toList)
			  }
		  )
		tagDS.rdd.reduceByKey(
			(list1, list2) => {
				(list1 ++ list2)
				  .groupBy(_._1)
				  .mapValues(_.foldLeft(0)(_ + _._2))
				  .toList
			}
		)
		  .saveAsTextFile(outputPath)

		sc.stop()
		spark.close()
	}
}
