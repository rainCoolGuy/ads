package com.dahua.tools

import com.dahua.utils.RedisUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis

object AppMappingToRedis {

	def main(args: Array[String]): Unit = {

		if (args.length != 1) {
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
		var Array(inputPath) = args
		sc
		  .textFile(inputPath)
		  .map(
			  line => {
				  val str: Array[String] = line.split("[:]", -1)
				  (str(0), str(1))
			  }
		  )
		  .foreachPartition(
			  iter => {
				  val jedis: Jedis = RedisUtils.getJedis
				  iter.foreach(mapping => {
					  jedis.del(mapping._1)
					  jedis.set(mapping._1, mapping._2)
					  val str: String = jedis.get(mapping._1)
				  })
				  jedis.close()
			  }
		  )

		sc.stop()
	}
}
