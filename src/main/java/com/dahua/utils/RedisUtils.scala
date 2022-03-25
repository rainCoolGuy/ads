package com.dahua.utils

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}

object RedisUtils {

	// 连接 redis的工具方法
	private val jedisPool = new JedisPool(new GenericObjectPoolConfig[Jedis], "127.0.0.1", 6379, 30000, null, 1)

	def getJedis: Jedis = jedisPool.getResource


}
