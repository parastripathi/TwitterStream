package config;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class JedisConfig {

    public static final String REDIS_HOST = "REDIS_HOST";
    public static final String REDIS_PORT = "REDIS_PORT";
    private static Jedis jedis = null;

    public static Jedis getJedisInstance() {

        if (null == jedis) {
            StormConfig stormConfig = StormConfig.getInstance();
            String redisHost = stormConfig.getProperty(REDIS_HOST);
            int redisPort = Integer.parseInt(stormConfig.getProperty(REDIS_PORT));

            JedisPool pool = new JedisPool(redisHost, redisPort);
            jedis = pool.getResource();

        }
        return jedis;
    }


}
