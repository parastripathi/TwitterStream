package config;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class JedisConfig {

    private static JedisPool pool = null;
    private static Jedis jedis = null;
    private static String redisHost;
    private static int redisPort;

    public static Jedis getJedisInstance() {

        if (null == jedis) {
            StormConfig stormConfig = StormConfig.getInstance();
            redisHost = stormConfig.getProperty("REDIS_HOST");
            redisPort = Integer.parseInt(stormConfig.getProperty("REDIS_PORT"));

            pool = new JedisPool(redisHost, redisPort);
            jedis = pool.getResource();
        }
        return jedis;
    }


}
