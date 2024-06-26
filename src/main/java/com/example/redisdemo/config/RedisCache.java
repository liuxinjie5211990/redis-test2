package com.example.redisdemo.config;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * redis缓存工具类
 */
@Component
public class RedisCache {

    @Autowired
    private RedisTemplate redisTemplate;

    /**
     * 缓存基本的对象，Integer、String、实体类等
     *
     * @param key 缓存的键值
     * @param value 缓存的值
     */
    public <T> void setCacheObject(final String key, final T value)
    {
        redisTemplate.opsForValue().set(key, value);
    }

    /**
     * 缓存基本的对象，Integer、String、实体类等
     *
     * @param key 缓存的键值
     * @param value 缓存的值
     * @param timeout 时间
     * @param timeUnit 时间颗粒度
     */
    public <T> void setCacheObject(final String key, final T value, final Integer timeout, final TimeUnit timeUnit)
    {
        redisTemplate.opsForValue().set(key, value, timeout, timeUnit);
    }

    /**
     * 设置有效时间
     * 默认【秒】为单位
     * @param key Redis键
     * @param timeout 超时时间
     * @return true=设置成功；false=设置失败
     */
    public boolean expire(final String key, final long timeout)
    {
        return expire(key, timeout, TimeUnit.SECONDS);
    }

    /**
     * 设置有效时间
     * 【自定义时间单位】
     * @param key Redis键
     * @param timeout 超时时间
     * @param unit 时间单位
     * @return true=设置成功；false=设置失败
     *
     *
     *  《TimeUnit详解》
     *    TimeUnit.DAYS //天
     *    TimeUnit.HOURS //小时
     *    TimeUnit.MINUTES //分钟
     *    TimeUnit.SECONDS //秒
     *    TimeUnit.MILLISECONDS //毫秒
     *    TimeUnit.NANOSECONDS //毫微秒
     *    TimeUnit.MICROSECONDS //微秒
     */
    public boolean expire(final String key, final long timeout, final TimeUnit unit)
    {
        return redisTemplate.expire(key, timeout, unit);
    }

    /**
     * 获得缓存的基本对象。
     *
     * @param key 缓存键值
     * @return 缓存键值对应的数据
     */
    public <T> T getCacheObject(final String key)
    {
        ValueOperations<String, T> operation = redisTemplate.opsForValue();
        return operation.get(key);
    }

    /**
     * 删除单个key
     *
     * @param key
     */
    public boolean deleteKey(final String key)
    {
        return redisTemplate.delete(key);
    }

    /**
     * 删除多个key
     *
     * @param collection 多个对象
     * @return
     */
    public long deleteKeys(final Collection collection)
    {
        return redisTemplate.delete(collection);
    }

    /**
     * 缓存List数据
     *
     * @param key 缓存的键值
     * @param dataList 待缓存的List数据
     * @return 缓存的对象
     */
    public <T> long setCacheList(final String key, final List<T> dataList)
    {
        Long count = redisTemplate.opsForList().rightPushAll(key, dataList);
        return count == null ? 0 : count;
    }

    /**
     * 获得缓存的list对象
     *
     * @param key 缓存的键值
     * @return 缓存键值对应的数据
     */
    public <T> List<T> getCacheList(final String key)
    {
        return redisTemplate.opsForList().range(key, 0, -1);
    }

    /**
     * 缓存Set
     *
     * @param key 缓存键值
     * @param value 缓存的数据可以传多个，用，隔开
     *              例如：setCacheSet("key","1","2","3")
     * @return 缓存数据的对象
     */
    public <T> long setCacheSet(final String key, final Object... value)
    {
        Long count = redisTemplate.opsForSet().add(key, value);
        return count == null ? 0 : count;
    }

    /**
     * 获得缓存的set
     *
     * @param key
     * @return
     */
    public <T> Set<T> getCacheSet(final String key)
    {
        return redisTemplate.opsForSet().members(key);
    }

    /**
     * 缓存Map
     *
     * @param key
     * @param dataMap
     */
    public <T> void setCacheMap(final String key, final Map<String, T> dataMap)
    {
        if (dataMap != null) {
            redisTemplate.opsForHash().putAll(key, dataMap);
        }
    }

    /**
     * 获得缓存的Map
     *
     * @param key
     * @return
     */
    public <T> Map<String, T> getCacheMap(final String key)
    {
        return redisTemplate.opsForHash().entries(key);
    }

    /**
     * 获取缓存map的所有key值
     *
     * @param key
     * @return
     */
    public <T> Set<String> getCacheMapKeys(final String key)
    {
        return redisTemplate.opsForHash().keys(key);
    }

    /**
     * 往Hash中存入数据
     *
     * @param key Redis键
     * @param hKey Hash键
     * @param value 值
     */
    public <T> void setCacheMapValue(final String key, final String hKey, final T value)
    {
        redisTemplate.opsForHash().put(key, hKey, value);
    }

    /**
     * 获取Hash中的数据
     *
     * @param key Redis键
     * @param hKey Hash键
     * @return Hash中的对象
     */
    public <T> T getCacheMapValue(final String key, final String hKey)
    {
        HashOperations<String, String, T> opsForHash = redisTemplate.opsForHash();
        return opsForHash.get(key, hKey);
    }

    /**
     * 获取多个Hash中的数据
     *
     * @param key Redis键
     * @param hKeys Hash键集合
     * @return Hash对象集合
     */
    public <T> List<T> getMultiCacheMapValue(final String key, final Collection<Object> hKeys)
    {
        return redisTemplate.opsForHash().multiGet(key, hKeys);
    }

    /**
     * 模糊查询所有key值
     *
     * @param pattern 字符串前缀
     * @return 对象列表
     */
    public Collection<String> keys(final String pattern)
    {
        return redisTemplate.keys(pattern);
    }

    public void batchInsert(Map<String, String> keyValueMap) {
        redisTemplate.opsForValue().multiSet(keyValueMap);
    }

    public void batchInsert(List<String> keys, List<String> values) {
        redisTemplate.execute((RedisCallback<Object>) connection -> {
            // 构建Lua脚本
            // 构建Lua脚本
            String script = "for i = 1, #KEYS do redis.call('SET', KEYS[i], ARGV[i]) end";

            // 将Lua脚本和参数转换为字节数组
            byte[] scriptBytes = script.getBytes();
            byte[][] keysAndArgs = new byte[keys.size() * 2][];
            for (int i = 0; i < keys.size(); i++) {
                keysAndArgs[i] = keys.get(i).getBytes();
                keysAndArgs[i + keys.size()] = values.get(i).getBytes();
            }

            // 执行Lua脚本
            connection.eval(scriptBytes, ReturnType.fromJavaType(Object.class), keys.size(), keysAndArgs);

            return null;
        });
    }

    public synchronized void executePipelined(Map<String, Object> map) {
        RedisSerializer keySerializer = redisTemplate.getKeySerializer();
        RedisSerializer valueSerializer = redisTemplate.getValueSerializer();
        redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            map.forEach((key, value) -> connection.set(keySerializer.serialize(key), valueSerializer.serialize(value)));
            return null;
        });
    }

    public synchronized Map<String, Object> executeGetPipelined(List<String> keys) {
        RedisSerializer keySerializer = redisTemplate.getKeySerializer();
        List<Object> objects = redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            for (String key : keys) {
                if (StringUtils.isBlank(key)) {
                    throw new RuntimeException("redis的key不能为空");
                }
                connection.get(keySerializer.serialize(key));
            }
            return null;
        });
        Map<String, Object> map = new HashMap<>();
        keys.forEach(t -> map.put(t, objects.get(keys.indexOf(t))));
        return map;
    }

}
