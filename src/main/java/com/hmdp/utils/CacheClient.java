package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.toolkit.StringUtils;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

/**
 * @packagename com.hmdp.utils
 * @Description
 * @Date 2022/11/25 0:41
 */
@Slf4j
@Component
public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit){
        //设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        //写入redis
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(redisData));
    }

    //缓存穿透
    public <R,ID> R queryWithPassThrough (String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback,
                                          Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        // 1.redis查询
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2.不为空则查询
        if (StringUtils.isNotBlank(json)) {
            R r = JSONUtil.toBean(json, type);
            return r;
        }

        if ("".equals(json)) {
            return null;
        }
        // 3.未命中则查询数据库
        R r = dbFallback.apply(id);

        if (r == null) {
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        // 4.若存在，写入redis
        this.set(key,r,time,unit);

        return r;
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    //缓存穿透
    public <R,ID> R queryWithLogicalExpire (String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback,
                                            Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        // 1.redis查询
        String Json = stringRedisTemplate.opsForValue().get(key);
        // 2.不为空则查询
        if (StringUtils.isBlank(Json)) {
            return null;
        }
        // 4. 命中,需要先把json反序列
        RedisData redisData = JSONUtil.toBean(Json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 5.判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())){
            // 5.1 未过期直接返回
            return r;
        }
        // 6.缓存重建
        // 6.1获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        // 6.2判断是否获取成功
        if (isLock){
            // TODO 6.3 成功,开启独立线程,实现缓存重建
            // DoubleCheck
            Json = stringRedisTemplate.opsForValue().get(key);
            // 2.不为空则查询
            if (StringUtils.isBlank(Json)) {
                return null;
            }
            // 4. 命中,需要先把json反序列
            redisData = JSONUtil.toBean(Json, RedisData.class);
            r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
            expireTime = redisData.getExpireTime();
            // 5.判断是否过期
            if(expireTime.isAfter(LocalDateTime.now())){
                // 5.1 未过期直接返回
                return r;
            }
            CACHE_REBUILD_EXECUTOR.submit(()->{
                try {
                    //重建缓存
                    //1.查询数据库
                    R r1 = dbFallback.apply(id);
                    //2.写入redis
                    this.setWithLogicalExpire(key,r1,time,unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    // 一定释放锁
                    unLock(lockKey);
                }
            });
        }
        // 6.4 返回过期的商品信息
        return r;
    }

    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key,"1",10,TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }

}
