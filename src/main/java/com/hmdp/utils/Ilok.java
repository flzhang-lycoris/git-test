package com.hmdp.utils;

/**
 * @packagename com.hmdp.utils
 * @Description
 * @Date 2022/11/26 20:03
 */
public interface Ilok {
    /**
     * 尝试获取锁
     * @param timeoutSec 锁持有的时间，过期后自动释放
     * @return ture表示获取锁成功,false代表获取锁失败
     */
    boolean tryLock(long timeoutSec);

    /**
     * 释放锁
     */
    void unlock();
}
