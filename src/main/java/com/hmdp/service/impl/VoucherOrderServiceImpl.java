package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;
//    private BlockingQueue<VoucherOrder> orderTask = new ArrayBlockingQueue<>(1024*1024);
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

//    @PostConstruct
//    private void init(){
//        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
//    }

    private class VoucherOrderHandler implements Runnable {
        String queueName = "stream.orders";
        @Override
        public void run() {
            while (true){
                try {
                    // 1.获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 200 0 STEAMS streams.order >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    // 2.判断获取消息是否成功
                    if(list == null || list.isEmpty()){
                        // 2.1若获取失败，说明无消息，继续下一次循环
                        continue;
                    }
                    // 3.解析MAP消息为订单实体类
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 4.如果获取成功，可以下单
                    handleVoucherOrder(voucherOrder);
                    // 5.ack确认
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常",e);
                    handlePendingList();
                }
            }
        }
        private void handlePendingList() {
            while (true){
                try {
                    // 1.获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 STEAMS streams.order 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                );
                    // 2.判断获取消息是否成功
                    if(list == null || list.isEmpty()){
                        // 2.1若获取失败，说明无消息，继续下一次循环
                        continue;
                    }
                    // 3.解析MAP消息为订单实体类
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 4.如果获取成功，可以下单
                    handleVoucherOrder(voucherOrder);
                    // 5.ack确认
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常",e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
    }



//    private class VoucherOrderHandler implements Runnable {
//        @Override
//        public void run() {
//            while (true){
//                try {
//                    // 1.从队列中获取订单信息
//                    VoucherOrder voucherOrder = orderTask.take();
//                    // 2.创建订单
//                    handleVoucherOrder(voucherOrder);
//
//                } catch (InterruptedException e) {
//                    log.error("处理订单异常",e);
//                }
//            }
//        }
//    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();
        //创建锁对象（兜底）
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //获取锁
        boolean isLock = lock.tryLock();
        //判断是否获取锁成功
        if (!isLock) {
            //获取失败,返回错误或者重试
            throw new RuntimeException("发送未知错误");
        }
        try {
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            //释放锁
            lock.unlock();
        }
    }

    /**
     * 秒杀优惠券（异步，基于redis的stream）
     * @param voucherId
     * @return
     */
    public Result seckillVoucher(Long voucherId) {
        //获取用户
        Long userId =  UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        //执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(),String.valueOf(orderId)
        );
        int r = result.intValue();
        //若结果不为0，未成功返回异常信息
        if(r!=0){
            // 1为库存不足,2为不能重复下单
            return Result.fail(r == 1?"库存不足":"不能重复下单");
        }
        //若结果为0，成功下单，保存到阻塞队列
        // 创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        // 订单id
        voucherOrder.setId(orderId);
        // 用户id
        voucherOrder.setUserId(userId);
        // 优惠券id
        voucherOrder.setVoucherId(voucherId);
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        //返回订单id
        return Result.ok(orderId);
    }


    /**
     * 秒杀优惠券（异步，基于阻塞队列）
     *
     * @param voucherId
     * @return
     */
    private IVoucherOrderService proxy = null;
    @Override
//    public Result seckillVoucher(Long voucherId) {
//        //获取用户
//        Long userId =  UserHolder.getUser().getId();
//        long orderId = redisIdWorker.nextId("order");
//        //执行lua脚本
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(), userId.toString()
//        );
//        int r = result.intValue();
//        //若结果不为0，未成功返回异常信息
//        if(r!=0){
//            // 1为库存不足,2为不能重复下单
//            return Result.fail(r == 1?"库存不足":"不能重复下单");
//        }
//        //若结果为0，成功下单，保存到阻塞队列
//        // 创建订单
//        VoucherOrder voucherOrder = new VoucherOrder();
//        // 订单id
//        voucherOrder.setId(orderId);
//        // 用户id
//        voucherOrder.setUserId(userId);
//        // 优惠券id
//        voucherOrder.setVoucherId(voucherId);
//        proxy = (IVoucherOrderService) AopContext.currentProxy();
//        // 放入阻塞队列
//        orderTask.add(voucherOrder);
//        //返回订单id
//        return Result.ok(orderId);
//    }

//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        // 1，查询优惠券
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        // 2.判断秒杀是否开始
//        if(voucher.getBeginTime().isAfter(LocalDateTime.now())){
//            return Result.fail("秒杀尚未开始");
//        }
//        // 3.判断秒杀是否结束
//        if(voucher.getEndTime().isBefore(LocalDateTime.now())){
//            return Result.fail("秒杀已经结束");
//        }
//        // 4.判断库存是否充足
//        if(voucher.getStock()<1){
//            return Result.fail("库存不足");
//        }
//        Long userId =  UserHolder.getUser().getId();
//
//        //存在集群下的线程并发安全问题
////        synchronized (userId.toString().intern()){
////            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
////            return proxy.createVoucherOrder(voucherId);
////        }
//
//        SimpleRedisLock lock = new SimpleRedisLock("order"+userId,stringRedisTemplate);
//        RLock lock1 = redissonClient.getLock("order" + userId);
//        boolean islock = false;
//        try {
//            islock = lock1.tryLock(1L, TimeUnit.SECONDS);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
////        boolean islock = lock.tryLock(1200);
//        if(!islock){
//            return Result.fail("不允许重复下单(已经有线程拿到该用户的下单锁)");
//        }
//        try {
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        } finally {
//            lock1.unlock();
//        }
//
//    }
    @Transactional
    public void createVoucherOrder(VoucherOrder vocherOrder) {
        // 5.一人一单
        Long userid =  vocherOrder.getId();
        Integer count = query().eq("user_id", userid).eq("voucher_id", vocherOrder.getVoucherId()).count();
        if(count>0){
            //用户已经购买
            log.error("库存不足");
        }

        // 6.扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1").eq("voucher_id", vocherOrder.getVoucherId()).gt("stock",0)//CAS判断指是否大于0
                .update();
        if(!success){
            //扣减失败
            log.error("库存不足");
            return;
        }
        // 6.创建订单
        save(vocherOrder);
    }
    /**
     * 创建订单方法，乐观锁判断扣减库存
     *
     * @param voucherId
     * @return
     */
//    @Transactional
//    public Result createVoucherOrder(Long voucherId) {
//        // 5.一人一单
//        Long userid =  UserHolder.getUser().getId();
//        Integer count = query().eq("user_id", userid).eq("voucher_id", voucherId).count();
//        if(count>0){
//            //用户已经购买
//            return Result.fail("用户已经购买过一次!");
//        }
//
//        // 6.扣减库存
//        boolean success = seckillVoucherService.update()
//                .setSql("stock = stock - 1").eq("voucher_id", voucherId).gt("stock",0)//CAS判断指是否大于0
//                .update();
//        if(!success){
//            //扣减失败
//            return Result.fail("库存不足");
//        }
//        // 6.创建订单
//        VoucherOrder voucherOrder = new VoucherOrder();
//        // 6.1 订单id
//        long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        // 6.2 用户id
//        Long userId = UserHolder.getUser().getId();
//        voucherOrder.setUserId(userId);
//        // 6.3 优惠券id
//        voucherOrder.setVoucherId(voucherId);
//        save(voucherOrder);
//
//        return Result.ok(orderId);
//    }


}
