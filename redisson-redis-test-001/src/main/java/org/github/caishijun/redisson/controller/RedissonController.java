package org.github.caishijun.redisson.controller;

import org.redisson.Redisson;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RBlockingQueue;
import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.Serializable;
import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@RestController
public class RedissonController implements Serializable {

    public RedissonClient clusterServersRedissonClient() {
        Config config = new Config();
        config.useClusterServers() //这是用的集群server
            .setScanInterval(200000) //设置集群状态扫描时间
            .setMasterConnectionPoolSize(10000) //设置连接数
            .setSlaveConnectionPoolSize(10000)
            .addNodeAddress("redis://192.168.3.51:7000",
                "redis://192.168.3.51:7001",
                "redis://192.168.3.51:7002",
                "redis://192.168.3.52:6379",
                "redis://192.168.3.52:6380",
                "redis://192.168.3.52:6381");
        RedissonClient redissonClient = Redisson.create(config);
        return redissonClient;
    }

    public RedissonClient singleServerRedissonClient() {
        Config config = new Config();
        config.useSingleServer()
            .setAddress("redis://192.168.1.33:6379")
            .setDatabase(15);
        RedissonClient redissonClient = Redisson.create(config);
        return redissonClient;
    }

    @RequestMapping("/syncInvoke")
    public String syncInvoke() throws Exception {

        String RAtomicName = "genId_";
        RedissonClient redissonClient = singleServerRedissonClient();
        //清空自增的ID数字
        RAtomicLong atomicLong = redissonClient.getAtomicLong(RAtomicName);
        atomicLong.set(111111);
        atomicLong.get();

        return "success";
    }

    @RequestMapping("/asyncInvoke")
    public String asyncInvoke() throws Exception {

        String RAtomicName = "genId_";
        RedissonClient redissonClient = singleServerRedissonClient();
        //清空自增的ID数字
        RAtomicLong atomicLong = redissonClient.getAtomicLong(RAtomicName);
        RFuture rFutureSet = atomicLong.setAsync(222222);
        rFutureSet.get();
        RFuture rFutureGet = atomicLong.getAsync();
        rFutureGet.get();

        return "success";
    }

    @RequestMapping("/syncInvokeCluster")
    public String syncInvokeCluster() throws Exception {

        String RAtomicName = "cluster_genId_";
        RedissonClient redissonClient = clusterServersRedissonClient();
        //清空自增的ID数字
        RAtomicLong atomicLong = redissonClient.getAtomicLong(RAtomicName);
        atomicLong.set(333333);
        //atomicLong.get();

        Long start = new Date().getTime();
        atomicLong.get();
        Long end = new Date().getTime();
        System.out.println("CaiTest : duration = " + (end - start));

        return "success";
    }

    @RequestMapping("/asyncInvokeCluster")
    public String asyncInvokeCluster() throws Exception {

        String RAtomicName = "cluster_genId_";
        RedissonClient redissonClient = clusterServersRedissonClient();
        //清空自增的ID数字
        RAtomicLong atomicLong = redissonClient.getAtomicLong(RAtomicName);
        RFuture rFutureSet = atomicLong.setAsync(444444);
        rFutureSet.get();
        RFuture rFutureGet = atomicLong.getAsync();
        rFutureGet.get();

        try {
            Long start = new Date().getTime();
            RFuture rFutureGetDuration = atomicLong.getAsync();
            Object resultDuration = rFutureGetDuration.get();
            Long end = new Date().getTime();
            System.out.println("CaiTest : duration = " + (end - start));
            System.out.println("resultDuration :" + resultDuration.toString());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        return "success";
    }

    @RequestMapping("/lockCluster")
    public String lockCluster() throws Exception {
        RedissonClient redissonClient = clusterServersRedissonClient();

        RLock lock = redissonClient.getLock("TEST");
        try {
            lock.lock();
            System.out.println("Request Thread - " + Thread.currentThread().getName() + " locked and begun...");
            Thread.sleep(5000); // 5 sec
            System.out.println("Request Thread - " + Thread.currentThread().getName() + " ended successfully...");
        } catch (Exception ex) {
            System.out.println("Error occurred");
        } finally {
            lock.unlock();
            System.out.println("Request Thread - " + Thread.currentThread().getName() + " unlocked...");
        }
        return "success";
    }

    @RequestMapping("/queueCluster")
    public String queueCluster() throws Exception {
        RedissonClient redissonClient = clusterServersRedissonClient();

        RBlockingQueue<RedissonController> queue = redissonClient.getBlockingQueue("anyQueue");
        queue.offer(new RedissonController());
        RedissonController obj = queue.peek();
        RedissonController someObj = queue.poll();
        RedissonController ob = queue.poll(1, TimeUnit.SECONDS);

        return "success";
    }

}

