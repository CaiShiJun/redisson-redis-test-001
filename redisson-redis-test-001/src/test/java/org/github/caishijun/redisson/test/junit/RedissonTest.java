package org.github.caishijun.redisson.test.junit;

import org.github.caishijun.redisson.test.http.HttpClientUtils;
import org.junit.Test;

public class RedissonTest {
    private static String HOST = "localhost";
    private static int PORT = 8080;

    private static int FOR_TIMES = 1;
    private static int SLEEP_TIME = 5000;

    public static String getUrl(String uri) {
        return "http://" + HOST + ":" + PORT + uri;
    }

    @Test
    public void syncInvoke() throws Exception {
        for (int i = 0; i < FOR_TIMES; i++) {
            HttpClientUtils.sendGetRequest(getUrl("/syncInvoke"));
            Thread.sleep(SLEEP_TIME);
        }
    }

    @Test
    public void asyncInvoke() throws Exception {
        int count = 1;
        for (int i = 0; i < FOR_TIMES; i++) {
            HttpClientUtils.sendGetRequest(getUrl("/asyncInvoke"));
            Thread.sleep(SLEEP_TIME);
        }
    }

    @Test
    public void syncInvokeCluster() throws Exception {
        int count = 1;
        for (int i = 0; i < FOR_TIMES; i++) {
            HttpClientUtils.sendGetRequest(getUrl("/syncInvokeCluster"));
            Thread.sleep(SLEEP_TIME);
        }
    }

    @Test
    public void asyncInvokeCluster() throws Exception {
        int count = 1;
        for (int i = 0; i < FOR_TIMES; i++) {
            HttpClientUtils.sendGetRequest(getUrl("/asyncInvokeCluster"));
            Thread.sleep(SLEEP_TIME);
        }
    }

    @Test
    public void lockCluster() throws Exception {
        int count = 1;
        for (int i = 0; i < FOR_TIMES; i++) {
            HttpClientUtils.sendGetRequest(getUrl("/lockCluster"));
            Thread.sleep(SLEEP_TIME);
        }
    }

    @Test
    public void queueCluster() throws Exception {
        int count = 1;
        for (int i = 0; i < FOR_TIMES; i++) {
            HttpClientUtils.sendGetRequest(getUrl("/queueCluster"));
            Thread.sleep(SLEEP_TIME);
        }
    }

    @Test
    public void runAll() throws Exception {
        while (true) {
            syncInvoke();
            asyncInvoke();
            syncInvokeCluster();
            asyncInvokeCluster();
            lockCluster();
            queueCluster();
        }
    }
}