package com.dianping.pigeon.remoting.provider.process.statistics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.dianping.pigeon.log.Logger;

import com.dianping.pigeon.log.LoggerLoader;

public class ProviderStatisticsChecker implements Runnable {

    private static final Logger logger = LoggerLoader.getLogger(ProviderStatisticsChecker.class);

    @Override
    public void run() {
        ProviderStatisticsHolder.init();
        ProviderCapacityBucket.init();
        int i = 0;
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
            }
            Map<String, ProviderCapacityBucket> appCapacityBuckets = ProviderStatisticsHolder.getCapacityBuckets();
            Map<String, ProviderCapacityBucket> methodCapacityBuckets = ProviderStatisticsHolder
                    .getMethodCapacityBuckets();
            Map<String, ConcurrentHashMap<String, ProviderCapacityBucket>> methodAppCapacityBuckets = ProviderStatisticsHolder
                    .getMethodAppCapacityBuckets();
            ProviderCapacityBucket globalCapacityBucket = ProviderStatisticsHolder.getGlobalCapacityBucket();
            if (appCapacityBuckets != null && methodCapacityBuckets != null && methodAppCapacityBuckets != null
                    && globalCapacityBucket != null) {
                try {
                    for (String key : appCapacityBuckets.keySet()) {
                        ProviderCapacityBucket bucket = appCapacityBuckets.get(key);
                        bucket.resetRequestsInSecondCounter();
                    }
                    for (String key : methodCapacityBuckets.keySet()) {
                        ProviderCapacityBucket bucket = methodCapacityBuckets.get(key);
                        bucket.resetRequestsInSecondCounter();
                    }
                    for (String method : methodAppCapacityBuckets.keySet()) {
                        Map<String, ProviderCapacityBucket> appCapacityBucketMap = methodAppCapacityBuckets.get(method);
                        for (String app : appCapacityBucketMap.keySet()) {
                            ProviderCapacityBucket bucket = appCapacityBucketMap.get(app);
                            bucket.resetRequestsInSecondCounter();
                        }
                    }
                    globalCapacityBucket.resetRequestsInSecondCounter();

                    if (++i % 12 == 0) {
                        i = 0;
                        for (ProviderCapacityBucket bucket : appCapacityBuckets.values()) {
                            bucket.resetRequestsInMinuteCounter();
                        }
                        for (ProviderCapacityBucket bucket : methodCapacityBuckets.values()) {
                            bucket.resetRequestsInMinuteCounter();
                        }
                        for (String method : methodAppCapacityBuckets.keySet()) {
                            Map<String, ProviderCapacityBucket> appCapacityBucketMap = methodAppCapacityBuckets
                                    .get(method);
                            for (String app : appCapacityBucketMap.keySet()) {
                                ProviderCapacityBucket bucket = appCapacityBucketMap.get(app);
                                bucket.resetRequestsInMinuteCounter();
                            }
                        }
                        globalCapacityBucket.resetRequestsInMinuteCounter();
                    }
                } catch (Throwable e) {
                    logger.error("Check expired request in app statistics failed, detail[" + e.getMessage() + "].", e);
                }
            }
        }
    }
}
