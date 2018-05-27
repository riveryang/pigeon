package com.dianping.pigeon.remoting.provider.process.statistics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.dianping.pigeon.log.Logger;

import com.dianping.pigeon.config.ConfigManagerLoader;
import com.dianping.pigeon.log.LoggerLoader;
import com.dianping.pigeon.remoting.common.domain.InvocationRequest;
import com.dianping.pigeon.remoting.common.util.Constants;

public final class ProviderStatisticsHolder {

    private static final Logger logger = LoggerLoader.getLogger(ProviderStatisticsHolder.class);

    private static ConcurrentHashMap<String, ProviderCapacityBucket> appCapacityBuckets = new ConcurrentHashMap<String, ProviderCapacityBucket>();

    private static ConcurrentHashMap<String, ProviderCapacityBucket> methodCapacityBuckets = new ConcurrentHashMap<String, ProviderCapacityBucket>();

    private static ConcurrentHashMap<String, ConcurrentHashMap<String, ProviderCapacityBucket>> methodAppCapacityBuckets = new ConcurrentHashMap<>();

    private static final ProviderCapacityBucket globalCapacityBucket = new ProviderCapacityBucket(null);

    public static final boolean statEnable = ConfigManagerLoader.getConfigManager()
            .getBooleanValue("pigeon.providerstat.enable", true);

    public static void init() {
    }

    public static Map<String, ProviderCapacityBucket> getCapacityBuckets() {
        return appCapacityBuckets;
    }

    public static ConcurrentHashMap<String, ProviderCapacityBucket> getMethodCapacityBuckets() {
        return methodCapacityBuckets;
    }

    public static ConcurrentHashMap<String, ConcurrentHashMap<String, ProviderCapacityBucket>> getMethodAppCapacityBuckets() {
        return methodAppCapacityBuckets;
    }

    public static ProviderCapacityBucket getMethodAppCapacityBucket(InvocationRequest request) {
        final String requestMethod = request.getServiceName() + "#" + request.getMethodName();
        ConcurrentHashMap<String, ProviderCapacityBucket> appBarrelMap = methodAppCapacityBuckets.get(requestMethod);
        if (appBarrelMap == null) {
            ConcurrentHashMap<String, ProviderCapacityBucket> newAppBarrelMap = new ConcurrentHashMap<>();
            appBarrelMap = methodAppCapacityBuckets.putIfAbsent(requestMethod, newAppBarrelMap);
            if (appBarrelMap == null) {
                appBarrelMap = newAppBarrelMap;
            }
        }

        String fromApp = request.getApp();
        if (fromApp == null) {
            fromApp = "";
        }

        ProviderCapacityBucket barrel = appBarrelMap.get(fromApp);
        if (barrel == null) {
            ProviderCapacityBucket newBarrel = new ProviderCapacityBucket(fromApp);
            barrel = appBarrelMap.putIfAbsent(fromApp, newBarrel);
            if (barrel == null) {
                barrel = newBarrel;
            }
        }

        return barrel;
    }

    public static ProviderCapacityBucket getCapacityBucket(InvocationRequest request) {
        String fromApp = request.getApp();
        if (fromApp == null) {
            fromApp = "";
        }
        ProviderCapacityBucket barrel = appCapacityBuckets.get(fromApp);
        if (barrel == null) {
            ProviderCapacityBucket newBarrel = new ProviderCapacityBucket(fromApp);
            barrel = appCapacityBuckets.putIfAbsent(fromApp, newBarrel);
            if (barrel == null) {
                barrel = newBarrel;
            }
        }
        return barrel;
    }

    public static ProviderCapacityBucket getCapacityBucket(String requestMethod) {
        ProviderCapacityBucket barrel = methodCapacityBuckets.get(requestMethod);

        if (barrel == null) {
            ProviderCapacityBucket newBarrel = new ProviderCapacityBucket(requestMethod);
            barrel = methodCapacityBuckets.putIfAbsent(requestMethod, newBarrel);
            if (barrel == null) {
                barrel = newBarrel;
            }
        }

        return barrel;
    }

    public static void flowIn(InvocationRequest request) {
        if (checkRequestNeedStat(request)) {
            // app level
            ProviderCapacityBucket barrel = getCapacityBucket(request);
            if (barrel != null) {
                barrel.flowIn(request);
            }

            // method level
            final String requestMethod = request.getServiceName() + "#" + request.getMethodName();
            ProviderCapacityBucket methodBarrel = getCapacityBucket(requestMethod);
            if (methodBarrel != null) {
                methodBarrel.flowIn(request);
            }

            // method app level
            ProviderCapacityBucket methodAppBarrel = getMethodAppCapacityBucket(request);
            if (methodAppBarrel != null) {
                methodAppBarrel.flowIn(request);
            }

            // global level
            globalCapacityBucket.flowIn(request);
        }
    }

    public static void flowOut(InvocationRequest request) {
        if (checkRequestNeedStat(request)) {
            // app level
            ProviderCapacityBucket barrel = getCapacityBucket(request);
            if (barrel != null) {
                barrel.flowOut(request);
            }

            // method level
            final String requestMethod = request.getServiceName() + "#" + request.getMethodName();
            ProviderCapacityBucket methodBarrel = getCapacityBucket(requestMethod);
            if (methodBarrel != null) {
                methodBarrel.flowOut(request);
            }

            // method app level
            ProviderCapacityBucket methodAppBarrel = getMethodAppCapacityBucket(request);
            if (methodAppBarrel != null) {
                methodAppBarrel.flowOut(request);
            }

            // global level
            globalCapacityBucket.flowOut(request);
        }
    }

    public static ProviderCapacityBucket getGlobalCapacityBucket() {
        return globalCapacityBucket;
    }

    public static boolean checkRequestNeedStat(InvocationRequest request) {
        if (request == null || request.getMessageType() != Constants.MESSAGE_TYPE_SERVICE) {
            return false;
        }
        return statEnable;
    }

    public static void removeCapacityBucket(String fromApp) {
        appCapacityBuckets.remove(fromApp);
    }
}
