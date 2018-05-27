/**
 * Dianping.com Inc.
 * Copyright (c) 2003-2013 All Rights Reserved.
 */
package com.dianping.pigeon.remoting.provider.process.filter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.dianping.pigeon.monitor.Monitor;
import com.dianping.pigeon.monitor.MonitorLoader;
import com.dianping.pigeon.remoting.common.codec.json.JacksonSerializer;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import com.dianping.pigeon.log.Logger;

import com.dianping.pigeon.config.ConfigChangeListener;
import com.dianping.pigeon.config.ConfigManager;
import com.dianping.pigeon.config.ConfigManagerLoader;
import com.dianping.pigeon.log.LoggerLoader;
import com.dianping.pigeon.remoting.common.domain.Disposable;
import com.dianping.pigeon.remoting.common.domain.InvocationContext.TimePhase;
import com.dianping.pigeon.remoting.common.domain.InvocationContext.TimePoint;
import com.dianping.pigeon.remoting.common.domain.InvocationRequest;
import com.dianping.pigeon.remoting.common.domain.InvocationResponse;
import com.dianping.pigeon.remoting.common.exception.RejectedException;
import com.dianping.pigeon.remoting.common.process.ServiceInvocationFilter;
import com.dianping.pigeon.remoting.common.process.ServiceInvocationHandler;
import com.dianping.pigeon.remoting.common.util.Constants;
import com.dianping.pigeon.remoting.provider.config.ProviderConfig;
import com.dianping.pigeon.remoting.provider.domain.ProviderContext;
import com.dianping.pigeon.remoting.provider.process.statistics.ProviderStatisticsChecker;
import com.dianping.pigeon.remoting.provider.process.statistics.ProviderStatisticsHolder;
import com.dianping.pigeon.remoting.provider.publish.ServiceChangeListener;
import com.dianping.pigeon.remoting.provider.publish.ServiceChangeListenerContainer;
import com.dianping.pigeon.remoting.provider.service.method.ServiceMethodCache;
import com.dianping.pigeon.remoting.provider.service.method.ServiceMethodFactory;
import com.dianping.pigeon.threadpool.DefaultThreadPool;
import com.dianping.pigeon.threadpool.ThreadPool;
import com.dianping.pigeon.util.CollectionUtils;
import com.dianping.pigeon.util.ThreadPoolUtils;

/**
 * @author xiangwu
 * 
 */
public class GatewayProcessFilter implements ServiceInvocationFilter<ProviderContext>, Disposable {

    private static final Logger logger = LoggerLoader.getLogger(GatewayProcessFilter.class);
    private static final ConfigManager configManager = ConfigManagerLoader.getConfigManager();
    private static final Monitor monitor = MonitorLoader.getMonitor();
    private static final String KEY_APPLIMIT_ENABLE = "pigeon.provider.applimit.enable";
    private static final String KEY_METHODAPPLIMIT_ENABLE = "pigeon.provider.methodapplimit.active";
    private static final String KEY_GLOBALLIMIT_ENABLE = "pigeon.provider.globallimit.enable";
    private static final String KEY_METHODTHREADSLIMIT_ENABLE = "pigeon.provider.methodthreadslimit.enable";
    private static final String KEY_APPLIMIT = "pigeon.provider.applimit";
    private static final String KEY_METHODAPPLIMIT = "pigeon.provider.methodapplimit";
    private static final String KEY_GLOBALLIMIT = "pigeon.provider.globallimit";
    private static volatile Map<String, Long> appLimitMap = new HashMap<>();
    // api#method --> {app1 --> qpslimit, app2 --> qpslimit}
    private static volatile Map<String, Map<String, Long>> methodAppLimitMap = new HashMap<>();
    private static volatile Long globalLimit = Long.MAX_VALUE;

    private static final JacksonSerializer jacksonSerializer = new JacksonSerializer();
    private static ThreadPool statisticsCheckerPool = new DefaultThreadPool("Pigeon-Server-Statistics-Checker");
    private static final ConcurrentHashMap<String, AtomicInteger> methodActives = new ConcurrentHashMap<String, AtomicInteger>();
    private static final AtomicInteger total = new AtomicInteger();
    private static final int MAX_THREADS = ConfigManagerLoader.getConfigManager()
            .getIntValue("pigeon.provider.pool.method.maxthreads", 100);
    private static volatile boolean enableMethodThreadsLimit = configManager
            .getBooleanValue(KEY_METHODTHREADSLIMIT_ENABLE, true);
    private static volatile boolean enableAppLimit = configManager.getBooleanValue(KEY_APPLIMIT_ENABLE, false);
    private static volatile boolean enableMethodAppLimit = configManager.getBooleanValue(KEY_METHODAPPLIMIT_ENABLE,
            false);
    private static volatile boolean enableGlobalLimit = configManager.getBooleanValue(KEY_GLOBALLIMIT_ENABLE, false);

    static {
        // 全局限流规则
        String globalLimitConfig = configManager.getStringValue(KEY_GLOBALLIMIT);
        parseGlobalLimitConfig(globalLimitConfig);
        // 方法级限流规则
        String methodAppLimitConfig = configManager.getStringValue(KEY_METHODAPPLIMIT);
        parseMethodAppLimitConfig(methodAppLimitConfig);
        // 应用级限流规则
        String appLimitConfig = configManager.getStringValue(KEY_APPLIMIT);
        parseAppLimitConfig(appLimitConfig);
        // 配置变更监听（如果配置了配置中心）
        configManager.registerConfigChangeListener(new InnerConfigChangeListener());
        // 流量计数器定时重置线程，用于清理一定时间内的调用统计数，默认执行时间间隔是5000毫秒
        ProviderStatisticsChecker appStatisticsChecker = new ProviderStatisticsChecker();
        statisticsCheckerPool.execute(appStatisticsChecker);
        // 服务变更监听，新建服务时会设置服务方法和计数器映射
        ServiceChangeListenerContainer.addServiceChangeListener(new InnerServiceChangeListener());
    }

    public void destroy() throws Exception {
        // 关闭线程池
        ThreadPoolUtils.shutdown(statisticsCheckerPool.getExecutor());
    }

    private static void parseGlobalLimitConfig(String globalLimitConfig) {
        if (StringUtils.isNotBlank(globalLimitConfig)) {
            try {
                Long _globalLimit = Long.parseLong(globalLimitConfig);
                if (_globalLimit >= 0) {
                    globalLimit = _globalLimit;
                }
            } catch (Throwable t) {
                logger.error("error while parsing global limit configuration:" + globalLimitConfig, t);
            }
        } else {
            globalLimit = Long.MAX_VALUE;
        }
    }

    private static void parseMethodAppLimitConfig(String methodAppLimitConfig) {
        if (StringUtils.isNotBlank(methodAppLimitConfig)) {
            Map<String, Map<String, Long>> map;
            try {
                map = (HashMap) jacksonSerializer.toObject(HashMap.class, methodAppLimitConfig);
                for (Map<String, Long> appLimitMap : map.values()) {
                    for (String app : new HashSet<String>(appLimitMap.keySet())) {
                        Long limit = Long.parseLong("" + appLimitMap.get(app));
                        appLimitMap.put(app, limit);
                    }
                }
                methodAppLimitMap = map;
            } catch (Throwable t) {
                logger.error("error while parsing method app limit configuration:" + methodAppLimitConfig, t);
            }
        } else {
            methodAppLimitMap = new HashMap<>();
        }
    }

    private static void parseAppLimitConfig(String appLimitConfig) {
        if (StringUtils.isNotBlank(appLimitConfig)) {
            Map<String, Long> map = new HashMap<>();
            try {
                String[] appLimitConfigPair = appLimitConfig.split(",");
                for (String str : appLimitConfigPair) {
                    if (StringUtils.isNotBlank(str)) {
                        String[] pair = str.split(":");
                        if (pair != null && pair.length == 2) {
                            map.put(pair[0], Long.valueOf(pair[1]));
                        }
                    }
                }
                appLimitMap = map;
            } catch (RuntimeException e) {
                logger.error("error while parsing app limit configuration:" + appLimitConfig, e);
            }
        } else {
            appLimitMap = new HashMap<>();
        }
    }

    @Override
    public InvocationResponse invoke(ServiceInvocationHandler handler, ProviderContext invocationContext)
            throws Throwable {
        invocationContext.getTimeline().add(new TimePoint(TimePhase.G));
        InvocationRequest request = invocationContext.getRequest();
        String fromApp = request.getApp();
        InvocationResponse response = null;
        final String requestMethod = request.getServiceName() + "#" + request.getMethodName();
        try {
            ProviderStatisticsHolder.flowIn(request);
            if (Constants.MESSAGE_TYPE_SERVICE == request.getMessageType()) {
                if (enableMethodThreadsLimit) {
                    incrementRequest(requestMethod);
                }

                if (enableMethodAppLimit && methodAppLimitMap.containsKey(requestMethod)
                        && StringUtils.isNotBlank(fromApp)) {
                    Long limit = methodAppLimitMap.get(requestMethod).get(fromApp);

                    if (limit != null && limit >= 0) {
                        long requests = ProviderStatisticsHolder.getMethodAppCapacityBucket(request)
                                .getRequestsInCurrentSecond();
                        if (requests + 1 > limit) {
                            monitor.logEvent("PigeonService.methodAppLimit", fromApp + ":" + limit, "");
                            throw new RejectedException(
                                    String.format("Max requests limit %s reached for request %s from app:%s", limit,
                                            requestMethod, fromApp));
                        }
                    }
                }

                if (enableAppLimit && StringUtils.isNotBlank(fromApp) && appLimitMap.containsKey(fromApp)) {
                    Long limit = appLimitMap.get(fromApp);
                    if (limit >= 0) {
                        long requests = ProviderStatisticsHolder.getCapacityBucket(request)
                                .getRequestsInCurrentSecond();
                        if (requests + 1 > limit) {
                            monitor.logEvent("PigeonService.appLimit", fromApp + ":" + limit, "");
                            throw new RejectedException(String
                                    .format("Max requests limit %s reached for request from app:%s", limit, fromApp));
                        }
                    }
                }

                if (enableGlobalLimit) {
                    Long limit = globalLimit;
                    if (limit >= 0) {
                        long requests = ProviderStatisticsHolder.getGlobalCapacityBucket().getRequestsInCurrentSecond();
                        if (requests + 1 > limit) {
                            monitor.logEvent("PigeonService.globalLimit", fromApp + ":" + limit, "");
                            throw new RejectedException(
                                    String.format("Max requests limit %s reached for global limitation", limit));
                        }
                    }
                }
            }
            response = handler.handle(invocationContext);
            return response;
        } finally {
            if (Constants.MESSAGE_TYPE_SERVICE == request.getMessageType() && enableMethodThreadsLimit) {
                decrementRequest(requestMethod);
            }
            if (!(Constants.REPLY_MANUAL || invocationContext.isAsync())) {
                ProviderStatisticsHolder.flowOut(request);
            }
        }
    }

    public static String getStatistics() {
        StringBuilder stats = new StringBuilder();
        if (!CollectionUtils.isEmpty(methodActives)) {
            stats.append(",[method actives=[");
            for (String key : methodActives.keySet()) {
                stats.append("[").append(key).append("=").append(methodActives.get(key)).append("]");
            }
            stats.append("]");
        }
        return stats.toString();
    }

    public static void checkRequest(final InvocationRequest request) {
        if (Constants.MESSAGE_TYPE_SERVICE == request.getMessageType() && enableMethodThreadsLimit) {
            final String requestMethod = request.getServiceName() + "#" + request.getMethodName();
            AtomicInteger count = methodActives.get(requestMethod);
            if (count != null) {
                int limit = getMaxThreadsForMethod(requestMethod, count.get());
                if (count.get() > limit) {
                    throw new RejectedException(
                            String.format("Reached the maximum limit %s for method: %s, current: %s", limit,
                                    requestMethod, count.get()));
                }
            }
        }
    }

    private static int getMaxThreadsForMethod(String requestMethod, int requestMethodThreadCount) {
        int totalThreads = total.get();
        int limit = MAX_THREADS > totalThreads ? MAX_THREADS - totalThreads + requestMethodThreadCount
                : requestMethodThreadCount;
        if (limit > MAX_THREADS - 20) {
            limit = MAX_THREADS - 20;
        }
        return limit;
    }

    private static void incrementRequest(String requestMethod) {
        total.incrementAndGet();
        AtomicInteger count = methodActives.get(requestMethod);
        if (count != null) {
            int limit = getMaxThreadsForMethod(requestMethod, count.get());
            if (count.incrementAndGet() > limit) {
                throw new RejectedException(String.format("Reached the maximum limit %s for method: %s, current: %s",
                        limit, requestMethod, count.get()));
            }
        }
    }

    private static void decrementRequest(String requestMethod) {
        total.decrementAndGet();
        AtomicInteger count = methodActives.get(requestMethod);
        if (count != null) {
            count.decrementAndGet();
        }
    }

    private static class InnerConfigChangeListener implements ConfigChangeListener {

        @Override
        public void onKeyUpdated(String key, String value) {
            try {
                if (key.endsWith(KEY_APPLIMIT)) {
                    parseAppLimitConfig(value);
                } else if (key.endsWith(KEY_METHODAPPLIMIT)) {
                    parseMethodAppLimitConfig(value);
                } else if (key.endsWith(KEY_APPLIMIT_ENABLE)) {
                    enableAppLimit = Boolean.valueOf(value);
                } else if (key.endsWith(KEY_METHODAPPLIMIT_ENABLE)) {
                    enableMethodAppLimit = Boolean.valueOf(value);
                } else if (key.endsWith(KEY_METHODTHREADSLIMIT_ENABLE)) {
                    enableMethodThreadsLimit = Boolean.valueOf(value);
                } else if (key.endsWith(KEY_GLOBALLIMIT_ENABLE)) {
                    enableGlobalLimit = Boolean.valueOf(value);
                } else if (key.endsWith(KEY_GLOBALLIMIT)) {
                    parseGlobalLimitConfig(value);
                }
            } catch (Throwable t) {
                logger.warn("invalid value for key " + key, t);
            }
        }

        @Override
        public void onKeyAdded(String key, String value) {

        }

        @Override
        public void onKeyRemoved(String key) {

        }

    }

    private static class InnerServiceChangeListener implements ServiceChangeListener {

        @Override
        public void notifyServicePublished(ProviderConfig<?> providerConfig) {
        }

        @Override
        public void notifyServiceUnpublished(ProviderConfig<?> providerConfig) {
        }

        @Override
        public void notifyServiceOnline(ProviderConfig<?> providerConfig) {
        }

        @Override
        public void notifyServiceOffline(ProviderConfig<?> providerConfig) {
        }

        @Override
        public void notifyServiceAdded(ProviderConfig<?> providerConfig) {
            String url = providerConfig.getUrl();
            ServiceMethodCache methodCache = ServiceMethodFactory.getServiceMethodCache(url);
            Set<String> methodNames = methodCache.getMethodMap().keySet();
            for (String method : methodNames) {
                methodActives.put(url + "#" + method, new AtomicInteger());
            }
        }

        @Override
        public void notifyServiceRemoved(ProviderConfig<?> providerConfig) {
            String url = providerConfig.getUrl();
            ServiceMethodCache methodCache = ServiceMethodFactory.getServiceMethodCache(url);
            Set<String> methodNames = methodCache.getMethodMap().keySet();
            for (String method : methodNames) {
                methodActives.remove(url + "#" + method);
            }
        }

    }
}
