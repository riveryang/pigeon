/**
 * Dianping.com Inc.
 * Copyright (c) 00-0 All Rights Reserved.
 */
package com.dianping.pigeon.remoting.provider.process.threadpool;

import java.util.*;
import java.util.concurrent.*;

import com.dianping.pigeon.remoting.common.domain.MessageType;
import com.dianping.pigeon.remoting.common.monitor.trace.*;
import com.dianping.pigeon.config.ConfigManager;
import com.dianping.pigeon.remoting.common.codec.json.JacksonSerializer;
import com.dianping.pigeon.remoting.common.util.Constants;
import com.dianping.pigeon.remoting.provider.config.*;
import com.dianping.pigeon.remoting.provider.publish.ServicePublisher;
import com.dianping.pigeon.util.CollectionUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;

import com.dianping.pigeon.config.ConfigChangeListener;
import com.dianping.pigeon.config.ConfigManagerLoader;
import com.dianping.pigeon.log.Logger;
import com.dianping.pigeon.log.LoggerLoader;
import com.dianping.pigeon.remoting.common.domain.InvocationContext.TimePhase;
import com.dianping.pigeon.remoting.common.domain.InvocationContext.TimePoint;
import com.dianping.pigeon.remoting.common.domain.InvocationRequest;
import com.dianping.pigeon.remoting.common.domain.InvocationResponse;
import com.dianping.pigeon.remoting.common.exception.RejectedException;
import com.dianping.pigeon.remoting.common.process.ServiceInvocationHandler;
import com.dianping.pigeon.remoting.provider.domain.ProviderContext;
import com.dianping.pigeon.remoting.provider.process.AbstractRequestProcessor;
import com.dianping.pigeon.remoting.provider.process.ProviderProcessHandlerFactory;
import com.dianping.pigeon.remoting.provider.process.filter.GatewayProcessFilter;
import com.dianping.pigeon.remoting.provider.service.method.ServiceMethodCache;
import com.dianping.pigeon.remoting.provider.service.method.ServiceMethodFactory;
import com.dianping.pigeon.threadpool.DynamicThreadPool;
import com.dianping.pigeon.threadpool.ThreadPool;

public class RequestThreadPoolProcessor extends AbstractRequestProcessor {

    private static final Logger logger = LoggerLoader.getLogger(RequestThreadPoolProcessor.class);

    private static final ConfigManager configManager = ConfigManagerLoader.getConfigManager();

    private static volatile boolean isTrace = true;

    private volatile boolean poolConfigSwitchable = false;

    private static DynamicThreadPool sharedRequestProcessThreadPool = null;

    private static final int SLOW_POOL_CORESIZE = configManager.getIntValue(
            "pigeon.provider.pool.slow.coresize", 30);

    private static final int SLOW_POOL_MAXSIZE = configManager.getIntValue(
            "pigeon.provider.pool.slow.maxsize", 200);

    private static final int SLOW_POOL_QUEUESIZE = configManager.getIntValue(
            "pigeon.provider.pool.slow.queuesize", 500);

    private static final DynamicThreadPool slowRequestProcessThreadPool = new DynamicThreadPool(
            "Pigeon-Server-Slow-Request-Processor", SLOW_POOL_CORESIZE, SLOW_POOL_MAXSIZE,
            SLOW_POOL_QUEUESIZE, new ThreadPoolExecutor.AbortPolicy(), false, true);

    private static final ConcurrentMap<String, DynamicThreadPool> methodThreadPools = new ConcurrentHashMap<>();

    private static final ConcurrentMap<String, DynamicThreadPool> serviceThreadPools = new ConcurrentHashMap<>();

    private static final int DEFAULT_POOL_ACTIVES = configManager.getIntValue(
            "pigeon.provider.pool.actives", 60);

    private static volatile float DEFAULT_POOL_RATIO_CORE = configManager.getFloatValue(
            "pigeon.provider.pool.ratio.coresize", 3f);

    private static volatile float cancelRatio = configManager.getFloatValue(
            "pigeon.timeout.cancelratio", 1f);

    private static Map<String, String> methodPoolConfigKeys = new HashMap<String, String>();

    private static String sharedPoolCoreSizeKey = null;

    private static String sharedPoolMaxSizeKey = null;

    private static String sharedPoolQueueSizeKey = null;

    private static volatile boolean enableSlowPool = configManager.getBooleanValue(
            "pigeon.provider.pool.slow.enable", true);

    private static final JacksonSerializer jacksonSerializer = new JacksonSerializer();
    private static final String KEY_PROVIDER_POOL_CONFIG_ENABLE = "pigeon.provider.pool.config.switch";
    private static final String KEY_PROVIDER_POOL_CONFIG = "pigeon.provider.pool.config";
    private static final String KEY_PROVIDER_POOL_API_CONFIG = "pigeon.provider.pool.api.config";

    // poolName --> poolConfig
    private volatile static ConcurrentMap<String, PoolConfig> poolConfigs = Maps.newConcurrentMap();
    // api --> poolName
    private volatile static ConcurrentMap<String, String> apiPoolNameMapping = Maps.newConcurrentMap();

    static {
        if (configManager.getBooleanValue(KEY_PROVIDER_POOL_CONFIG_ENABLE, false)) {
            try {
                initPool();
            } catch (Throwable t) {
                throw new RuntimeException("failed to init pool config! please check!", t);
            }
        }
    }

    public RequestThreadPoolProcessor() {
        isTrace = configManager.getBooleanValue(Constants.KEY_PROVIDER_TRACE_ENABLE, Constants.DEFAULT_PROVIDER_TRACE_ENABLE);
        poolConfigSwitchable = configManager.getBooleanValue(KEY_PROVIDER_POOL_CONFIG_ENABLE, false);
        configManager.registerConfigChangeListener(new InnerConfigChangeListener());
    }

    private static void initPool() throws Throwable {
        String poolInfo = configManager.getStringValue(KEY_PROVIDER_POOL_CONFIG, "");
        refreshPool(poolInfo);
        String apiPoolConfig = configManager.getStringValue(KEY_PROVIDER_POOL_API_CONFIG, "");
        refreshApiPoolMapping(apiPoolConfig);
        logger.info("init pool config success!");
    }

    private static synchronized void refreshPool(String poolInfo) throws Throwable {
        if (StringUtils.isNotBlank(poolInfo)) {
            PoolConfig[] poolConfigArr = (PoolConfig[]) jacksonSerializer.toObject(PoolConfig[].class, poolInfo);
            ConcurrentMap<String, PoolConfig> newPoolConfigs = Maps.newConcurrentMap();

            for (PoolConfig poolConfig : poolConfigArr) {
                if (PoolConfigFactory.validate(poolConfig)) {
                    newPoolConfigs.put(poolConfig.getPoolName(), poolConfig);
                } else {//报异常,保持原状
                    throw new IllegalArgumentException("pool arg error! please check: " + poolConfig);
                }
            }

            if (newPoolConfigs.size() != poolConfigArr.length) {//报异常,保持原状
                throw new RuntimeException("conflict pool name exists, please check!");
            } else {
                List<PoolConfig> poolToClose = Lists.newLinkedList();
                for (Map.Entry<String, PoolConfig> oldPoolConfigEntry : poolConfigs.entrySet()) {
                    String oldPoolName = oldPoolConfigEntry.getKey();
                    PoolConfig oldPoolConfig = oldPoolConfigEntry.getValue();
                    PoolConfig newPoolConfig = newPoolConfigs.get(oldPoolName);
                    if (newPoolConfig != null) {
                        oldPoolConfig.setCorePoolSize(newPoolConfig.getCorePoolSize());
                        oldPoolConfig.setMaxPoolSize(newPoolConfig.getMaxPoolSize());
                        oldPoolConfig.setWorkQueueSize(newPoolConfig.getWorkQueueSize());
                        newPoolConfigs.put(oldPoolName, oldPoolConfig);
                    } else {
                        poolToClose.add(oldPoolConfig);
                    }
                }
                for (PoolConfig poolConfig : newPoolConfigs.values()) {
                    DynamicThreadPoolFactory.refreshThreadPool(poolConfig);
                }
                poolConfigs = newPoolConfigs;
                for (PoolConfig _poolToClose : poolToClose) {
                    DynamicThreadPoolFactory.closeThreadPool(_poolToClose);
                }
            }
            logger.info("refresh pool config success!");
        }
    }

    private static synchronized void refreshApiPoolMapping(String apiPoolName) throws Throwable {
        if (StringUtils.isNotBlank(apiPoolName)) {
            Map<String, String> _apiPoolNameMapping = (Map) jacksonSerializer.toObject(Map.class, apiPoolName);
            apiPoolNameMapping = new ConcurrentHashMap<>(_apiPoolNameMapping);
            logger.info("refresh api pool mapping success!");
        }
    }

    public void doStop() {
    }

    public Future<InvocationResponse> doProcessRequest(final InvocationRequest request,
                                                       final ProviderContext providerContext) {
        requestContextMap.put(request, providerContext);

        // 设置调用跟踪
        startMonitorData(request, providerContext);

        Callable<InvocationResponse> requestExecutor = new Callable<InvocationResponse>() {

            @Override
            public InvocationResponse call() throws Exception {
                providerContext.getTimeline().add(new TimePoint(TimePhase.T));
                try {
                    ServiceInvocationHandler invocationHandler = ProviderProcessHandlerFactory
                            .selectInvocationHandler(providerContext.getRequest().getMessageType());
                    if (invocationHandler != null) {
                        providerContext.setThread(Thread.currentThread());
                        return invocationHandler.handle(providerContext);
                    }
                } catch (Throwable t) {
                    logger.error("Process request failed with invocation handler, you should never be here.", t);
                } finally {
                    requestContextMap.remove(request);
                }
                return null;
            }
        };
        final ThreadPool pool = selectThreadPool(request);

        try {
            checkRequest(pool, request);
            providerContext.getTimeline().add(new TimePoint(TimePhase.T));
            return pool.submit(requestExecutor);
        } catch (RejectedExecutionException e) {
            requestContextMap.remove(request);
            endMonitorData(request, providerContext);
            throw new RejectedException(getProcessorStatistics(pool), e);
        }

    }

    private void startMonitorData(InvocationRequest request, ProviderContext providerContext) {
        if (isTrace) {
            if (MessageType.isService((byte) request.getMessageType())) {

                ProviderMonitorData monitorData = MonitorDataFactory.newProviderMonitorData(new ApplicationKey(request.getApp()),
                        new MethodKey(request.getServiceName(), request.getMethodName()));

                providerContext.setMonitorData(monitorData);

                monitorData.start();
            }
        }
    }

    private void endMonitorData(InvocationRequest request, ProviderContext providerContext) {
        ProviderMonitorData monitorData = (ProviderMonitorData) providerContext.getMonitorData();
        if (monitorData != null) {
            monitorData.setCallType((byte) request.getCallType());
            monitorData.setSerialize(request.getSerialize());
            monitorData.setTimeout(request.getTimeout());

            monitorData.add();

            monitorData.setIsSuccess(false);
            monitorData.complete();
        }
    }


    private void checkRequest(final ThreadPool pool, final InvocationRequest request) {
        GatewayProcessFilter.checkRequest(request);
    }

    private ThreadPool selectThreadPool(final InvocationRequest request) {
        ThreadPool pool = null;
        String serviceKey = request.getServiceName();
        String methodKey = serviceKey + "#" + request.getMethodName();

        // spring poolConfig
        pool = getConfigThreadPool(request);

        // spring actives
        // 如果设置了方法级别的线程隔离，则使用方法级别的线程池，否则选择服务级别的线程池
        if (pool == null && !CollectionUtils.isEmpty(methodThreadPools)) {
            pool = methodThreadPools.get(methodKey);
        }
        if (pool == null && !CollectionUtils.isEmpty(serviceThreadPools)) {
            pool = serviceThreadPools.get(serviceKey);
        }

        // lion poolConfig
        if (pool == null && poolConfigSwitchable && !CollectionUtils.isEmpty(apiPoolNameMapping)) {
            PoolConfig poolConfig = null;
            String poolName = apiPoolNameMapping.get(methodKey);
            if (StringUtils.isNotBlank(poolName)) { // 方法级别
                poolConfig = poolConfigs.get(poolName);
                if (poolConfig != null) {
                    pool = DynamicThreadPoolFactory.getThreadPool(poolConfig);
                }
            } else { // 服务级别
                poolName = apiPoolNameMapping.get(serviceKey);
                if (StringUtils.isNotBlank(poolName)) {
                    poolConfig = poolConfigs.get(poolName);
                    if (poolConfig != null) {
                        pool = DynamicThreadPoolFactory.getThreadPool(poolConfig);
                    }
                }
            }
        }

        // 默认方式
        if (pool == null) {
            if (enableSlowPool && requestTimeoutListener.isSlowRequest(request)) {
                pool = slowRequestProcessThreadPool;
            } else {
                pool = sharedRequestProcessThreadPool;
            }
        }

        return pool;
    }

    private ThreadPool getConfigThreadPool(InvocationRequest request) {
        String serviceName = request.getServiceName();
        String methodName = request.getMethodName();
        if (StringUtils.isNotBlank(serviceName) && StringUtils.isNotBlank(methodName)) {
            ProviderConfig providerConfig = ServicePublisher.getServiceConfig(serviceName);

            if (providerConfig != null && needStandalonePool(providerConfig)) {
                Map<String, ProviderMethodConfig> methods = providerConfig.getMethods();
                if (!CollectionUtils.isEmpty(methods)) {
                    ProviderMethodConfig methodConfig = methods.get(methodName);
                    if (methodConfig != null && methodConfig.getPoolConfig() != null) {
                        return DynamicThreadPoolFactory.getThreadPool(methodConfig.getPoolConfig());
                    }
                }

                if (providerConfig.getPoolConfig() != null) {
                    return DynamicThreadPoolFactory.getThreadPool(providerConfig.getPoolConfig());
                }
            }
        }

        return null;
    }

    @Override
    public String getProcessorStatistics() {
        Set<String> keys = Sets.newHashSet();
        StringBuilder stats = new StringBuilder();

        stats.append("[shared=").append(getDynamicThreadPoolStatistics(sharedRequestProcessThreadPool)).append("]");
        stats.append("[slow=").append(getDynamicThreadPoolStatistics(slowRequestProcessThreadPool)).append("]");

        // spring poolConfig
        if (!CollectionUtils.isEmpty(ServicePublisher.getAllServiceProviders())) {
            for (ProviderConfig<?> providerConfig : ServicePublisher.getAllServiceProviders().values()) {
                if (needStandalonePool(providerConfig)) {
                    if (!CollectionUtils.isEmpty(providerConfig.getMethods())) {
                        for (ProviderMethodConfig methodConfig : providerConfig.getMethods().values()) {
                            if (methodConfig.getPoolConfig() != null) {
                                String api = providerConfig.getUrl() + "#" + methodConfig.getName();
                                stats.append(",[").append(api).append("=").append(
                                        getDynamicThreadPoolStatistics(DynamicThreadPoolFactory.getThreadPool(methodConfig.getPoolConfig())))
                                        .append("]");
                                keys.add(api);
                            }
                        }
                    }

                    if (providerConfig.getPoolConfig() != null) {
                        stats.append(",[").append(providerConfig.getUrl()).append("=").append(
                                getDynamicThreadPoolStatistics(DynamicThreadPoolFactory.getThreadPool(providerConfig.getPoolConfig())))
                                .append("]");
                        keys.add(providerConfig.getUrl());
                    }
                }
            }
        }

        if (!CollectionUtils.isEmpty(serviceThreadPools)) {
            for (String key : serviceThreadPools.keySet()) {
                stats.append(",[").append(key).append("=").append(getDynamicThreadPoolStatistics(serviceThreadPools.get(key)))
                        .append("]");
            }
            keys.addAll(serviceThreadPools.keySet());
        }
        if (!CollectionUtils.isEmpty(methodThreadPools)) {
            for (String key : methodThreadPools.keySet()) {
                stats.append(",[").append(key).append("=").append(getDynamicThreadPoolStatistics(methodThreadPools.get(key)))
                        .append("]");
            }
            keys.addAll(methodThreadPools.keySet());
        }

        // lion poolConfig
        if (!CollectionUtils.isEmpty(apiPoolNameMapping)) {
            for (Map.Entry<String, String> entry : apiPoolNameMapping.entrySet()) {
                String api = entry.getKey();
                if (!keys.contains(api)) {
                    String poolName = entry.getValue();
                    PoolConfig poolConfig = poolConfigs.get(poolName);
                    if (poolConfig != null) {
                        stats.append(",[").append(api).append("=")
                                .append(getDynamicThreadPoolStatistics(DynamicThreadPoolFactory.getThreadPool(poolConfig))).append("]");
                    }
                }
            }
        }

        stats.append(GatewayProcessFilter.getStatistics());
        return stats.toString();
    }

    private boolean needStandalonePool(ProviderConfig<?> providerConfig) {
        return !providerConfig.isUseSharedPool();
    }

    @Override
    public synchronized <T> void addService(ProviderConfig<T> providerConfig) {
        String url = providerConfig.getUrl();
        Map<String, ProviderMethodConfig> methodConfigs = providerConfig.getMethods();
        ServiceMethodCache methodCache = ServiceMethodFactory.getServiceMethodCache(url);
        Set<String> methodNames = methodCache.getMethodMap().keySet();
        if (needStandalonePool(providerConfig)) {
            if (providerConfig.getPoolConfig() != null) { // 服务的poolConfig方式,支持方法的fallback
                DynamicThreadPoolFactory.refreshThreadPool(providerConfig.getPoolConfig());
            } else if (providerConfig.getActives() > 0 && CollectionUtils.isEmpty(methodConfigs)) { // 服务的actives方式,不支持方法的fallback,不支持动态修改
                DynamicThreadPool pool = serviceThreadPools.get(url);
                if (pool == null) {
                    int actives = providerConfig.getActives();
                    int coreSize = (int) (actives / DEFAULT_POOL_RATIO_CORE) > 0 ? (int) (actives / DEFAULT_POOL_RATIO_CORE)
                            : actives;
                    int maxSize = actives;
                    int queueSize = actives;
                    pool = new DynamicThreadPool("Pigeon-Server-Request-Processor-service", coreSize, maxSize,
                            queueSize);
                    serviceThreadPools.putIfAbsent(url, pool);
                }
            }

            if (!CollectionUtils.isEmpty(methodConfigs)) { // 方法级设置方式
                for (String name : methodNames) {
                    String key = url + "#" + name;
                    ProviderMethodConfig methodConfig = methodConfigs.get(name);
                    DynamicThreadPool pool = methodThreadPools.get(key);
                    if (methodConfig != null) {
                        if (methodConfig.getPoolConfig() != null) { // 方法poolConfig方式
                            DynamicThreadPoolFactory.refreshThreadPool(methodConfig.getPoolConfig());
                        } else if (pool == null) { // 方法actives方式
                            int actives = DEFAULT_POOL_ACTIVES;
                            if (methodConfig.getActives() > 0) {
                                actives = methodConfig.getActives();
                            }
                            int coreSize = (int) (actives / DEFAULT_POOL_RATIO_CORE) > 0 ? (int) (actives / DEFAULT_POOL_RATIO_CORE)
                                    : actives;
                            int maxSize = actives;
                            int queueSize = actives;
                            pool = new DynamicThreadPool("Pigeon-Server-Request-Processor-method", coreSize, maxSize,
                                    queueSize);
                            methodThreadPools.putIfAbsent(key, pool);
                        }
                    }
                }
            }
        }
    }

    @Override
    public String getProcessorStatistics(final InvocationRequest request) {
        ThreadPool pool = selectThreadPool(request);
        return getThreadPoolStatistics(pool);
    }

    @Override
    public String getProcessorStatistics(final ThreadPool pool) {
        return getThreadPoolStatistics(pool);
    }

    private String getDynamicThreadPoolStatistics(DynamicThreadPool pool) {
        if (pool == null) {
            return null;
        }
        ThreadPoolExecutor e = pool.getExecutor();
        String stats = String.format(
                "request pool size:%d(active:%d,core:%d,max:%d,largest:%d),task count:%d(completed:%d),queue size:%d,queue capacity:%d",
                e.getPoolSize(), e.getActiveCount(), e.getCorePoolSize(), e.getMaximumPoolSize(), e.getLargestPoolSize(),
                e.getTaskCount(), e.getCompletedTaskCount(), e.getQueue().size(), pool.getWorkQueueCapacity());
        return stats;
    }

    private String getThreadPoolStatistics(ThreadPool pool) {
        if (pool == null) {
            return null;
        }
        ThreadPoolExecutor e = pool.getExecutor();
        String stats = String.format(
                "request pool size:%d(active:%d,core:%d,max:%d,largest:%d),task count:%d(completed:%d),queue size:%d,queue remaining:%d",
                e.getPoolSize(), e.getActiveCount(), e.getCorePoolSize(), e.getMaximumPoolSize(), e.getLargestPoolSize(),
                e.getTaskCount(), e.getCompletedTaskCount(), e.getQueue().size(), e.getQueue().remainingCapacity());
        return stats;
    }

    @Override
    public synchronized <T> void removeService(ProviderConfig<T> providerConfig) {
        if (needStandalonePool(providerConfig)) {

            // spring poolConfig
            if (providerConfig.getPoolConfig() != null) {
                DynamicThreadPoolFactory.closeThreadPool(providerConfig.getPoolConfig());
            }
            Map<String, ProviderMethodConfig> methodConfigs = providerConfig.getMethods();
            if (!CollectionUtils.isEmpty(methodConfigs)) {
                for (ProviderMethodConfig methodConfig : methodConfigs.values()) {
                    if (methodConfig.getPoolConfig() != null) {
                        DynamicThreadPoolFactory.closeThreadPool(methodConfig.getPoolConfig());
                    }
                }
            }

            Set<String> toRemoveKeys = new HashSet<String>();
            for (String key : methodThreadPools.keySet()) {
                if (key.startsWith(providerConfig.getUrl() + "#")) {
                    toRemoveKeys.add(key);
                }
            }
            for (String key : toRemoveKeys) {
                ThreadPool pool = methodThreadPools.remove(key);
                if (pool != null) {
                    pool.getExecutor().shutdown();
                }
            }
            ThreadPool pool = serviceThreadPools.remove(providerConfig.getUrl());
            if (pool != null) {
                pool.getExecutor().shutdown();
            }
        }
    }

    @Override
    public boolean needCancelRequest(InvocationRequest request) {
        ThreadPool pool = selectThreadPool(request);
        return pool.getExecutor().getPoolSize() >= pool.getExecutor().getMaximumPoolSize() * cancelRatio;
    }

    @Override
    public ThreadPool getRequestProcessThreadPool() {
        return sharedRequestProcessThreadPool;
    }

    @Override
    public void doStart() {
        synchronized (RequestThreadPoolProcessor.class) {
            try {
                if (sharedRequestProcessThreadPool == null) {
                    sharedRequestProcessThreadPool = new DynamicThreadPool("Pigeon-Server-Request-Processor",
                            serverConfig.getCorePoolSize(), serverConfig.getMaxPoolSize(), serverConfig.getWorkQueueSize(),
                            new ThreadPoolExecutor.AbortPolicy(), false, false);
                } else {
                    sharedRequestProcessThreadPool.setCorePoolSize(serverConfig.getCorePoolSize());
                    sharedRequestProcessThreadPool.setMaximumPoolSize(serverConfig.getMaxPoolSize());
                    sharedRequestProcessThreadPool.setWorkQueueCapacity(serverConfig.getWorkQueueSize());
                }
            } catch (Throwable t) {
                logger.error("error serverConfig args: " + serverConfig + ", please check...", t);
                System.exit(-1);
            }
        }
    }

    public static Map<String, String> getMethodPoolConfigKeys() {
        return methodPoolConfigKeys;
    }

    public static void setSharedPoolQueueSizeKey(String sharedPoolQueueSizeKey) {
        RequestThreadPoolProcessor.sharedPoolQueueSizeKey = sharedPoolQueueSizeKey;
    }

    public static void setSharedPoolMaxSizeKey(String sharedPoolMaxSizeKey) {
        RequestThreadPoolProcessor.sharedPoolMaxSizeKey = sharedPoolMaxSizeKey;
    }

    public static void setSharedPoolCoreSizeKey(String sharedPoolCoreSizeKey) {
        RequestThreadPoolProcessor.sharedPoolCoreSizeKey = sharedPoolCoreSizeKey;
    }

    private class InnerConfigChangeListener implements ConfigChangeListener {

        @Override
        public void onKeyUpdated(String key, String value) {
            if (key.endsWith(KEY_PROVIDER_POOL_CONFIG_ENABLE)) {
                if ("true".equals(value)) {
                    try {
                        initPool();
                    } catch (Throwable t) {
                        logger.warn("failed to refresh pool config, fallback to previous settings, please check...", t);
                    }
                } else if ("false".equals(value)) {
                    try {
                        synchronized (RequestThreadPoolProcessor.class) {
                            apiPoolNameMapping = Maps.newConcurrentMap();
                            Map<String, PoolConfig> poolConfigToClose = poolConfigs;
                            poolConfigs = Maps.newConcurrentMap();
                            for (PoolConfig poolConfig : poolConfigToClose.values()) {
                                DynamicThreadPoolFactory.closeThreadPool(poolConfig);
                            }
                            logger.info("close pool config success!");
                        }
                    } catch (Throwable t) {
                        logger.warn("failed to close pool config, please check...", t);
                    }
                }
            } else if (key.endsWith(KEY_PROVIDER_POOL_CONFIG)) {
                if (configManager.getBooleanValue(KEY_PROVIDER_POOL_CONFIG_ENABLE, false)) {
                    try {
                        refreshPool(value);
                    } catch (Throwable t) {
                        logger.warn("failed to refresh pool config, fallback to previous settings, please check...", t);
                    }
                }
            } else if (key.endsWith(KEY_PROVIDER_POOL_API_CONFIG)) {
                if (configManager.getBooleanValue(KEY_PROVIDER_POOL_CONFIG_ENABLE, false)) {
                    try {
                        refreshApiPoolMapping(value);
                    } catch (Throwable t) {
                        logger.warn("failed to refresh api pool config, fallback to previous settings, please check...", t);
                    }
                }
            } else if (key.endsWith("pigeon.provider.pool.slow.enable")) {
                enableSlowPool = Boolean.valueOf(value);
                logger.info("set slow pool to " + enableSlowPool);
            } else if (key.endsWith("pigeon.timeout.cancelratio")) {
                cancelRatio = Float.valueOf(value);
                logger.info("set cancel ratio to " + cancelRatio);
            } else if (key.endsWith("pigeon.provider.pool.ratio.coresize")) {
                DEFAULT_POOL_RATIO_CORE = Float.valueOf(value);
                logger.info("set default pool ratio core to " + DEFAULT_POOL_RATIO_CORE);
            } else if (StringUtils.isNotBlank(sharedPoolCoreSizeKey) && key.endsWith(sharedPoolCoreSizeKey)) {
                int size = Integer.valueOf(value);
                if (size != sharedRequestProcessThreadPool.getExecutor().getCorePoolSize() && size >= 0) {
                    DynamicThreadPool oldPool = sharedRequestProcessThreadPool;
                    try {
                        oldPool.setCorePoolSize(size);
                        logger.info("changed shared pool, key:" + key + ", value:" + value);
                    } catch (Throwable e) {
                        logger.warn("error while changing shared pool, key:" + key + ", value:" + value, e);
                    }
                }
            } else if (StringUtils.isNotBlank(sharedPoolMaxSizeKey) && key.endsWith(sharedPoolMaxSizeKey)) {
                int size = Integer.valueOf(value);
                if (size != sharedRequestProcessThreadPool.getExecutor().getMaximumPoolSize() && size >= 0) {
                    DynamicThreadPool oldPool = sharedRequestProcessThreadPool;
                    try {
                        oldPool.setMaximumPoolSize(size);
                        logger.info("changed shared pool, key:" + key + ", value:" + value);
                    } catch (Throwable e) {
                        logger.warn("error while changing shared pool, key:" + key + ", value:" + value, e);
                    }
                }
            } else if (StringUtils.isNotBlank(sharedPoolQueueSizeKey) && key.endsWith(sharedPoolQueueSizeKey)) {
                int size = Integer.valueOf(value);
                DynamicThreadPool oldPool = sharedRequestProcessThreadPool;
                int queueSize = oldPool.getWorkQueueCapacity();
                if (size != queueSize && size >= 0) {
                    try {
                        oldPool.setWorkQueueCapacity(size);
                        logger.info("changed shared pool, key:" + key + ", value:" + value);
                    } catch (Throwable e) {
                        logger.warn("error while changing shared pool, key:" + key + ", value:" + value, e);
                    }
                }
            } else if (key.endsWith(Constants.KEY_PROVIDER_TRACE_ENABLE)) {
                try {
                    isTrace = Boolean.valueOf(value);
                } catch (RuntimeException e) {
                }
            } else if (key.endsWith(KEY_PROVIDER_POOL_CONFIG_ENABLE)) {
                poolConfigSwitchable = Boolean.valueOf(value);
            } else {
                for (String k : methodPoolConfigKeys.keySet()) {
                    String v = methodPoolConfigKeys.get(k);
                    if (key.endsWith(v)) {
                        String serviceKey = k;
                        if (StringUtils.isNotBlank(serviceKey)) {
                            DynamicThreadPool pool = null;
                            if (!CollectionUtils.isEmpty(methodThreadPools)) {
                                pool = methodThreadPools.get(serviceKey);
                                int actives = Integer.valueOf(value);
                                if (pool != null && actives != pool.getExecutor().getMaximumPoolSize()
                                        && actives >= 0) {
                                    int coreSize = (int) (actives / DEFAULT_POOL_RATIO_CORE) > 0 ? (int) (actives / DEFAULT_POOL_RATIO_CORE)
                                            : actives;
                                    int queueSize = actives;
                                    int maxSize = actives;
                                    try {
                                        pool.setCorePoolSize(coreSize);
                                        pool.setMaximumPoolSize(maxSize);
                                        pool.setWorkQueueCapacity(queueSize);
                                        logger.info("changed method pool, key:" + serviceKey + ", value:" + actives);
                                    } catch (Throwable e) {
                                        logger.warn("error while changing method pool, key:" + key + ", value:" + value, e);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        @Override
        public void onKeyAdded(String key, String value) {
        }

        @Override
        public void onKeyRemoved(String key) {
        }

    }
}
