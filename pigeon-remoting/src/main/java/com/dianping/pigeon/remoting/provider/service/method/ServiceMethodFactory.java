/**
 * Dianping.com Inc.
 * Copyright (c) 2003-2013 All Rights Reserved.
 */
package com.dianping.pigeon.remoting.provider.service.method;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.dianping.pigeon.config.ConfigChangeListener;
import com.dianping.pigeon.config.ConfigManager;
import com.dianping.pigeon.config.ConfigManagerLoader;
import org.apache.commons.lang.StringUtils;

import com.dianping.pigeon.log.Logger;
import com.dianping.pigeon.log.LoggerLoader;
import com.dianping.pigeon.remoting.common.domain.CompactRequest;
import com.dianping.pigeon.remoting.common.domain.InvocationRequest;
import com.dianping.pigeon.remoting.common.domain.ServiceId;
import com.dianping.pigeon.remoting.common.exception.BadRequestException;
import com.dianping.pigeon.remoting.provider.config.ProviderConfig;
import com.dianping.pigeon.remoting.provider.exception.InvocationFailureException;
import com.dianping.pigeon.remoting.provider.process.filter.ContextTransferProcessFilter;
import com.dianping.pigeon.remoting.provider.publish.ServicePublisher;
import com.dianping.pigeon.util.LangUtils;

public final class ServiceMethodFactory {

    private static final Logger logger = LoggerLoader.getLogger(ContextTransferProcessFilter.class);

    private static Map<String, ServiceMethodCache> methods = new ConcurrentHashMap<String, ServiceMethodCache>();

    private static Set<String> ingoreMethods = new HashSet<String>();

    private static final String KEY_COMPACT = "pigeon.invoker.request.compact";

    private static final ConfigManager configManager = ConfigManagerLoader.getConfigManager();
    private static volatile boolean isCompact = configManager.getBooleanValue(KEY_COMPACT, true);

    static {
        // 过滤 Object 里的所有方法，不开放给Pigeon作为服务方法调用
        Method[] objectMethodArray = Object.class.getMethods();
        for (Method method : objectMethodArray) {
            ingoreMethods.add(method.getName());
        }

        // 过滤 Class 里的所有的方法，不开放给Pigeon作为服务方法调用
        Method[] classMethodArray = Class.class.getMethods();
        for (Method method : classMethodArray) {
            ingoreMethods.add(method.getName());
        }

        configManager.registerConfigChangeListener(new InnerConfigChangeListener());
    }

    public static ServiceMethod getMethod(InvocationRequest request) throws InvocationFailureException {
        String serviceName = request.getServiceName();
        String methodName = request.getMethodName();
        if (StringUtils.isBlank(methodName)) {
            throw new IllegalArgumentException("method name is required");
        }
        String[] paramClassNames = request.getParamClassName();
        String version = request.getVersion();
        String newUrl = ServicePublisher.getServiceUrlWithVersion(serviceName, version);
        if (logger.isDebugEnabled()) {
            logger.debug("get method for service url:" + request);
        }
        ServiceMethodCache serviceMethodCache = getServiceMethodCache(newUrl);
        if (serviceMethodCache == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("no service found for version:" + version + ", use the default version of service:"
                        + serviceName);
            }
            serviceMethodCache = getServiceMethodCache(serviceName);
        }
        if (serviceMethodCache == null) {
            throw new BadRequestException("cannot find service for request:" + request);
        }
        return serviceMethodCache.getMethod(methodName, new ServiceParam(paramClassNames));
    }

    public static ServiceMethodCache getServiceMethodCache(String url) {
        ServiceMethodCache serviceMethodCache = methods.get(url);
        if (serviceMethodCache == null) {
            // 获取所有本地服务配置
            Map<String, ProviderConfig<?>> services = ServicePublisher.getAllServiceProviders();
            ProviderConfig<?> providerConfig = services.get(url);
            if (providerConfig != null) {
                Object service = providerConfig.getService();
                Method[] methodArray = service.getClass().getMethods();
                serviceMethodCache = new ServiceMethodCache(url, service);
                for (Method method : methodArray) {
                    // 过滤屏蔽的，不发布的服务方法
                    if (!ingoreMethods.contains(method.getName())) {
                        method.setAccessible(true);
                        serviceMethodCache.addMethod(method.getName(), new ServiceMethod(service, method));

                        if (isCompact) {
                            int id = LangUtils.hash(url + "#" + method.getName(), 0, Integer.MAX_VALUE);
                            ServiceId serviceId = new ServiceId(url, method.getName());
                            ServiceId lastId = CompactRequest.PROVIDER_ID_MAP.putIfAbsent(id, serviceId);
                            if (lastId != null && !serviceId.equals(lastId)) {
                                throw new IllegalArgumentException(
                                        "same id for service:" + url + ", method:" + method.getName());
                            }
                        }

                    }
                }
                methods.put(url, serviceMethodCache);
            }
        }
        return serviceMethodCache;
    }

    public static void init(String url) {
        getServiceMethodCache(url);
    }

    public static Map<String, ServiceMethodCache> getAllMethods() {
        return methods;
    }

    private static class InnerConfigChangeListener implements ConfigChangeListener {
        @Override
        public void onKeyUpdated(String key, String value) {
            if (key.endsWith(KEY_COMPACT)) {
                try {
                    isCompact = Boolean.valueOf(value);
                } catch (RuntimeException e) {
                    logger.warn("invalid value for key " + key, e);
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
