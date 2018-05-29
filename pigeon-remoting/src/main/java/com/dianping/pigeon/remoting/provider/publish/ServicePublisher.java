/**
 * Dianping.com Inc.
 * Copyright (c) 2003-2013 All Rights Reserved.
 */
package com.dianping.pigeon.remoting.provider.publish;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;

import com.dianping.pigeon.config.ConfigManager;
import com.dianping.pigeon.config.ConfigManagerLoader;
import com.dianping.pigeon.log.Logger;
import com.dianping.pigeon.log.LoggerLoader;
import com.dianping.pigeon.registry.RegistryManager;
import com.dianping.pigeon.registry.exception.RegistryException;
import com.dianping.pigeon.remoting.common.util.Constants;
import com.dianping.pigeon.remoting.provider.ProviderBootStrap;
import com.dianping.pigeon.remoting.provider.Server;
import com.dianping.pigeon.remoting.provider.config.ProviderConfig;
import com.dianping.pigeon.remoting.provider.listener.HeartBeatListener;
import com.dianping.pigeon.remoting.provider.service.DisposableService;
import com.dianping.pigeon.remoting.provider.service.InitializingService;
import com.dianping.pigeon.remoting.provider.service.method.ServiceMethodFactory;
import com.dianping.pigeon.util.VersionUtils;

/**
 * @author xiangwu
 * @Sep 30, 2013
 */
public final class ServicePublisher {

    private static Logger logger = LoggerLoader.getLogger(ServicePublisher.class);

    private static ConcurrentHashMap<String, ProviderConfig<?>> serviceCache = new ConcurrentHashMap<String, ProviderConfig<?>>();

    private static ConfigManager configManager = ConfigManagerLoader.getConfigManager();

    private static ServiceChangeListener serviceChangeListener = new DefaultServiceChangeListener();

    private static boolean DEFAULT_HEARTBEAT_ENABLE = true;

    private static ConcurrentHashMap<String, Integer> serverWeightCache = new ConcurrentHashMap<String, Integer>();

    private static final int UNPUBLISH_WAITTIME = configManager.getIntValue(Constants.KEY_UNPUBLISH_WAITTIME,
            Constants.DEFAULT_UNPUBLISH_WAITTIME);

    private static final boolean THROW_EXCEPTION_IF_FORBIDDEN = configManager.getBooleanValue(
            "pigeon.publish.forbidden.throwexception", false);

    private static final boolean GROUP_FORBIDDEN = configManager.getBooleanValue("pigeon.publish.forbidden.group",
            false);

    private static final String registryBlackList = configManager.getStringValue("pigeon.registry.blacklist", "");

    private static final String registryWhiteList = configManager.getStringValue("pigeon.registry.whitelist", "");

    private static final boolean canRegisterDefault = configManager.getBooleanValue(
            "pigeon.registry.canregister.default", true);

    public static String getServiceUrlWithVersion(String url, String version) {
        String newUrl = url;
        if (!StringUtils.isBlank(version)) {
            newUrl = url + "_" + version;
        }
        return newUrl;
    }

    public static <T> void addService(ProviderConfig<T> providerConfig) throws Exception {
        if (logger.isInfoEnabled()) {
            logger.info("add service:" + providerConfig);
        }
        // 设置URL和Provider配置的映射关系
        String version = providerConfig.getVersion();
        String url = providerConfig.getUrl();
        if (StringUtils.isBlank(version)) {// default version
            serviceCache.put(url, providerConfig);
        } else {
            String urlWithVersion = getServiceUrlWithVersion(url, version);
            if (serviceCache.containsKey(url)) {
                serviceCache.put(urlWithVersion, providerConfig);
                ProviderConfig<?> providerConfigDefault = serviceCache.get(url);
                String defaultVersion = providerConfigDefault.getVersion();
                if (!StringUtils.isBlank(defaultVersion)) {
                    if (VersionUtils.compareVersion(defaultVersion, providerConfig.getVersion()) < 0) {
                        // replace existing service with this newer service as
                        // the default provider
                        serviceCache.put(url, providerConfig);
                    }
                }
            } else {
                serviceCache.put(urlWithVersion, providerConfig);
                // use this service as the default provider
                serviceCache.put(url, providerConfig);
            }
        }
        // 如果服务实现了接口 InitializingService，在这里会进行一次initialize的调用
        T service = providerConfig.getService();
        if (service instanceof InitializingService) {
            ((InitializingService) service).initialize();
        }
        // 设置URL + 方法名和方法的映射关系，用于后续调用过程中获取对应的实现和方法
        ServiceMethodFactory.init(url);
    }

    public static <T> void publishService(ProviderConfig<T> providerConfig) throws RegistryException {
        publishService(providerConfig, true);
    }

    // atom
    public static <T> void publishService(ProviderConfig<T> providerConfig, boolean forcePublish)
            throws RegistryException {
        String url = providerConfig.getUrl();
        boolean existingService = false;
        for (String key : serviceCache.keySet()) {
            ProviderConfig<?> pc = serviceCache.get(key);
            if (pc.getUrl().equals(url)) {
                existingService = true;
                break;
            }
        }
        if (logger.isInfoEnabled()) {
            logger.info("try to publish service to registry:" + providerConfig + ", existing service:"
                    + existingService);
        }
        if (existingService) {
            boolean autoPublishEnable = ConfigManagerLoader.getConfigManager().getBooleanValue(
                    Constants.KEY_AUTOPUBLISH_ENABLE, true);
            if (autoPublishEnable || forcePublish) {
                // Pigeon默认提供JettyHttpServer和NettyServer
                List<Server> servers = ProviderBootStrap.getServers(providerConfig);
                int registerCount = 0;
                for (Server server : servers) {
                    // JettyHttpServer的URL规则：%HTTP% + url
                    // NettyServer的URL规则：url
                    publishServiceToRegistry(url, server.getRegistryUrl(url), server.getPort(),
                            RegistryManager.getInstance().getGroup(url), providerConfig.isSupported());
                    registerCount++;
                }
                if (registerCount > 0) {
                    boolean isHeartbeatEnable = configManager.getBooleanValue(Constants.KEY_HEARTBEAT_ENABLE,
                            DEFAULT_HEARTBEAT_ENABLE);
                    if (isHeartbeatEnable) {
                        // 设置心跳线程
                        HeartBeatListener.registerHeartBeat(providerConfig);
                    }

                    // 基于Governor治理门户的配置推送
                    boolean isNotify = configManager
                            .getBooleanValue(Constants.KEY_NOTIFY_ENABLE, false);
                    if (isNotify && serviceChangeListener != null) {
                        /** @see com.dianping.pigeon.remoting.provider.publish.DefaultServiceChangeListener */
                        serviceChangeListener.notifyServicePublished(providerConfig);
                    }

                    // ServiceOnlineTask用于设置应用的权重，让服务在线可用
                    boolean autoRegisterEnable = ConfigManagerLoader.getConfigManager().getBooleanValue(
                            Constants.KEY_AUTOREGISTER_ENABLE, true);
                    if (autoRegisterEnable) {
                        // 不延迟设置应用在线
                        ServiceOnlineTask.start();
                    } else {
                        logger.info("auto register is disabled");
                    }

                    // 发布服务完毕
                    providerConfig.setPublished(true);
                }
            } else {
                logger.info("auto publish is disabled");
            }
        }
    }

    public static boolean isAutoPublish() {
        boolean autoPublishEnable = ConfigManagerLoader.getConfigManager().getBooleanValue(
                Constants.KEY_AUTOPUBLISH_ENABLE, true);
        boolean autoRegisterEnable = ConfigManagerLoader.getConfigManager().getBooleanValue(
                Constants.KEY_AUTOREGISTER_ENABLE, true);
        return autoPublishEnable && autoRegisterEnable;
    }

    public static void publishService(String url) throws RegistryException {
        if (logger.isInfoEnabled()) {
            logger.info("publish service:" + url);
        }
        ProviderConfig<?> providerConfig = serviceCache.get(url);
        if (providerConfig != null) {
            for (String key : serviceCache.keySet()) {
                ProviderConfig<?> pc = serviceCache.get(key);
                if (pc.getUrl().equals(url)) {
                    publishService(pc, true);
                }
            }
        }
    }

    private synchronized static <T> void publishServiceToRegistry(String url, String registryUrl, int port, String group, boolean support)
            throws RegistryException {
        String ip = configManager.getLocalIp();
        // 白名单和黑名单规则校验，验证当前应用是否允许注册服务
        if (!canRegister(ip)) {
            boolean canRegister = false;
            if (StringUtils.isNotBlank(group) && !GROUP_FORBIDDEN) {
                canRegister = true;
            }
            if (!canRegister) {
                if (THROW_EXCEPTION_IF_FORBIDDEN) {
                    throw new SecurityException("service registration of " + ip + " is not allowed!");
                } else {
                    logger.warn("service registration of " + ip + " is not allowed, url:" + registryUrl + ", port:"
                            + port + ", group:" + group);
                    return;
                }
            }
        }
        String serverAddress = ip + ":" + port;
        // 权重值，默认权重为0，可以通过属性或配置中心进行修改
        int weight = Constants.WEIGHT_INITIAL;
        // 是否自动进行注册，默认允许自动注册，如果设置为false，权重自动置0
        boolean autoRegisterEnable = ConfigManagerLoader.getConfigManager().getBooleanValue(
                Constants.KEY_AUTOREGISTER_ENABLE, true);
        if (!autoRegisterEnable) {
            weight = 0;
        }
        /*boolean enableOnlineTask = ConfigManagerLoader.getConfigManager().getBooleanValue("pigeon.online.task.enable",
                true);
        if (!enableOnlineTask) {
            weight = Constants.WEIGHT_DEFAULT;
        }*/
        // 防止同一服务重复注册，在registerService里注册权重信息时，只对 weight >= 0 的服务写权重配置
        if (serverWeightCache.containsKey(serverAddress)) {
            weight = -1;
        }
        if (logger.isInfoEnabled()) {
            logger.info("publish service to registry, url:" + registryUrl + ", port:" + port + ", group:" + group
                    + ", address:" + serverAddress + ", weight:" + weight + ", support: " + support);
        }
        // 往注册中心写权重(Weight)和服务(Service)配置
        RegistryManager.getInstance().registerService(registryUrl, group, serverAddress, weight);
        // 往注册中心写协议关系配置
        RegistryManager.getInstance().registerSupportNewProtocol(serverAddress, registryUrl, support);

        if (weight >= 0) {
            if (!serverWeightCache.containsKey(serverAddress)) {
                // 往注册中心写当前应用服务的应用名
                RegistryManager.getInstance().setServerApp(serverAddress, configManager.getAppName());
                // 往注册中心写当前应用服务的Pigeon版本号
                RegistryManager.getInstance().setServerVersion(serverAddress, VersionUtils.VERSION);
            }
            serverWeightCache.put(serverAddress, weight);
        }
    }

    public static Map<String, Integer> getServerWeight() {
        return serverWeightCache;
    }

    public synchronized static void setServerWeight(int weight) throws RegistryException {
        // 权重只能设置在 0 到 100
        if (weight < 0 || weight > 100) {
            throw new IllegalArgumentException("The weight must be within the range of 0 to 100:" + weight);
        }
        // 遍历在服务注册阶段添加的<host:port>服务
        for (String serverAddress : serverWeightCache.keySet()) {
            if (logger.isInfoEnabled()) {
                logger.info("set weight, address:" + serverAddress + ", weight:" + weight);
            }
            // 往注册中心写权重配置
            RegistryManager.getInstance().setServerWeight(serverAddress, weight);
            /** 在服务注册阶段已经设置过应用名和版本号信息? */
            if (!serverWeightCache.containsKey(serverAddress)) {
                // 往注册中心写当前应用服务的应用名
                RegistryManager.getInstance().setServerApp(serverAddress, configManager.getAppName());
                // 往注册中心写当前应用服务的Pigeon版本号
                RegistryManager.getInstance().setServerVersion(serverAddress, VersionUtils.VERSION);
            }
            serverWeightCache.put(serverAddress, weight);
        }
    }

    public synchronized static <T> void unpublishService(ProviderConfig<T> providerConfig) throws RegistryException {
        String url = providerConfig.getUrl();
        boolean existingService = false;
        for (String key : serviceCache.keySet()) {
            ProviderConfig<?> pc = serviceCache.get(key);
            if (pc.getUrl().equals(url)) {
                existingService = true;
                break;
            }
        }
        if (logger.isInfoEnabled()) {
            logger.info("try to unpublish service from registry:" + providerConfig + ", existing service:"
                    + existingService);
        }
        if (existingService) {
            List<Server> servers = ProviderBootStrap.getServers(providerConfig);
            for (Server server : servers) {
                String serverAddress = configManager.getLocalIp() + ":" + server.getPort();
                String registryUrl = server.getRegistryUrl(providerConfig.getUrl());
                // 移除注册中心的服务
                RegistryManager.getInstance().unregisterService(registryUrl,
                        RegistryManager.getInstance().getGroup(url), serverAddress);
                // unregister protocol, include http server
                // 移除注册中心的数据协议
                RegistryManager.getInstance().unregisterSupportNewProtocol(serverAddress, registryUrl,
                        providerConfig.isSupported());

                // 移除服务权重映射
                Integer weight = serverWeightCache.remove(serverAddress);
                if (weight != null) {
                    // 移除注册中心的应用名，直接删除
                    RegistryManager.getInstance().unregisterServerApp(serverAddress);
                    // 移除注册中心的Pigeon版本号，直接删除
                    RegistryManager.getInstance().unregisterServerVersion(serverAddress);
                }
            }

            boolean isHeartbeatEnable = configManager.getBooleanValue(Constants.KEY_HEARTBEAT_ENABLE,
                    DEFAULT_HEARTBEAT_ENABLE);
            if (isHeartbeatEnable) {
                // 移除心跳线程
                HeartBeatListener.unregisterHeartBeat(providerConfig);
            }

            // 如果配置了Governor治理门户，则发送移除服务请求
            boolean isNotify = configManager.getBooleanValue(Constants.KEY_NOTIFY_ENABLE, false);
            if (isNotify && serviceChangeListener != null) {
                serviceChangeListener.notifyServiceUnpublished(providerConfig);
            }
            // 服务下线
            providerConfig.setPublished(false);
            if (logger.isInfoEnabled()) {
                logger.info("unpublished service from registry:" + providerConfig);
            }
        }
    }

    public static void unpublishService(String url) throws RegistryException {
        if (logger.isInfoEnabled()) {
            logger.info("unpublish service:" + url);
        }
        ProviderConfig<?> providerConfig = serviceCache.get(url);
        if (providerConfig != null) {
            for (String key : serviceCache.keySet()) {
                ProviderConfig<?> pc = serviceCache.get(key);
                if (pc.getUrl().equals(url)) {
                    unpublishService(pc);
                }
            }
        }
    }

    public static ProviderConfig<?> getServiceConfig(String url) {
        ProviderConfig<?> providerConfig = serviceCache.get(url);
        return providerConfig;
    }

    public static void removeService(String url) throws RegistryException {
        if (logger.isInfoEnabled()) {
            logger.info("remove service:" + url);
        }
        List<String> toRemovedUrls = new ArrayList<String>();
        for (String key : serviceCache.keySet()) {
            ProviderConfig<?> pc = serviceCache.get(key);
            if (pc.getUrl().equals(url)) {
                unpublishService(pc);
                toRemovedUrls.add(key);
                Object service = pc.getService();
                if (service instanceof DisposableService) {
                    try {
                        ((DisposableService) service).destroy();
                    } catch (Throwable e) {
                        logger.warn("error while destroy service:" + url + ", caused by " + e.getMessage());
                    }
                }
            }
        }
        for (String key : toRemovedUrls) {
            serviceCache.remove(key);
        }
    }

    public static void removeAllServices() throws RegistryException {
        if (logger.isInfoEnabled()) {
            logger.info("remove all services");
        }
        unpublishAllServices();
        serviceCache.clear();
    }

    public static void unpublishAllServices() throws RegistryException {
        if (logger.isInfoEnabled()) {
            logger.info("unpublish all services");
        }
        // 设置 isStop = true
        ServiceOnlineTask.stop();
        // 设置所有服务的权重为0，下线所有服务
        setServerWeight(0);
        try {
            // 等待服务下线
            Thread.sleep(UNPUBLISH_WAITTIME);
        } catch (InterruptedException e) {
        }
        // 遍历移除发布至注册中心的服务
        for (String url : serviceCache.keySet()) {
            ProviderConfig<?> providerConfig = serviceCache.get(url);
            if (providerConfig != null) {
                // 移除服务
                unpublishService(providerConfig);
            }
        }
    }

    public static void publishAllServices() throws RegistryException {
        publishAllServices(true);
    }

    public static void publishAllServices(boolean forcePublish) throws RegistryException {
        if (logger.isInfoEnabled()) {
            logger.info("publish all services, " + forcePublish);
        }
        for (String url : serviceCache.keySet()) {
            ProviderConfig<?> providerConfig = serviceCache.get(url);
            if (providerConfig != null) {
                publishService(providerConfig, forcePublish);
            }
        }
    }

    public static Map<String, ProviderConfig<?>> getAllServiceProviders() {
        return serviceCache;
    }

    public static void notifyServiceOnline() {
        for (String url : serviceCache.keySet()) {
            ProviderConfig<?> providerConfig = serviceCache.get(url);
            if (providerConfig != null) {
                // do notify
                if (serviceChangeListener != null) {
                    serviceChangeListener.notifyServiceOnline(providerConfig);
                }
            }
        }
    }

    public static void notifyServiceOffline() {
        for (String url : serviceCache.keySet()) {
            ProviderConfig<?> providerConfig = serviceCache.get(url);
            if (providerConfig != null) {
                // do notify
                if (serviceChangeListener != null) {
                    serviceChangeListener.notifyServiceOffline(providerConfig);
                }
            }
        }
    }

    public static boolean canRegister(String ip) {
        // 白名单过滤
        String[] whiteArray = registryWhiteList.split(",");
        for (String addr : whiteArray) {
            if (StringUtils.isBlank(addr)) {
                continue;
            }
            if (ip.startsWith(addr)) {
                return true;
            }
        }
        // 黑名单过滤
        String[] blackArray = registryBlackList.split(",");
        for (String addr : blackArray) {
            if (StringUtils.isBlank(addr)) {
                continue;
            }
            if (ip.startsWith(addr)) {
                return false;
            }
        }
        return canRegisterDefault;
    }

    public static Class<?> getInterface(String url) {
        ProviderConfig<?> config = getServiceConfig(url);
        return config.getServiceInterface();
    }

}
