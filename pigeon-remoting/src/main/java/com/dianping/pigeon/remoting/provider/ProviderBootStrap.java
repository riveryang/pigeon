/**
 * Dianping.com Inc.
 * Copyright (c) 2003-2013 All Rights Reserved.
 */
package com.dianping.pigeon.remoting.provider;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dianping.pigeon.extension.ExtensionLoader;
import com.dianping.pigeon.log.Logger;
import com.dianping.pigeon.log.LoggerLoader;
import com.dianping.pigeon.registry.RegistryManager;
import com.dianping.pigeon.registry.exception.RegistryException;
import com.dianping.pigeon.remoting.common.codec.SerializerFactory;
import com.dianping.pigeon.remoting.common.util.Constants;
import com.dianping.pigeon.remoting.provider.config.ProviderConfig;
import com.dianping.pigeon.remoting.provider.config.ServerConfig;
import com.dianping.pigeon.remoting.provider.listener.ShutdownHookListener;
import com.dianping.pigeon.remoting.provider.process.ProviderProcessHandlerFactory;
import com.dianping.pigeon.remoting.provider.publish.ServicePublisher;
import com.dianping.pigeon.util.ClassUtils;
import com.dianping.pigeon.util.NetUtils;
import com.dianping.pigeon.util.VersionUtils;

public final class ProviderBootStrap {

    private static Logger logger = LoggerLoader.getLogger(ServicePublisher.class);
    static Server httpServer = null;
    static volatile Map<String, Server> serversMap = new HashMap<String, Server>();
    static volatile boolean isInitialized = false;
    static Date startTime = new Date();

    public static Date getStartTime() {
        return startTime;
    }

    /**
     * ProviderBootstrap初始化过程.
     */
    public static void init() {
        if (!isInitialized) {
            synchronized (ProviderBootStrap.class) {
                if (!isInitialized) {
                    // 1. 初始化事件处理器初始化
                    ProviderProcessHandlerFactory.init();
                    // 2. 序列化工具初始化
                    SerializerFactory.init();
                    ClassUtils.loadClasses("com.dianping.pigeon");
                    // 3. 注册ShutdownHook，保证服务优雅停止
                    Thread shutdownHook = new Thread(new ShutdownHookListener());
                    shutdownHook.setDaemon(true);
                    shutdownHook.setPriority(Thread.MAX_PRIORITY);
                    Runtime.getRuntime().addShutdownHook(shutdownHook);
                    ServerConfig config = new ServerConfig();
                    config.setProtocol(Constants.PROTOCOL_HTTP);
                    // 4. 初始化注册中心管理器
                    RegistryManager.getInstance();
                    /**
                     * 通过SPI获取Server实现，在Pigeon中默认实现了JettyHttpServer和NettyServer
                     * 获取Server对象时会自动进行对象初始化
                     * NettyServer在这个时候也进行了初始化工作
                     * @see com.dianping.pigeon.remoting.netty.provider.NettyServer
                     */
                    List<Server> servers = ExtensionLoader.getExtensionList(Server.class);
                    for (Server server : servers) {
                        if (!server.isStarted()) {
                            if (server.support(config)) {
                                // 5. 启动JettyHttpServer
                                server.start(config);
                                // 6. 向注册中心注册Console客户端，注册内容: /DP/CONSOLE/host:port --> null
                                registerConsoleServer(config);
                                // 7. 初始化RegistryConfig，从注册中心获取配置: /pigeon/config/host
                                initRegistryConfig(config);

                                httpServer = server;
                                serversMap.put(server.getProtocol() + server.getPort(), server);
                                logger.warn("pigeon " + server + "[version:" + VersionUtils.VERSION + "] has been started");
                            }
                        }
                    }
                    isInitialized = true;
                }
            }
        }
    }

    public static ServerConfig startup(ProviderConfig<?> providerConfig) {
        // 最初构建的配置
        ServerConfig serverConfig = providerConfig.getServerConfig();
        if (serverConfig == null) {
            throw new IllegalArgumentException("server config is required");
        }
        Server server = serversMap.get(serverConfig.getProtocol() + serverConfig.getPort());
        if (server != null) {
            server.addService(providerConfig);
            return server.getServerConfig();
        } else {
            synchronized (ProviderBootStrap.class) {
                List<Server> servers = ExtensionLoader.newExtensionList(Server.class);
                for (Server s : servers) {
                    if (!s.isStarted()) {
                        // 默认的ServiceConfig的protocol是default，这里初始化的是NettyServer
                        if (s.support(serverConfig)) {
                            // 启动Server
                            s.start(serverConfig);
                            // 主要为服务绑定线程池和限流计数器
                            s.addService(providerConfig);
                            serversMap.put(s.getProtocol() + serverConfig.getPort(), s);
                            logger.warn("pigeon " + s + "[version:" + VersionUtils.VERSION + "] has been started");
                            break;
                        }
                    }
                }
                server = serversMap.get(serverConfig.getProtocol() + serverConfig.getPort());
                if (server != null) {
                    // 预先为共享线程池开启所有线程，避免后期请求时再进行线程创建，出现一定量的性能损耗
                    server.getRequestProcessor().getRequestProcessThreadPool().prestartAllCoreThreads();
                    return server.getServerConfig();
                }
                return null;
            }
        }
    }

    public static void shutdown() {
        for (Server server : serversMap.values()) {
            if (server != null) {
                logger.info("start to stop " + server);
                try {
                    // 移除Console
                    unregisterConsoleServer(server.getServerConfig());
                    // 停Server
                    server.stop();
                } catch (Throwable e) {
                }
                if (logger.isInfoEnabled()) {
                    logger.info(server + " has been shutdown");
                }
            }
        }
        try {
            // 销毁事件处理器
            ProviderProcessHandlerFactory.destroy();
        } catch (Throwable e) {
        }
    }

    public static List<Server> getServers(ProviderConfig<?> providerConfig) {
        List<Server> servers = new ArrayList<Server>();
        servers.add(httpServer);
        String protocol = providerConfig.getServerConfig().getProtocol();
        int port = providerConfig.getServerConfig().getPort();
        servers.add(serversMap.get(protocol + port));

        return servers;
    }

    public static Map<String, Server> getServersMap() {
        return serversMap;
    }

    public static Server getHttpServer() {
        return httpServer;
    }

    private static void initRegistryConfig(ServerConfig config) {
        try {
            RegistryManager.getInstance().initRegistryConfig(config.getIp());
        } catch (RegistryException e) {
            logger.warn("failed to init registry config, set config to blank, please check!", e);
        }
    }

    public static void registerConsoleServer(ServerConfig config) {
        RegistryManager.getInstance().setConsoleAddress(NetUtils.toAddress(config.getIp(), config.getHttpPort()));
    }

    public static void unregisterConsoleServer(ServerConfig config) {
        // 只有JettyHttpServer进行了注册，删除Http协议的Console信息
        if (Constants.PROTOCOL_HTTP.equals(config.getProtocol())) {
            RegistryManager.getInstance().unregisterConsoleAddress(NetUtils.toAddress(config.getIp(), config.getHttpPort()));
        }
    }

}
