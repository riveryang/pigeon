/**
 * Dianping.com Inc.
 * Copyright (c) 2003-2013 All Rights Reserved.
 */
package com.dianping.pigeon.remoting.http.provider;

import java.util.List;
import java.util.Random;

import com.dianping.pigeon.log.LoggerLoader;

import com.dianping.pigeon.log.Logger;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.thread.QueuedThreadPool;

import com.dianping.pigeon.config.ConfigManager;
import com.dianping.pigeon.config.ConfigManagerLoader;
import com.dianping.pigeon.extension.ExtensionLoader;
import com.dianping.pigeon.remoting.common.domain.Disposable;
import com.dianping.pigeon.remoting.common.util.Constants;
import com.dianping.pigeon.remoting.http.HttpUtils;
import com.dianping.pigeon.remoting.provider.AbstractServer;
import com.dianping.pigeon.remoting.provider.config.ProviderConfig;
import com.dianping.pigeon.remoting.provider.config.ServerConfig;
import com.dianping.pigeon.util.NetUtils;

public class JettyHttpServer extends AbstractServer implements Disposable {

    protected final Logger logger = LoggerLoader.getLogger(this.getClass());
    private Server server;
    private int port;
    private volatile boolean started = false;
    private final ConfigManager configManager = ConfigManagerLoader.getConfigManager();
    private final int minThreads = configManager.getIntValue("pigeon.provider.http.minthreads", 2);
    private final int maxThreads = configManager.getIntValue("pigeon.provider.http.maxthreads", 300);
    private final Random random = new Random();

    public JettyHttpServer() {
    }

    @Override
    public void destroy() throws Exception {
        this.stop();
    }

    @Override
    public boolean support(ServerConfig serverConfig) {
        if (serverConfig.getProtocol().equals(this.getProtocol())) {
            return true;
        }
        return false;
    }

    private Server newServer(ServerConfig serverConfig) {
        this.port = NetUtils.getAvailablePort(this.port);

        DispatcherServlet.addHttpHandler(port, new HttpServerHandler(this));
        QueuedThreadPool threadPool = new QueuedThreadPool();
        threadPool.setDaemon(true);
        threadPool.setMaxThreads(maxThreads);
        threadPool.setMinThreads(minThreads);
        Server server = new Server(port);
        server.setThreadPool(threadPool);
        Context context = new Context(Context.SESSIONS);
        context.setContextPath("/");
        server.addHandler(context);
        // 本地调试service时通过此路由进入DispatchServlet
        context.addServlet(new ServletHolder(new DispatcherServlet()), "/service");
        List<JettyHttpServerProcessor> processors = ExtensionLoader.getExtensionList(JettyHttpServerProcessor.class);
        if (processors != null) {
            for (JettyHttpServerProcessor processor : processors) {
                processor.preStart(serverConfig, server, context);
            }
        }
        return server;
    }

    @Override
    public void doStart(ServerConfig serverConfig) {
        int retries = 0;
        this.port = serverConfig.getHttpPort();
        while (!started) {
            server = newServer(serverConfig);
            retries++;
            try {
                // 启动Jetty
                server.start();
                serverConfig.setIp(configManager.getLocalIp());
                serverConfig.setHttpPort(this.port);
                started = true;
            } catch (Throwable e) {
                if (retries > 5) {
                    this.port += random.nextInt(20);
                    try {
                        Thread.sleep(random.nextInt(100));
                    } catch (InterruptedException e1) {
                        //
                    }
                    while (!started) {
                        server = newServer(serverConfig);
                        retries++;
                        try {
                            server.start();
                            serverConfig.setIp(configManager.getLocalIp());
                            serverConfig.setHttpPort(this.port);
                            started = true;
                        } catch (Throwable e1) {
                            if (retries > 10) {
                                throw new IllegalStateException("failed to start jetty server on " + this.port
                                        + ", cause: " + e1.getMessage(), e1);
                            }
                        }
                    }
                } else {
                    this.port ++;
                    try {
                        Thread.sleep(random.nextInt(100));
                    } catch (InterruptedException e1) {
                        //
                    }
                }
            }
        }
    }

    @Override
    public void doStop() {
        if (server != null) {
            try {
                server.stop();
            } catch (Throwable e) {
                logger.warn(e.getMessage(), e);
            }
        }
    }

    @Override
    public <T> void doAddService(ProviderConfig<T> providerConfig) {
    }

    @Override
    public <T> void doRemoveService(ProviderConfig<T> providerConfig) {
    }

    @Override
    public String toString() {
        return "http server-" + port;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public String getRegistryUrl(String url) {
        return HttpUtils.getHttpServiceUrl(url);
    }

    @Override
    public List<String> getInvokerMetaInfo() {
        return null;
    }

    @Override
    public boolean isStarted() {
        return started;
    }

    @Override
    public String getProtocol() {
        return Constants.PROTOCOL_HTTP;
    }
}
