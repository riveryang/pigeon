/**
 * Dianping.com Inc.
 * Copyright (c) 2003-2013 All Rights Reserved.
 */
package com.dianping.pigeon.remoting.invoker;

import com.dianping.pigeon.config.ConfigManager;
import com.dianping.pigeon.config.ConfigManagerLoader;
import com.dianping.pigeon.domain.HostInfo;
import com.dianping.pigeon.log.Logger;
import com.dianping.pigeon.log.LoggerLoader;
import com.dianping.pigeon.registry.RegistryManager;
import com.dianping.pigeon.registry.config.RegistryConfig;
import com.dianping.pigeon.registry.listener.*;
import com.dianping.pigeon.remoting.ServiceFactory;
import com.dianping.pigeon.remoting.common.domain.Disposable;
import com.dianping.pigeon.remoting.common.domain.InvocationRequest;
import com.dianping.pigeon.remoting.common.util.Utils;
import com.dianping.pigeon.remoting.invoker.config.InvokerConfig;
import com.dianping.pigeon.remoting.invoker.domain.ConnectInfo;
import com.dianping.pigeon.remoting.invoker.exception.ServiceUnavailableException;
import com.dianping.pigeon.remoting.invoker.listener.ClusterListenerManager;
import com.dianping.pigeon.remoting.invoker.listener.DefaultClusterListener;
import com.dianping.pigeon.remoting.invoker.listener.ProviderAvailableListener;
import com.dianping.pigeon.remoting.invoker.route.DefaultRouteManager;
import com.dianping.pigeon.remoting.invoker.route.RouteManager;
import com.dianping.pigeon.threadpool.DefaultThreadFactory;
import com.dianping.pigeon.threadpool.DefaultThreadPool;
import com.dianping.pigeon.threadpool.ThreadPool;
import com.dianping.pigeon.util.ThreadPoolUtils;
import org.apache.commons.lang.StringUtils;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;

public class ClientManager {

    private static final Logger logger = LoggerLoader.getLogger(ClientManager.class);

    private ClusterListenerManager clusterListenerManager = ClusterListenerManager.getInstance();

    private DefaultClusterListener clusterListener;

    private ProviderAvailableListener providerAvailableListener;

    private RouteManager routerManager = DefaultRouteManager.INSTANCE;

    private ConfigManager configManager = ConfigManagerLoader.getConfigManager();

    private ServiceProviderChangeListener providerChangeListener = new InnerServiceProviderChangeListener();

    private static ExecutorService providerAvailableThreadPool = Executors.newFixedThreadPool(1,
            new DefaultThreadFactory("Pigeon-Client-ProviderAvailable-ThreadPool"));

    private static int registerPoolCoreSize = ConfigManagerLoader.getConfigManager()
            .getIntValue("pigeon.invoker.registerpool.coresize", 10);

    private static int registerPoolMaxSize = ConfigManagerLoader.getConfigManager()
            .getIntValue("pigeon.invoker.registerpool.maxsize", 30);

    private static int registerPoolQueueSize = ConfigManagerLoader.getConfigManager()
            .getIntValue("pigeon.invoker.registerpool.queuesize", 50);

    private static ThreadPool registerThreadPool = new DefaultThreadPool("Pigeon-Client-Register-Pool",
            registerPoolCoreSize, registerPoolMaxSize, new LinkedBlockingQueue<Runnable>(registerPoolQueueSize),
            new CallerRunsPolicy());

    private static ClientManager instance = new ClientManager();

    private RegistryConnectionListener registryConnectionListener = new InnerRegistryConnectionListener();

    private GroupChangeListener groupChangeListener = new InnerGroupChangeListener();

    private static boolean enableVip = ConfigManagerLoader.getConfigManager()
            .getBooleanValue("pigeon.invoker.vip.enable", false);

    private static volatile boolean enableRegisterConcurrently = ConfigManagerLoader.getConfigManager()
            .getBooleanValue("pigeon.invoker.registerconcurrently.enable", true);

    public static ClientManager getInstance() {
        return instance;
    }

    private ClientManager() {
        // 服务可用检查线程
        this.providerAvailableListener = new ProviderAvailableListener();
        // 配置对集群化的服务可用检查
        this.clusterListener = new DefaultClusterListener(providerAvailableListener);
        // 添加到监听管理器
        this.clusterListenerManager.addListener(this.clusterListener);
        // 执行服务可用检查
        providerAvailableThreadPool.execute(this.providerAvailableListener);
        // 添加服务变更监听
        RegistryEventListener.addListener(providerChangeListener);
        RegistryEventListener.addListener(registryConnectionListener);
        RegistryEventListener.addListener(groupChangeListener);
        registerThreadPool.getExecutor().allowCoreThreadTimeOut(true);
    }

    public void registerClient(String serviceName, String host, int port, int weight) {
        ConnectInfo connectInfo = new ConnectInfo(serviceName, host, port, weight);
        this.clusterListenerManager.addConnect(connectInfo, serviceName);
        RegistryManager.getInstance().addServiceAddress(serviceName, host, port, weight);
    }

    public Client getClient(InvokerConfig<?> invokerConfig, InvocationRequest request, List<Client> excludeClients) {
        List<Client> clientList = clusterListener.getClientList(invokerConfig);
        List<Client> clientsToRoute = new ArrayList<Client>(clientList);
        if (excludeClients != null) {
            clientsToRoute.removeAll(excludeClients);
        }
        return routerManager.route(clientsToRoute, invokerConfig, request);
    }

    public List<Client> getAvailableClients(InvokerConfig<?> invokerConfig, InvocationRequest request) {
        List<Client> clientList = clusterListener.getClientList(invokerConfig);
        return routerManager.getAvailableClients(clientList, invokerConfig, request);
    }

    public void destroy() throws Exception {
        if (clusterListenerManager instanceof Disposable) {
            ((Disposable) clusterListenerManager).destroy();
        }
        if (routerManager instanceof Disposable) {
            ((Disposable) routerManager).destroy();
        }
        RegistryEventListener.removeListener(providerChangeListener);
        ThreadPoolUtils.shutdown(providerAvailableThreadPool);
        // ThreadPoolUtils.shutdown(heartBeatThreadPool);
        // ThreadPoolUtils.shutdown(reconnectThreadPool);
        this.clusterListener.destroy();
    }

    public String getServiceAddress(InvokerConfig invokerConfig) {
        String remoteAppkey = invokerConfig.getRemoteAppKey();
        String serviceName = invokerConfig.getUrl();
        String group = RegistryManager.getInstance().getGroup(serviceName);
        String vip = invokerConfig.getVip();

        String serviceAddress = null;
        boolean useVip = false;
        if (StringUtils.isNotBlank(vip)) {
            if (enableVip) {
                useVip = true;
            }
            if (vip.startsWith("console:")) {
                useVip = true;
                vip = vip.replaceAll("console", configManager.getLocalIp());
            }
        }
        try {
            if (useVip) {
                serviceAddress = vip;
            } else if (StringUtils.isNotBlank(remoteAppkey)) {
                serviceAddress = RegistryManager.getInstance().getServiceAddress(remoteAppkey, serviceName, group);
            } else {
                serviceAddress = RegistryManager.getInstance().getServiceAddress(serviceName, group);
            }
        } catch (Throwable e) {
            logger.error("cannot get service provider for service:" + serviceName, e);
            throw new ServiceUnavailableException("cannot get service provider for service:" + serviceName
                    + ", remote app:" + remoteAppkey + ", env:" + configManager.getEnv(), e);
        }

        if (StringUtils.isBlank(serviceAddress)) {
            throw new ServiceUnavailableException("empty service address from registry for service:" + serviceName
                    + ", group:" + group + ", remote app:" + remoteAppkey + ", env:" + configManager.getEnv());
        }

        if (logger.isInfoEnabled()) {
            logger.info("selected service provider address is:" + serviceAddress + " with service:" + serviceName
                    + ",group:" + group + ", remote app:" + remoteAppkey);
        }
        serviceAddress = serviceAddress.trim();
        return serviceAddress;
    }

    public Set<HostInfo> registerClients(InvokerConfig invokerConfig) {
        String remoteAppkey = invokerConfig.getRemoteAppKey();
        String serviceName = invokerConfig.getUrl();
        String group = RegistryManager.getInstance().getGroup(serviceName);
        String vip = invokerConfig.getVip();

        logger.info("start to register clients for service '" + serviceName + "#" + group + "'");
        String localHost = null;
        if (vip != null && vip.startsWith("console:")) {
            localHost = configManager.getLocalIp() + vip.substring(vip.indexOf(":"));
        }
        String serviceAddress = getServiceAddress(invokerConfig);
        String[] addressArray = serviceAddress.split(",");
        Set<HostInfo> addresses = Collections.newSetFromMap(new ConcurrentHashMap<HostInfo, Boolean>());
        for (int i = 0; i < addressArray.length; i++) {
            if (StringUtils.isNotBlank(addressArray[i])) {
                // addressList.add(addressArray[i]);
                String address = addressArray[i];
                int idx = address.lastIndexOf(":");
                if (idx != -1) {
                    String host = null;
                    int port = -1;
                    try {
                        host = address.substring(0, idx);
                        port = Integer.parseInt(address.substring(idx + 1));
                    } catch (RuntimeException e) {
                        logger.warn("invalid address:" + address + " for service:" + serviceName);
                    }
                    if (host != null && port > 0) {
                        if (localHost != null && !localHost.equals(host + ":" + port)) {
                            continue;
                        }
                        try {
                            int weight = RegistryManager.getInstance().getServiceWeight(address, serviceName, false);
                            addresses.add(new HostInfo(host, port, weight));
                        } catch (Throwable e) {
                            logger.error("error while registering service invoker:" + serviceName + ", address:"
                                    + address + ", env:" + configManager.getEnv(), e);
                            throw new ServiceUnavailableException("error while registering service invoker:"
                                    + serviceName + ", address:" + address + ", env:" + configManager.getEnv(), e);
                        }
                    }
                } else {
                    logger.warn("invalid address:" + address + " for service:" + serviceName);
                }
            }
        }
        final String url = serviceName;
        long start = System.nanoTime();
        if (enableRegisterConcurrently) {
            final CountDownLatch latch = new CountDownLatch(addresses.size());
            for (final HostInfo hostInfo : addresses) {
                Runnable r = new Runnable() {

                    @Override
                    public void run() {
                        try {
                            RegistryEventListener.providerAdded(url, hostInfo.getHost(), hostInfo.getPort(),
                                    hostInfo.getWeight());
                            RegistryEventListener.serverInfoChanged(url, hostInfo.getConnect());
                        } catch (Throwable t) {
                            logger.error("failed to add provider client:" + hostInfo, t);
                        } finally {
                            latch.countDown();
                        }
                    }

                };
                registerThreadPool.submit(r);
            }
            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.info("", e);
            }
        } else {
            for (final HostInfo hostInfo : addresses) {
                RegistryEventListener.providerAdded(url, hostInfo.getHost(), hostInfo.getPort(), hostInfo.getWeight());
                RegistryEventListener.serverInfoChanged(url, hostInfo.getConnect());
            }
        }
        long end = System.nanoTime();
        logger.info("end to register clients for service '" + serviceName + "#" + group + "', cost:"
                + ((end - start) / 1000000));
        return addresses;
    }

    public Map<String, Set<HostInfo>> getServiceHosts() {
        return RegistryManager.getInstance().getAllReferencedServiceAddresses();
    }

    /**
     * @return the clusterListener
     */
    public DefaultClusterListener getClusterListener() {
        return clusterListener;
    }

    class InnerServiceProviderChangeListener implements ServiceProviderChangeListener {
        @Override
        public void providerAdded(ServiceProviderChangeEvent event) {
            if (logger.isInfoEnabled()) {
                logger.info("add " + event.getHost() + ":" + event.getPort() + ":" + event.getWeight() + " to "
                        + event.getServiceName());
            }
            registerClient(event.getServiceName(), event.getHost(), event.getPort(), event.getWeight());
        }

        @Override
        public void providerRemoved(ServiceProviderChangeEvent event) {
            HostInfo hostInfo = new HostInfo(event.getHost(), event.getPort(), event.getWeight());
            RegistryManager.getInstance().removeServiceAddress(event.getServiceName(), hostInfo);
        }

        @Override
        public void hostWeightChanged(ServiceProviderChangeEvent event) {
        }
    }

    class InnerRegistryConnectionListener implements RegistryConnectionListener {

        @Override
        public void reconnected() {
            Set<InvokerConfig<?>> services = ServiceFactory.getAllServiceInvokers().keySet();
            Map<String, Set<HostInfo>> serviceAddresses = RegistryManager.getInstance()
                    .getAllReferencedServiceAddresses();
            logger.info("begin to sync service addresses:" + services.size());

            for (InvokerConfig<?> invokerConfig : services) {
                String url = invokerConfig.getUrl();
                try {
                    Set<HostInfo> addresses = registerClients(invokerConfig);
                    // remove unreferenced service address
                    Set<HostInfo> currentAddresses = serviceAddresses.get(url);
                    if (currentAddresses != null && addresses != null) {
                        logger.info(
                                url + " 's addresses, new:" + addresses.size() + ", old:" + currentAddresses.size());

                        Set<HostInfo> toRemoveAddresses = new HashSet<>();
                        for (HostInfo currentAddress : currentAddresses) {
                            if (!addresses.contains(currentAddress)) {
                                toRemoveAddresses.add(currentAddress);
                            }
                        }

                        for (HostInfo hostPort : toRemoveAddresses) {
                            RegistryEventListener.providerRemoved(url, hostPort.getHost(), hostPort.getPort());
                        }
                    }
                } catch (Throwable t) {
                    logger.warn(
                            "error while trying to sync service addresses:" + url + ", caused by:" + t.getMessage());
                }
            }

            logger.info("succeed to sync service addresses");
        }

    }

    private class InnerGroupChangeListener implements GroupChangeListener {

        @Override
        public void onGroupChange(String ip, RegistryConfig oldRegistryConfig, RegistryConfig newRegistryConfig) {
            // reconnect to new ip:port list
            for (InvokerConfig<?> invokerConfig : ServiceFactory.getAllServiceInvokers().keySet()) {
                try {
                    logger.info("invoker group changed, service: " + invokerConfig.getUrl() + ", new group: "
                            + RegistryManager.getInstance().getGroup(invokerConfig.getUrl()));
                    String hosts = "";
                    try {
                        hosts = getServiceAddress(invokerConfig);
                    } catch (ServiceUnavailableException e) {
                        logger.info("hosts is empty!", e);
                    }
                    List<String[]> hostDetail = Utils.getServiceIpPortList(hosts);
                    DefaultServiceChangeListener.INSTANCE.onServiceHostChange(invokerConfig.getUrl(), hostDetail);
                } catch (Throwable e) {
                    logger.warn("failed to change refresh invoker to new group, caused by: " + e.getMessage());
                }
            }
        }
    }

    public void clear() {
        clusterListener.clear();
    }

    public static ThreadPool getRegisterThreadPool() {
        return registerThreadPool;
    }
}
