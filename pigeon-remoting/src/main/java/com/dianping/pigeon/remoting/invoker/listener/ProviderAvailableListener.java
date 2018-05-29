/**
 * WS  * Dianping.com Inc.
 * Copyright (c) 2003-2013 All Rights Reserved.
 */
package com.dianping.pigeon.remoting.invoker.listener;

import com.dianping.pigeon.config.ConfigManager;
import com.dianping.pigeon.config.ConfigManagerLoader;
import com.dianping.pigeon.domain.HostInfo;
import com.dianping.pigeon.log.LoggerLoader;
import com.dianping.pigeon.registry.RegistryManager;
import com.dianping.pigeon.registry.listener.RegistryEventListener;
import com.dianping.pigeon.registry.util.HeartBeatSupport;
import com.dianping.pigeon.remoting.ServiceFactory;
import com.dianping.pigeon.remoting.invoker.Client;
import com.dianping.pigeon.remoting.invoker.ClientManager;
import com.dianping.pigeon.remoting.invoker.config.InvokerConfig;
import com.dianping.pigeon.util.VersionUtils;
import org.apache.commons.lang.StringUtils;
import com.dianping.pigeon.log.Logger;
import org.springframework.util.CollectionUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ProviderAvailableListener implements Runnable {

    private static final Logger logger = LoggerLoader.getLogger(ProviderAvailableListener.class);

    private Map<String, List<Client>> workingClients;

    private static ConfigManager configManager = ConfigManagerLoader.getConfigManager();

    private static final String KEY_INTERVAL = "pigeon.providerlistener.interval";

    private static final String KEY_AVAILABLE_LEAST = "pigeon.providerlistener.availableleast";

    public ProviderAvailableListener() {
        configManager.getLongValue(KEY_INTERVAL, 3000);
        configManager.getIntValue(KEY_AVAILABLE_LEAST, 1);
    }

    private int getAvailableClients(List<Client> clientList, String serviceName) {
        int available = 0;
        if (CollectionUtils.isEmpty(clientList)) {
            available = 0;
        } else {
            for (Client client : clientList) {
                int w = RegistryManager.getInstance().getServiceWeight(client.getAddress(), serviceName);
                if (w > 0 && !client.isClosed() && client.isActive()) {
                    available += w;
                }
            }
        }
        return available;
    }

    public void run() {
        long sleepTime = configManager.getLongValue(KEY_INTERVAL, 3000);
        int checkCount = 0;
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(sleepTime);
                try {
                    checkReferencedServices();
                } catch (Throwable e) {
                    logger.info("check referenced services failed:", e);
                }

                Set<InvokerConfig<?>> services = ServiceFactory.getAllServiceInvokers().keySet();
                Map<String, Set<HostInfo>> serviceAddresses = RegistryManager.getInstance()
                        .getAllReferencedServiceAddresses();
                long now = System.nanoTime();

                for (InvokerConfig<?> invokerConfig : services) {
                    String url = invokerConfig.getUrl();
                    String vip = invokerConfig.getVip();

                    if (StringUtils.isNotBlank(vip) && vip.startsWith("console:")) {
                        continue;
                    }

                    int available = getAvailableClients(this.getWorkingClients().get(url), url);

                    if (available < configManager.getIntValue(KEY_AVAILABLE_LEAST, 1)) {
                        logger.info("check provider available for service:" + url);
                        String error = null;
                        try {
                            Set<HostInfo> addresses = ClientManager.getInstance().registerClients(invokerConfig);

                            Set<HostInfo> currentAddresses = serviceAddresses.get(url);
                            if (currentAddresses != null && addresses != null) {
                                logger.info(url + " 's addresses, new:" + addresses.size() + ", old:"
                                        + currentAddresses.size());

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
                        } catch (Throwable e) {
                            error = e.getMessage();
                        }
                        if (error != null) {
                            logger.warn("[provider-available] failed to get providers, caused by:" + error);
                        }
                    }
                }

                sleepTime = configManager.getLongValue(KEY_INTERVAL, 3000) - ((System.nanoTime() - now) / 1000000);

                // close register thread pool
                /*
                 * if (++checkCount > 0) {
                 * ClientManager.getInstance().closeRegisterThreadPool(); }
                 */
            } catch (Throwable e) {
                logger.info("[provider-available] task failed:", e);
            } finally {
                if (sleepTime < 1000) {
                    sleepTime = 1000;
                }
            }
        }
    }

    private void checkReferencedServices() {
        Map<String, Set<HostInfo>> serviceAddresses = RegistryManager.getInstance().getAllReferencedServiceAddresses();
        for (String serviceName : serviceAddresses.keySet()) {
            Set<HostInfo> hosts = serviceAddresses.get(serviceName);
            if (hosts != null) {
                for (HostInfo host : hosts) {
                    if (StringUtils.isBlank(host.getApp())) {
                        String app = RegistryManager.getInstance().getReferencedApp(host.getConnect(), serviceName);
                        logger.info("set " + host.getConnect() + "'s app to " + app);
                        host.setApp(app);
                        RegistryManager.getInstance().setReferencedApp(host.getConnect(), app);
                    }
                    if (StringUtils.isBlank(host.getVersion())) {
                        String version = RegistryManager.getInstance().getReferencedVersion(host.getConnect(),
                                serviceName);
                        logger.info("set " + host.getConnect() + "'s version to " + version);
                        host.setVersion(version);
                        RegistryManager.getInstance().setReferencedVersion(host.getConnect(), version);
                        if (StringUtils.isNotBlank(version)) {
                            byte heartBeatSupport;
                            if (VersionUtils.isThriftSupported(version)) {
                                heartBeatSupport = HeartBeatSupport.BothSupport.getValue();
                            } else {
                                heartBeatSupport = HeartBeatSupport.P2POnly.getValue();
                            }
                            logger.info("set " + host.getConnect() + "'s heartBeatSupport to " + heartBeatSupport);
                            host.setHeartBeatSupport(heartBeatSupport);
                            RegistryManager.getInstance().setServerHeartBeatSupport(host.getConnect(),
                                    heartBeatSupport);
                        }
                    }
                }
            }
        }
    }

    public Map<String, List<Client>> getWorkingClients() {
        return workingClients;
    }

    public void setWorkingClients(Map<String, List<Client>> workingClients) {
        this.workingClients = workingClients;
    }
}
