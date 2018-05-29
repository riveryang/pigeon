package com.dianping.pigeon.registry.zookeeper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.KeeperException.BadVersionException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.Stat;

import com.dianping.pigeon.config.ConfigManager;
import com.dianping.pigeon.config.ConfigManagerLoader;
import com.dianping.pigeon.log.Logger;
import com.dianping.pigeon.log.LoggerLoader;
import com.dianping.pigeon.registry.Registry;
import com.dianping.pigeon.registry.config.RegistryConfig;
import com.dianping.pigeon.registry.exception.RegistryException;
import com.dianping.pigeon.registry.util.Constants;
import com.dianping.pigeon.registry.util.HeartBeatSupport;
import com.dianping.pigeon.util.CollectionUtils;
import com.dianping.pigeon.util.VersionUtils;
import com.google.common.collect.ImmutableMap;

public class CuratorRegistry implements Registry {

    private static Logger logger = LoggerLoader.getLogger(CuratorRegistry.class);

    private ConfigManager configManager = ConfigManagerLoader.getConfigManager();

    private CuratorClient client;

    private volatile boolean inited = false;

    private final boolean delEmptyNode = configManager.getBooleanValue("pigeon.registry.delemptynode", true);

    @Override
    public void init() {
        if (!inited) {
            synchronized (this) {
                if (!inited) {
                    try {
                        // 初始化Curator封装实现
                        client = new CuratorClient();
                        if (!client.isConnected()) {
                            throw new IllegalStateException("unable to connect to zookeeper");
                        }
                        inited = true;
                    } catch (Exception ex) {
                        logger.error("failed to initialize zookeeper client", ex);
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
    }

    @Override
    public boolean isEnable() {
        return inited;
    }

    @Override
    public String getName() {
        return Constants.REGISTRY_CURATOR_NAME;
    }

    @Override
    public String getServiceAddress(String serviceName) throws RegistryException {
        return getServiceAddress(serviceName, Constants.DEFAULT_GROUP);
    }

    public String getServiceAddress(String serviceName, String group) throws RegistryException {
        return getServiceAddress(serviceName, group, true);
    }

    public String getServiceAddress(String serviceName, String group, boolean fallbackDefaultGroup)
            throws RegistryException {
        return getServiceAddress(serviceName, group, fallbackDefaultGroup, true);
    }

    @Override
    public String getServiceAddress(String remoteAppkey, String serviceName, String group, boolean fallbackDefaultGroup)
            throws RegistryException {
        return getServiceAddress(remoteAppkey, serviceName, group, fallbackDefaultGroup, true);
    }

    @Override
    public void registerService(String serviceName, String group, String serviceAddress, int weight)
            throws RegistryException {
        registerPersistentNode(serviceName, group, serviceAddress, weight);
    }

    void registerPersistentNode(String serviceName, String group, String serviceAddress, int weight)
            throws RegistryException {
        // 设置权重的Path：/DP/WEIGHT/host:port
        String weightPath = Utils.getWeightPath(serviceAddress);
        // 设置服务的Path：/DP/SERVER/serviceName
        String servicePath = Utils.getServicePath(serviceName, group);
        try {
            // 验证Zookeeper中是否已经存在服务Path配置
            if (client.exists(servicePath, false)) {
                Stat stat = new Stat();
                // 获取服务Path的值，Pigeon往Zookeeper中写的规则是 host:port,host:port,...
                String addressValue = client.getWithNodeExistsEx(servicePath, stat);
                String[] addressArray = addressValue.split(",");
                List<String> addressList = new ArrayList<String>();
                for (String addr : addressArray) {
                    addr = addr.trim();
                    if (addr.length() > 0 && !addressList.contains(addr)) {
                        addressList.add(addr.trim());
                    }
                }
                // 如果列表中没有当前应用服务的地址，则往列表中添加当前应用服务的地址
                if (!addressList.contains(serviceAddress)) {
                    addressList.add(serviceAddress);
                    Collections.sort(addressList);
                    // 更新服务的应用地址列表
                    client.set(servicePath, StringUtils.join(addressList.iterator(), ","), stat.getVersion());
                }
            } else {
                // 如果注册中心没有当前服务Path，新建之
                client.create(servicePath, serviceAddress);
            }

            // 设置服务权重
            if (weight >= 0) {
                client.set(weightPath, "" + weight);
            }
            if (logger.isInfoEnabled()) {
                logger.info("registered service to persistent node: " + servicePath);
            }
        } catch (Throwable e) {
            // 防止并发操作时数据不一致，保证数据一致性，在这里做重试处理
            if (e instanceof BadVersionException || e instanceof NodeExistsException) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ie) {
                    // ignore
                }
                // 重试服务注册
                registerPersistentNode(serviceName, group, serviceAddress, weight);
            } else {
                logger.info("failed to register service to " + servicePath, e);
                throw new RegistryException(e);
            }

        }
    }

    @Override
    public void unregisterService(String serviceName, String serviceAddress) throws RegistryException {
        unregisterService(serviceName, Constants.DEFAULT_GROUP, serviceAddress);
    }

    @Override
    public void unregisterService(String serviceName, String group, String serviceAddress) throws RegistryException {
        unregisterPersistentNode(serviceName, group, serviceAddress);
    }

    public void unregisterPersistentNode(String serviceName, String group, String serviceAddress)
            throws RegistryException {
        // 设置服务Path：/DP/SERVICE/serviceName
        String servicePath = Utils.getServicePath(serviceName, group);
        try {
            if (client.exists(servicePath, false)) {
                Stat stat = new Stat();
                // 获取注册中心的服务配置
                String addressValue = client.getWithNodeExistsEx(servicePath, stat);
                String[] addressArray = addressValue.split(",");
                List<String> addressList = new ArrayList<String>();
                for (String addr : addressArray) {
                    addr = addr.trim();
                    if (addr.length() > 0 && !addressList.contains(addr)) {
                        addressList.add(addr);
                    }
                }
                // 如果注册中心里有当前服务
                if (addressList.contains(serviceAddress)) {
                    // 移除之
                    addressList.remove(serviceAddress);
                    // 移除完后是否变成空列表
                    if (!addressList.isEmpty()) {
                        Collections.sort(addressList);
                        // 不是空列表，重写注册中心的配置
                        client.set(servicePath, StringUtils.join(addressList.iterator(), ","), stat.getVersion());
                    } else {
                        // 变成了空列表
                        // 看看这个Path在注册中心是否有子节点
                        List<String> children = client.getChildren(servicePath, false);
                        // 有子节点
                        if (CollectionUtils.isEmpty(children)) {
                            // 是否删除空节点，默认是true，由属性 pigeon.registry.delemptynode 进行设置
                            if (delEmptyNode) {
                                try {
                                    // 删除整个节点
                                    client.delete(servicePath);
                                } catch (NoNodeException e) {
                                    logger.warn("Already deleted path:" + servicePath + ":" + e.getMessage());
                                }
                            } else {
                                // 不删除空节点，置空值
                                client.set(servicePath, "", stat.getVersion());
                            }
                        } else {
                            logger.warn("Existing children [" + children + "] under path:" + servicePath);
                            // 存在子节点，置空值
                            client.set(servicePath, "", stat.getVersion());
                        }
                    }
                }
                if (logger.isInfoEnabled()) {
                    logger.info("unregistered service from " + servicePath);
                }
            }
        } catch (Throwable e) {
            // 保证数据一致性，重试之
            if (e instanceof BadVersionException) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ie) {
                    // ignore
                }
                unregisterPersistentNode(serviceName, group, serviceAddress);
            } else {
                logger.info("failed to unregister service from " + servicePath, e);
                throw new RegistryException(e);
            }
        }
    }

    @Override
    public int getServerWeight(String serverAddress, String serviceName) throws RegistryException {
        String path = Utils.getWeightPath(serverAddress);
        String strWeight;
        try {
            strWeight = client.get(path);
            int result = Constants.DEFAULT_WEIGHT;
            if (strWeight != null) {
                try {
                    result = Integer.parseInt(strWeight);
                } catch (NumberFormatException e) {
                    logger.warn("invalid weight for " + serverAddress + ": " + strWeight);
                }
            }
            return result;
        } catch (Throwable e) {
            logger.info("failed to get weight for " + serverAddress);
            throw new RegistryException(e);
        }
    }

    @Override
    public void setServerWeight(String serverAddress, int weight) throws RegistryException {
        String path = Utils.getWeightPath(serverAddress);
        try {
            client.set(path, weight);
        } catch (Throwable e) {
            logger.info("failed to set weight of " + serverAddress + " to " + weight);
            throw new RegistryException(e);
        }
    }

    @Override
    public List<String> getChildren(String path) throws RegistryException {
        try {
            List<String> children = client.getChildren(path);
            return children;
        } catch (Throwable e) {
            logger.info("failed to get children of node: " + path, e);
            throw new RegistryException(e);
        }
    }

    public void close() {
        client.close();
    }

    public CuratorClient getCuratorClient() {
        return client;
    }

    @Override
    public String getServerApp(String serverAddress, String serviceName) throws RegistryException {
        String path = Utils.getAppPath(serverAddress);
        try {
            return client.get(path);
        } catch (Throwable e) {
            logger.info("failed to get app for " + serverAddress);
            throw new RegistryException(e);
        }
    }

    @Override
    public void setServerApp(String serverAddress, String app) {
        // 设置应用名Path：/DP/APP/host:port
        String path = Utils.getAppPath(serverAddress);
        if (StringUtils.isNotBlank(app)) {
            try {
                client.set(path, app);
            } catch (Throwable e) {
                logger.info("failed to set app of " + serverAddress + " to " + app);
            }
        }
    }

    public void unregisterServerApp(String serverAddress) {
        String path = Utils.getAppPath(serverAddress);
        try {
            if (client.exists(path, false)) {
                client.delete(path);
            }
        } catch (Throwable e) {
            logger.info("failed to delete app:" + path + ", caused by:" + e.getMessage());
        }
    }

    @Override
    public void setServerVersion(String serverAddress, String version) {
        // 设置Pigeon版本号Path：/DP/VERSION/host:port
        String path = Utils.getVersionPath(serverAddress);
        if (StringUtils.isNotBlank(version)) {
            try {
                client.set(path, version);
            } catch (Throwable e) {
                logger.info("failed to set version of " + serverAddress + " to " + version);
            }
        }
    }

    @Override
    public String getServerVersion(String serverAddress, String serviceName) throws RegistryException {
        String path = Utils.getVersionPath(serverAddress);
        try {
            return client.get(path);
        } catch (Throwable e) {
            logger.info("failed to get version for " + serverAddress);
            throw new RegistryException(e);
        }
    }

    public void unregisterServerVersion(String serverAddress) {
        String path = Utils.getVersionPath(serverAddress);
        try {
            if (client.exists(path, false)) {
                client.delete(path);
            }
        } catch (Throwable e) {
            logger.info("failed to delete version:" + path + ", caused by:" + e.getMessage());
        }
    }

    @Override
    public String getStatistics() {
        return getName() + ":" + client.getStatistics();
    }

    @Override
    public byte getServerHeartBeatSupport(String serviceAddress, String serviceName) throws RegistryException {
        if (isSupportNewProtocol(serviceAddress)) {
            return HeartBeatSupport.BothSupport.getValue();
        } else {
            return HeartBeatSupport.P2POnly.getValue();
        }
    }

    @Override
    public void setServerService(String serviceName, String group, String hosts) throws RegistryException {
        String servicePath = Utils.getServicePath(serviceName, group);

        try {
            client.set(servicePath, hosts);
        } catch (Throwable e) {
            logger.info("failed to set service hosts of " + serviceName + " to " + hosts);
            throw new RegistryException(e);
        }
    }

    @Override
    public void delServerService(String serviceName, String group) throws RegistryException {
        String servicePath = Utils.getServicePath(serviceName, group);

        try {
            List<String> children = client.getChildren(servicePath);

            if (children != null && children.size() > 0) {
                client.set(servicePath, "");
            } else {
                client.delete(servicePath);
            }
        } catch (Throwable e) {
            logger.info("failed to delete service hosts of " + serviceName);
            throw new RegistryException(e);
        }
    }

    @Override
    public void setHostsWeight(String serviceName, String group, String hosts, int weight) throws RegistryException {

        for (String host : hosts.split(",")) {
            setServerWeight(host, weight);
        }
    }

    @Override
    public String getServiceAddress(String remoteAppkey, String serviceName, String group, boolean fallbackDefaultGroup,
                                    boolean needListener) throws RegistryException {
        // blank
        return "";
    }

    @Override
    public String getServiceAddress(String serviceName, String group, boolean fallbackDefaultGroup,
                                    boolean needListener) throws RegistryException {
        try {
            String path = Utils.getServicePath(serviceName, group);
            String address = client.get(path, needListener);
            if (!StringUtils.isBlank(group)) {
                boolean needFallback = false;
                if (!Utils.isValidAddress(address)) {
                    needFallback = true;
                }
                if (fallbackDefaultGroup && needFallback) {
                    logger.info("node " + path + " does not exist, fallback to default group");
                    path = Utils.getServicePath(serviceName, Constants.DEFAULT_GROUP);
                    address = client.get(path, needListener);
                }
            }
            return address;
        } catch (Exception e) {
            logger.info("failed to get service address for " + serviceName + "/" + group, e);
            throw new RegistryException(e);
        }
    }

    @Override
    public void updateHeartBeat(String serviceAddress, Long heartBeatTimeMillis) {
        try {
            String heartBeatPath = Utils.getHeartBeatPath(serviceAddress);
            client.set(heartBeatPath, heartBeatTimeMillis);
        } catch (Throwable e) {
            logger.info("failed to update heartbeat", e);
        }
    }

    @Override
    public void deleteHeartBeat(String serviceAddress) {
        try {
            String heartBeatPath = Utils.getHeartBeatPath(serviceAddress);
            client.delete(heartBeatPath);
        } catch (Throwable e) {
            logger.info("failed to delete heartbeat", e);
        }
    }

    @Override
    public boolean isSupportNewProtocol(String serviceAddress) throws RegistryException {
        String version = getServerVersion(serviceAddress, null);

        if (StringUtils.isBlank(version)) {
            throw new RegistryException("version is blank");
        }

        return VersionUtils.isThriftSupported(version);
    }

    @Override
    public Map<String, Boolean> getServiceProtocols(String serviceAddress, String serviceName) throws RegistryException {
        try {
            String protocolPath = Utils.getProtocolPath(serviceAddress);
            String info = client.get(protocolPath);

            if (info != null) {
                return Utils.getProtocolInfoMap(info);
            }
        } catch (Throwable e) {
            logger.info("failed to get service protocols of host:" + serviceAddress + ", caused by:"
                    + e.getMessage());
            throw new RegistryException(e);
        }

        return new HashMap<>();
    }

    @Override
    public boolean isSupportNewProtocol(String serviceAddress, String serviceName) throws RegistryException {
        try {
            String protocolPath = Utils.getProtocolPath(serviceAddress);
            String info = client.get(protocolPath);

            if (info != null) {
                Map<String, Boolean> infoMap = Utils.getProtocolInfoMap(info);
                Boolean support = infoMap.get(serviceName);
                if (support != null) {
                    return support;
                }
            }

            return false;
        } catch (Throwable e) {
            logger.info("failed to get protocol:" + serviceName + "of host:" + serviceAddress + ", caused by:"
                    + e.getMessage());
            throw new RegistryException(e);
        }
    }

    /**
     * 只有当为Pigeon新增了新的数据协议的时候才会往Zookeeper里写配置，用于减少写Zookeeper的压力<br>
     * support的值从ProviderConfig.supported中获取，在初始化ProviderConfig时确定是否是新的数据协议<br>
     * 目前Pigeon中只对Thrift提供支持，对于服务是否是Thrift协议由ThriftUtils判断
     * @see com.dianping.pigeon.remoting.provider.config.ProviderConfig
     * @see com.dianping.pigeon.util.ThriftUtils
     */
    @Override
    public void setSupportNewProtocol(String serviceAddress, String serviceName, boolean support)
            throws RegistryException {
        // only write to zk when support new protocol to relieve the pressure of zk
        if (!support) return;

        try {
            // 设置协议Path：/DP/PROTOCOL/host:port
            String protocolPath = Utils.getProtocolPath(serviceAddress);
            if (client.exists(protocolPath, false)) {
                Stat stat = new Stat();
                String info = client.getWithNodeExistsEx(protocolPath, stat);
                Map<String, Boolean> infoMap = Utils.getProtocolInfoMap(info);
                infoMap.put(serviceName, support);
                client.set(protocolPath, Utils.getProtocolInfo(infoMap), stat.getVersion());
            } else {
                Map<String, Boolean> infoMap = ImmutableMap.of(serviceName, support);
                client.create(protocolPath, Utils.getProtocolInfo(infoMap));
            }

        } catch (Throwable e) {
            // 防止并发操作时数据不一致，保证数据一致性，在这里做重试处理
            if (e instanceof BadVersionException || e instanceof NodeExistsException) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ie) {
                    // ignore
                }
                // 重新执行
                setSupportNewProtocol(serviceAddress, serviceName, support);
            } else {
                logger.info("failed to set protocol:" + serviceName + "of host:" + serviceAddress + " to:" + support
                        + ", caused by:" + e.getMessage());
                throw new RegistryException(e);
            }

        }
    }

    /**
     * 同setSupportNewProtocol一样的解释
     * @see #setSupportNewProtocol(String, String, boolean)
     */
    @Override
    public void unregisterSupportNewProtocol(String serviceAddress, String serviceName, boolean support)
            throws RegistryException {
        // only write to zk when support new protocol to relieve the pressure of zk
        if (!support) return;

        try {
            String protocolPath = Utils.getProtocolPath(serviceAddress);
            if (client.exists(protocolPath, false)) {
                Stat stat = new Stat();
                String info = client.getWithNodeExistsEx(protocolPath, stat);
                Map<String, Boolean> infoMap = Utils.getProtocolInfoMap(info);
                infoMap.remove(serviceName);

                // 重写数据协议配置
                if (infoMap.size() == 0 && delEmptyNode) {
                    client.set(protocolPath, "{}", stat.getVersion());
                } else {
                    client.set(protocolPath, Utils.getProtocolInfo(infoMap), stat.getVersion());
                }
            }

        } catch (Throwable e) {
            // 保证数据一致性
            if (e instanceof BadVersionException || e instanceof NodeExistsException) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ie) {
                    // ignore
                }
                unregisterSupportNewProtocol(serviceAddress, serviceName, support);
            } else {
                logger.info("failed to del protocol:" + serviceName + "of host:" + serviceAddress + ", caused by:"
                        + e.getMessage());
                throw new RegistryException(e);
            }

        }
    }

    @Override
    public void setConsoleAddress(String consoleAddress) {
        String clientPath = Utils.getConsolePath(consoleAddress);
        try {
            client.set(clientPath, null);
        } catch (Throwable t) {
            logger.info("failed to set consolePath " + clientPath, t);
        }
    }

    @Override
    public void unregisterConsoleAddress(String consoleAddress) {
        // 设置Console Path：/DP/CONSOLE/host:port
        String clientPath = Utils.getConsolePath(consoleAddress);
        try {
            client.delete(clientPath);
        } catch (Throwable t) {
            logger.info("failed to delete consolePath " + clientPath, t);
        }

    }

    @Override
    public List<String> getConsoleAddresses() {
        List<String> consoleAddresses = null;
        String consoleRootPath = Utils.getConsoleRootPath();

        try {
            consoleAddresses = client.getChildren(consoleRootPath);
        } catch (Throwable t) {
            logger.info("failed to get consoleRootPath " + consoleRootPath, t);
        }

        return consoleAddresses;
    }

    @Override
    public RegistryConfig getRegistryConfig(String ip) throws RegistryException {
        try {
            return Utils.getRegistryConfig(client.get(Utils.getRegistryConfigPath(configManager.getLocalIp())));
        } catch (Exception e) {
            throw new RegistryException(e);
        }
    }
}
