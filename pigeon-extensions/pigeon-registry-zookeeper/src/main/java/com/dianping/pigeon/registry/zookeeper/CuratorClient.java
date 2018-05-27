package com.dianping.pigeon.registry.zookeeper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.*;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.EnsurePath;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import com.dianping.pigeon.config.ConfigChangeListener;
import com.dianping.pigeon.config.ConfigManager;
import com.dianping.pigeon.config.ConfigManagerLoader;
import com.dianping.pigeon.log.Logger;
import com.dianping.pigeon.log.LoggerLoader;
import com.dianping.pigeon.monitor.Monitor;
import com.dianping.pigeon.monitor.MonitorLoader;
import com.dianping.pigeon.registry.listener.RegistryEventListener;
import com.dianping.pigeon.threadpool.DefaultThreadFactory;

public class CuratorClient {

    private static final String CHARSET = "UTF-8";

    private static Logger logger = LoggerLoader.getLogger(CuratorClient.class);

    private ConfigManager configManager = ConfigManagerLoader.getConfigManager();

    private CuratorFramework client;

    private volatile int retries = configManager.getIntValue("pigeon.registry.curator.retries", Integer.MAX_VALUE);

    private volatile int retryInterval = configManager.getIntValue("pigeon.registry.curator.retryinterval", 3000);

    private volatile int retryLimit = configManager.getIntValue("pigeon.registry.curator.retrylimit", 50);

    private int sessionTimeout = configManager.getIntValue("pigeon.registry.curator.sessiontimeout", 30 * 1000);

    private int connectionTimeout = configManager.getIntValue("pigeon.registry.curator.connectiontimeout", 15 * 1000);

    private static ExecutorService curatorStateListenerThreadPool = Executors
            .newCachedThreadPool(new DefaultThreadFactory("Pigeon-Curator-State-Listener"));

    private static ExecutorService curatorEventListenerThreadPool = Executors
            .newCachedThreadPool(new DefaultThreadFactory("Pigeon-Curator-Event-Listener"));

    private static Monitor monitor = MonitorLoader.getMonitor();

    private static final String KEY_REGISTRY_ADDRESS = "pigeon.registry.address";

    private volatile String address;

    private volatile boolean initialized = false;

    private volatile boolean curatorStateListenerActive = false;

    private static final String KEY_REGISTRY_CURATOR_CLIENT_ACTIVE = "pigeon.registry.curator.client.active";

    private final String EVENT_NAME = "Pigeon.registry";

    public CuratorClient() throws Exception {
        boolean curatorClientActive = configManager.getBooleanValue(KEY_REGISTRY_CURATOR_CLIENT_ACTIVE, true);
        if (curatorClientActive) {
            init();
        } else {
            destroy();
        }
        configManager.registerConfigChangeListener(new InnerConfigChangeListener());
    }

    private void init() throws Exception {
        if (!initialized) {
            synchronized (this) {
                if (!initialized) {
                    curatorStateListenerActive = true;
                    newCuratorClient(configManager.getStringValue(KEY_REGISTRY_ADDRESS));
                    initialized = true;
                }
            }
        }
    }

    private void newCuratorClient(String zkAddress) throws Exception {
        if (StringUtils.isBlank(zkAddress)) {
            throw new IllegalArgumentException("zookeeper address is required");
        }
        logger.info("start to initialize zookeeper client:" + zkAddress);
        this.address = zkAddress;
        // 初始化Curator客户端
        newCuratorClient();
        // 设置断线重连线程
        curatorStateListenerThreadPool.execute(new CuratorStateListener());
        logger.info("succeed to initialize zookeeper client:" + zkAddress);
    }

    private boolean newCuratorClient() throws InterruptedException {
        logger.info("begin to create zookeeper client:" + address);
        // CuratorFramework client = CuratorFrameworkFactory.newClient(address,
        // sessionTimeout, connectionTimeout,
        // new MyRetryPolicy(retries, retryInterval));
        CuratorFramework client = CuratorFrameworkFactory.builder().connectString(address)
                .sessionTimeoutMs(sessionTimeout).connectionTimeoutMs(connectionTimeout)
                .retryPolicy(new MyRetryPolicy(retries, retryInterval)).build();
        client.getConnectionStateListenable().addListener(new ConnectionStateListener() {
            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                logger.info("zookeeper state changed to " + newState);
                if (newState == ConnectionState.RECONNECTED) {
                    RegistryEventListener.connectionReconnected();
                }
                monitor.logEvent(EVENT_NAME, "zookeeper:" + newState.name().toLowerCase(), "");
            }
        });
        // 添加事件监听，主要用于Pigeon中实现的RegistryEventListener的变更通知
        client.getCuratorListenable().addListener(new CuratorEventListener(this), curatorEventListenerThreadPool);
        client.start();
        // 断线重连时关闭老的客户端，并设置新的客户端
        boolean isConnected = client.getZookeeperClient().blockUntilConnectedOrTimedOut();
        CuratorFramework oldClient = this.client;
        this.client = client;
        close(oldClient);

        if (isConnected) {
            logger.info("succeed to connect to zookeeper:" + address);
            monitor.logEvent(EVENT_NAME, "zookeeper:rebuild_success", "");
        } else {
            logger.warn("unable to connect to zookeeper:" + address);
            monitor.logEvent(EVENT_NAME, "zookeeper:rebuild_failure", "");
        }

        return isConnected;
    }

    public CuratorFramework getClient() {
        return client;
    }

    public boolean isConnected() {
        final CuratorFramework cf = getClient();
        if (cf != null) {
            if (cf instanceof BlankCuratorFramework) {
                return true;
            }
            try {
                return cf.getZookeeperClient().getZooKeeper().getState().isConnected()
                        && cf.getZookeeperClient().isConnected();
            } catch (Exception e) {
                return false;
            }
        }
        return false;
    }

    private class CuratorStateListener implements Runnable {

        private final Logger logger = LoggerLoader.getLogger(CuratorStateListener.class);

        public void run() {
            int failCount = 0;
            boolean isSuccess = true;
            logger.info("start [curator-state] task");
            while (!Thread.currentThread().isInterrupted() && curatorStateListenerActive) {
                try {
                    Thread.sleep(retryInterval * (1 + RandomUtils.nextInt(20)));
                    final CuratorFramework cf = getClient();
                    if (cf != null && !(cf instanceof BlankCuratorFramework)) {
                        int retryCount = ((MyRetryPolicy) cf.getZookeeperClient().getRetryPolicy()).getRetryCount();
                        boolean isConnected = false;
                        try {
                            // 获取客户端连接状态
                            isConnected = isConnected();
                        } catch (Exception e) {
                            logger.info("error with zookeeper client's connection:" + e.toString());
                        }
                        if (isConnected) {
                            if (!isSuccess) {
                                logger.info("begin to rebuild zookeeper client after reconnected");
                                isSuccess = rebuildCuratorClient();
                                logger.info("succeed to rebuild zookeeper client");
                            }
                            failCount = 0;
                        } else {
                            failCount++;
                            if (retryCount > 0) {
                                logger.info("zookeeper client's retries:" + retryCount + ", fails:" + failCount
                                        + ", limit:" + retryLimit);
                            }
                        }
                        if (failCount > retryLimit) {
                            logger.info("begin to rebuild zookeeper client after " + retryCount + "/" + failCount
                                    + " retries");
                            isSuccess = rebuildCuratorClient();
                            logger.info("succeed to rebuild zookeeper client");
                            failCount = 0;
                        }
                    }
                } catch (Throwable e) {
                    logger.warn("[curator-state] task failed:", e);
                }
            }
            logger.info("exit [curator-state] task");
        }

    }

    private boolean rebuildCuratorClient() throws InterruptedException {
        if (curatorStateListenerActive) {
            boolean isSuccess = newCuratorClient();
            if (isSuccess) {
                RegistryEventListener.connectionReconnected();
            }
            return isSuccess;
        }
        return false;
    }

    private static class MyRetryPolicy extends RetryNTimes {
        private final int sleepMsBetweenRetries;
        private int retryCount;

        public MyRetryPolicy(int n, int sleepMsBetweenRetries) {
            super(n, sleepMsBetweenRetries);
            this.sleepMsBetweenRetries = sleepMsBetweenRetries;
        }

        @Override
        protected int getSleepTimeMs(int retryCount, long elapsedTimeMs) {
            this.retryCount = retryCount;
            return sleepMsBetweenRetries * (1 + RandomUtils.nextInt(20));
        }

        public int getRetryCount() {
            return retryCount;
        }
    }

    public String get(String path) throws Exception {
        return get(path, true);
    }

    public String getWithNodeExistsEx(String path, Stat stat) throws Exception {
        if (exists(path, false)) {
            byte[] bytes = client.getData().storingStatIn(stat).forPath(path);
            String value = new String(bytes, CHARSET);
            if (logger.isDebugEnabled()) {
                logger.debug("get value of node " + path + ", value " + value);
            }
            return value;
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("node " + path + " does not exist");
            }
            throw new KeeperException.NodeExistsException("node " + path + " does not exist");
        }
    }

    public String get(String path, Stat stat) throws Exception {
        if (exists(path, false)) {
            byte[] bytes = client.getData().storingStatIn(stat).forPath(path);
            String value = new String(bytes, CHARSET);
            if (logger.isDebugEnabled()) {
                logger.debug("get value of node " + path + ", value " + value);
            }
            return value;
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("node " + path + " does not exist");
            }
            return null;
        }
    }

    public String get(String path, boolean watch) throws Exception {
        if (exists(path, watch)) {
            byte[] bytes = client.getData().forPath(path);
            String value = new String(bytes, CHARSET);
            if (logger.isDebugEnabled()) {
                logger.debug("get value of node " + path + ", value " + value);
            }
            return value;
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("node " + path + " does not exist");
            }
            return null;
        }
    }

    public void set(String path, Object value, int version) throws Exception {
        byte[] bytes = (value == null ? new byte[0] : value.toString().getBytes(CHARSET));
        if (exists(path, false)) {
            client.setData().withVersion(version).forPath(path, bytes);
            if (logger.isDebugEnabled()) {
                logger.debug("set value of node " + path + " to " + value);
            }
        } else {
            client.create().creatingParentsIfNeeded().forPath(path, bytes);
            if (logger.isDebugEnabled()) {
                logger.debug("create node " + path + " value " + value);
            }
        }
    }

    public void set(String path, Object value) throws Exception {
        byte[] bytes = (value == null ? new byte[0] : value.toString().getBytes(CHARSET));
        if (exists(path, false)) {
            client.setData().forPath(path, bytes);
            if (logger.isDebugEnabled()) {
                logger.debug("set value of node " + path + " to " + value);
            }
        } else {
            client.create().creatingParentsIfNeeded().forPath(path, bytes);
            if (logger.isDebugEnabled()) {
                logger.debug("create node " + path + " value " + value);
            }
        }
    }

    public void create(String path) throws Exception {
        create(path, null);
    }

    public void create(String path, Object value, int version) throws Exception {
        byte[] bytes = (value == null ? new byte[0] : value.toString().getBytes(CHARSET));
        client.create().creatingParentsIfNeeded().withProtection().forPath(path, bytes);
        if (logger.isInfoEnabled()) {
            logger.info("create node " + path + " value " + value);
        }
    }

    public void create(String path, Object value) throws Exception {
        byte[] bytes = (value == null ? new byte[0] : value.toString().getBytes(CHARSET));
        client.create().creatingParentsIfNeeded().forPath(path, bytes);
        if (logger.isInfoEnabled()) {
            logger.info("create node " + path + " value " + value);
        }
    }

    public void createEphemeral(String path, String value) throws Exception {
        byte[] bytes = (value == null ? new byte[0] : value.toString().getBytes(CHARSET));
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path, bytes);
        if (logger.isInfoEnabled()) {
            logger.info("create ephemeral node " + path + " value " + value);
        }
    }

    public void createEphemeral(String path) throws Exception {
        createEphemeral(path, null);
    }

    public boolean exists(String path) throws Exception {
        Stat stat = client.checkExists().watched().forPath(path);
        return stat != null;
    }

    public boolean exists(String path, boolean watch) throws Exception {
        Stat stat = watch ? client.checkExists().watched().forPath(path) : client.checkExists().forPath(path);
        return stat != null;
    }

    public List<String> getChildren(String path) throws Exception {
        return getChildren(path, true);
    }

    public List<String> getChildren(String path, boolean watch) throws Exception {
        try {
            List<String> children = watch ? client.getChildren().watched().forPath(path)
                    : client.getChildren().forPath(path);
            if (logger.isDebugEnabled()) {
                logger.debug("get children of node " + path + ": " + StringUtils.join(children.iterator(), ','));
            }
            return children;
        } catch (KeeperException.NoNodeException e) {
            logger.debug("node " + path + " does not exist");
            return new ArrayList<>();
        }
    }

    public void deleteIfExists(String path) throws Exception {
        if (exists(path, false)) {
            delete(path);
        } else {
            logger.warn("node " + path + " not exists!");
        }
    }

    public void delete(String path) throws Exception {
        client.delete().forPath(path);
        if (logger.isInfoEnabled()) {
            logger.info("delete node " + path);
        }
    }

    public void watch(String path) throws Exception {
        client.checkExists().watched().forPath(path);
    }

    public void watchChildren(String path) throws Exception {
        if (exists(path))
            client.getChildren().watched().forPath(path);
    }

    public void close() {
        this.close(this.client);
    }

    private void close(CuratorFramework client) {
        if (client != null) {
            logger.info("begin to close zookeeper client");
            try {
                client.close();
                logger.info("succeed to close zookeeper client");
            } catch (Exception e) {
            }
        }
    }

    public void destroy() throws Exception {
        if (initialized) {
            synchronized (this) {
                if (initialized) {
                    curatorStateListenerActive = false;
                    if (client != null) {
                        client.close();
                    }
                    client = new BlankCuratorFramework();
                    initialized = false;
                }
            }
        }
    }

    private class InnerConfigChangeListener implements ConfigChangeListener {

        @Override
        public void onKeyUpdated(String key, String value) {
            if (key.endsWith("pigeon.registry.curator.retries")) {
                try {
                    retries = Integer.valueOf(value);
                    MyRetryPolicy retryPolicy = new MyRetryPolicy(retries, retryInterval);
                    client.getZookeeperClient().setRetryPolicy(retryPolicy);
                } catch (RuntimeException e) {
                }
            } else if (key.endsWith("pigeon.registry.curator.retryinterval")) {
                try {
                    retryInterval = Integer.valueOf(value);
                    MyRetryPolicy retryPolicy = new MyRetryPolicy(retries, retryInterval);
                    client.getZookeeperClient().setRetryPolicy(retryPolicy);
                } catch (RuntimeException e) {
                }
            } else if (key.endsWith("pigeon.registry.curator.retrylimit")) {
                try {
                    retryLimit = Integer.valueOf(value);
                } catch (RuntimeException e) {
                }
            } else if (key.endsWith(KEY_REGISTRY_ADDRESS)) {
                address = value;
                logger.info("registry address changed:" + address);
                try {
                    Thread.sleep(RandomUtils.nextInt(180) * 1000);
                    rebuildCuratorClient();
                } catch (Exception e) {
                    logger.warn("rebuild curator client failed:", e);
                }
            } else if (key.endsWith(KEY_REGISTRY_CURATOR_CLIENT_ACTIVE)) {
                try {
                    if (Boolean.valueOf(value)) {
                        init();
                        if (isConnected()) {
                            RegistryEventListener.connectionReconnected();
                        }
                        logger.info("reopen curator client success!");
                    } else {
                        destroy();
                        logger.info("close curator client success!");
                    }
                } catch (Exception e) {
                    logger.warn("Failed to handle key: [" + key + "] with value: [" + value + "].");
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

    public String getStatistics() {
        if (getClient() instanceof BlankCuratorFramework) {
            return BlankCuratorFramework.class.toString();
        }

        CuratorZookeeperClient client = getClient().getZookeeperClient();
        return new StringBuilder().append("address:").append(client.getCurrentConnectionString()).append(", connected:")
                .append(isConnected()).append(", retries:")
                .append(((MyRetryPolicy) client.getRetryPolicy()).getRetryCount()).toString();
    }

    private class BlankCuratorFramework implements CuratorFramework {

        @Override
        public void start() {

        }

        @Override
        public void close() {

        }

        @Override
        public CuratorFrameworkState getState() {
            return null;
        }

        @Override
        public boolean isStarted() {
            return false;
        }

        @Override
        public CreateBuilder create() {
            return null;
        }

        @Override
        public DeleteBuilder delete() {
            return null;
        }

        @Override
        public ExistsBuilder checkExists() {
            return null;
        }

        @Override
        public GetDataBuilder getData() {
            return null;
        }

        @Override
        public SetDataBuilder setData() {
            return null;
        }

        @Override
        public GetChildrenBuilder getChildren() {
            return null;
        }

        @Override
        public GetACLBuilder getACL() {
            return null;
        }

        @Override
        public SetACLBuilder setACL() {
            return null;
        }

        @Override
        public CuratorTransaction inTransaction() {
            return null;
        }

        @Override
        public void sync(String path, Object backgroundContextObject) {

        }

        @Override
        public SyncBuilder sync() {
            return null;
        }

        @Override
        public Listenable<ConnectionStateListener> getConnectionStateListenable() {
            return null;
        }

        @Override
        public Listenable<CuratorListener> getCuratorListenable() {
            return null;
        }

        @Override
        public Listenable<UnhandledErrorListener> getUnhandledErrorListenable() {
            return null;
        }

        @Override
        public CuratorFramework nonNamespaceView() {
            return null;
        }

        @Override
        public CuratorFramework usingNamespace(String newNamespace) {
            return null;
        }

        @Override
        public String getNamespace() {
            return null;
        }

        @Override
        public CuratorZookeeperClient getZookeeperClient() {
            return null;
        }

        @Override
        public EnsurePath newNamespaceAwareEnsurePath(String path) {
            return null;
        }

        @Override
        public void clearWatcherReferences(Watcher watcher) {

        }

        @Override
        public boolean blockUntilConnected(int maxWaitTime, TimeUnit units) throws InterruptedException {
            return false;
        }

        @Override
        public void blockUntilConnected() throws InterruptedException {

        }
    }

}
