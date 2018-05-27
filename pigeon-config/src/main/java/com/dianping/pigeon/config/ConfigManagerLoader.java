package com.dianping.pigeon.config;

import com.dianping.pigeon.config.file.PropertiesFileConfigManager;
import com.dianping.pigeon.extension.ExtensionLoader;
import com.dianping.pigeon.log.Log4j2Logger;
import com.dianping.pigeon.log.Logger;
import com.dianping.pigeon.log.LoggerLoader;

public class ConfigManagerLoader {

    /**
     * 配置管理器<br>
     * 在Pigeon中默认实现了本地属性文件(Properties)的配置管理器<br>
     * Pigeon提供自定义扩展，可以结合其他注册中心来使用，方便在线更新配置<br>
     * 点评提供了Lion配置中心(未开源)，并且提供了pigeon-config-lion的实现<br>
     * {@link https://github.com/wu-xiang/pigeon-config-lion}
     */
    private static ConfigManager configManager = ExtensionLoader.getExtension(ConfigManager.class);
    private static final Logger logger = LoggerLoader.getLogger(ConfigManagerLoader.class);
    private static final String KEY_LOG_DEBUG_ENABLE = "pigeon.log.debug.enable";

    static {
        if (configManager == null) {
            configManager = new PropertiesFileConfigManager();
        }
        logger.info("config manager:" + configManager);
        configManager.init();
        initLoggerConfig();
    }

    private static void initLoggerConfig() {
        try {
            Log4j2Logger.setDebugEnabled(configManager.getBooleanValue(KEY_LOG_DEBUG_ENABLE, false));
        } catch (RuntimeException e) {
        }
        ConfigManagerLoader.getConfigManager().registerConfigChangeListener(new InnerConfigChangeListener());
    }

    private static class InnerConfigChangeListener implements ConfigChangeListener {

        @Override
        public void onKeyUpdated(String key, String value) {
            if (key.endsWith(KEY_LOG_DEBUG_ENABLE)) {
                try {
                    Log4j2Logger.setDebugEnabled(Boolean.valueOf(value));
                } catch (RuntimeException e) {
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

    public static ConfigManager getConfigManager() {
        return configManager;
    }
}
