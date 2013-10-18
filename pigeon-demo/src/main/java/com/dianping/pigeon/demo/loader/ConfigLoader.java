package com.dianping.pigeon.demo.loader;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.dianping.pigeon.config.ConfigManager;
import com.dianping.pigeon.extension.ExtensionLoader;

public class ConfigLoader {

	private static final String PROPERTIES_PATH = "config/applicationContext.properties";

	private static final Logger logger = Logger.getLogger(ConfigLoader.class);

	public static void init() {
		Properties properties = new Properties();
		InputStream input = Thread.currentThread().getContextClassLoader().getResourceAsStream(PROPERTIES_PATH);
		if (input != null) {
			try {
				properties.load(input);
				input.close();
			} catch (IOException e) {
				logger.error("", e);
			}
		}
		try {
			ExtensionLoader.getExtension(ConfigManager.class).init(properties);
		} catch (Exception e) {
			logger.error("", e);
		}
	}

}
