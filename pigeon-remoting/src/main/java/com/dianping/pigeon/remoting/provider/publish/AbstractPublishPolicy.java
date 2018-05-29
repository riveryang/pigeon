package com.dianping.pigeon.remoting.provider.publish;

import com.dianping.pigeon.config.ConfigManager;
import com.dianping.pigeon.config.ConfigManagerLoader;
import com.dianping.pigeon.log.Logger;
import com.dianping.pigeon.log.LoggerLoader;
import com.dianping.pigeon.remoting.ServiceFactory;
import com.dianping.pigeon.remoting.common.exception.RpcException;
import com.dianping.pigeon.remoting.provider.ProviderBootStrap;
import com.dianping.pigeon.remoting.provider.config.ProviderConfig;
import com.dianping.pigeon.remoting.provider.config.ServerConfig;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Created by chenchongze on 16/11/3.
 */
public class AbstractPublishPolicy implements PublishPolicy {

    private static final Logger logger = LoggerLoader.getLogger(AbstractPublishPolicy.class);
    private static final ConfigManager configManager = ConfigManagerLoader.getConfigManager();
    private static final boolean IS_CHECK_SERVICE_EXCEPTION_DEFAULT
            = configManager.getBooleanValue("pigeon.check.is.stock.service.failure.exception.default", true);
    private static final boolean IS_CHECK_SERVICE_DEFAULT
            = configManager.getBooleanValue("pigeon.check.is.stock.service.failure.default", true);
    private static final boolean IS_ALLOW_CUSTOMIZED_SERVICENAME
            = configManager.getBooleanValue("pigeon.check.is.allow.customized.servicename", false);

    @Override
    public void init() {

    }

    @Override
    public void doAddService(ProviderConfig providerConfig) {
        try {
            // 验证服务名是否有效，结合 url和 serviceInterface 进行校验
            checkServiceName(providerConfig);
            // 设置服务映射和方法映射
            ServicePublisher.addService(providerConfig);
            // 启动NettyServer
            ServerConfig serverConfig = ProviderBootStrap.startup(providerConfig);
            providerConfig.setServerConfig(serverConfig);
            // 服务注册
            ServicePublisher.publishService(providerConfig, false);
        } catch (Throwable t) {
            throw new RpcException("error while adding service:" + providerConfig, t);
        }
    }

    private void checkServiceName(ProviderConfig providerConfig) {
        String serviceUrl = ServiceFactory.getServiceUrl(providerConfig);
        String customUrl = providerConfig.getUrl();

        if (StringUtils.isBlank(customUrl)) {
            providerConfig.setUrl(serviceUrl);
        } else if (!serviceUrl.equals(customUrl) && !isStockService(customUrl)) {
            if (IS_ALLOW_CUSTOMIZED_SERVICENAME) {
                return;
            }

            // 非存量服务,不允许注册,抛出异常或强制转换为类路径服务名
            if (IS_CHECK_SERVICE_EXCEPTION_DEFAULT) {
                logger.error("customized [serviceName]: " + customUrl
                        + " cannot provide service to OCTO invoker "
                        + "unless set the [serviceName] to full class name "
                        + "or just keep [serviceName] config to blank.\n"
                        + "[serviceName] should be replaced by full class name: "
                        + serviceUrl + ", more help refer to: "
                        + configManager.getStringValue("pigeon.help.provider.octo.url"
                        , "http://wiki.sankuai.com/pages/viewpage.action?pageId=606809899"));
                System.exit(1);
            } else {
                logger.warn("customized [serviceName]: " + customUrl
                        + " cannot provide service to OCTO invoker "
                        + "unless set the [serviceName] to full class name "
                        + "or just keep [serviceName] config to blank.\n"
                        + "[serviceName] will be replaced by full class name: "
                        + serviceUrl + ", more help refer to: "
                        + configManager.getStringValue("pigeon.help.provider.octo.url"
                        , "http://wiki.sankuai.com/pages/viewpage.action?pageId=606809899"));
                providerConfig.setUrl(serviceUrl);
            }
        }
    }

    /**
     * 检查用户自定义服务名是否为存量服务
     * @param customUrl
     * @return
     */
    private boolean isStockService(String customUrl) {
        String checkUrl = configManager.getStringValue("pigeon.governor.check.is.stock.service.url",
                "http://pigeon.sankuai.com/api/service/check/exist");

        StringBuilder url = new StringBuilder();
        url.append(checkUrl).append("?id=3&service=").append(customUrl);

        try {
            return doCheckService(url.toString());
        } catch (Throwable t) {
            logger.info("error while check service in stock:" + url + "\n" + t.toString());
        }

        return IS_CHECK_SERVICE_DEFAULT;
    }

    private boolean doCheckService(String url) throws IOException {
        HttpClient httpClient = getHttpClient();
        GetMethod getMethod = null;
        String response = null;
        logger.info("check service in stock:" + url);
        try {
            getMethod = new GetMethod(url);
            httpClient.executeMethod(getMethod);
            if (getMethod.getStatusCode() >= 300) {
                throw new IllegalStateException("Did not receive successful HTTP response: status code = "
                        + getMethod.getStatusCode() + ", status message = [" + getMethod.getStatusText() + "]");
            }
            InputStream inputStream = getMethod.getResponseBodyAsStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
            StringBuilder sb = new StringBuilder();
            String str = null;
            while ((str = br.readLine()) != null) {
                sb.append(str);
            }
            response = sb.toString();
            br.close();
        } finally {
            if (getMethod != null) {
                getMethod.releaseConnection();
            }
        }
        boolean isSuccess = IS_CHECK_SERVICE_DEFAULT;
        if (response.startsWith("0")) {
            isSuccess = true;
        } else if (response.startsWith("1")) {
            isSuccess = false;
        }
        return isSuccess;
    }

    private HttpClient getHttpClient() {
        HttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
        HttpConnectionManagerParams params = new HttpConnectionManagerParams();
        params.setMaxTotalConnections(300);
        params.setDefaultMaxConnectionsPerHost(50);
        params.setConnectionTimeout(3000);
        params.setTcpNoDelay(true);
        params.setSoTimeout(3000);
        params.setStaleCheckingEnabled(true);
        connectionManager.setParams(params);
        HttpClient httpClient = new HttpClient();
        httpClient.setHttpConnectionManager(connectionManager);

        return httpClient;
    }


}
