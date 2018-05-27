/**
 * Dianping.com Inc.
 * Copyright (c) 2003-${year} All Rights Reserved.
 */
package com.dianping.pigeon.remoting.provider.process.filter;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Map;

import com.dianping.pigeon.remoting.common.codec.SerializerType;
import org.apache.commons.lang.StringUtils;
import org.springframework.util.CollectionUtils;

import com.dianping.pigeon.config.ConfigManagerLoader;
import com.dianping.pigeon.extension.ExtensionLoader;
import com.dianping.pigeon.log.Logger;
import com.dianping.pigeon.log.LoggerLoader;
import com.dianping.pigeon.monitor.Monitor;
import com.dianping.pigeon.monitor.MonitorLoader;
import com.dianping.pigeon.monitor.MonitorTransaction;
import com.dianping.pigeon.remoting.common.domain.InvocationContext.TimePhase;
import com.dianping.pigeon.remoting.common.domain.InvocationContext.TimePoint;
import com.dianping.pigeon.remoting.common.domain.InvocationRequest;
import com.dianping.pigeon.remoting.common.domain.InvocationResponse;
import com.dianping.pigeon.remoting.common.monitor.SizeMonitor;
import com.dianping.pigeon.remoting.common.process.ServiceInvocationFilter;
import com.dianping.pigeon.remoting.common.process.ServiceInvocationHandler;
import com.dianping.pigeon.remoting.common.util.Constants;
import com.dianping.pigeon.remoting.common.util.ContextUtils;
import com.dianping.pigeon.remoting.common.util.InvocationUtils;
import com.dianping.pigeon.remoting.provider.domain.ProviderChannel;
import com.dianping.pigeon.remoting.provider.domain.ProviderContext;
import com.dianping.pigeon.remoting.provider.process.ProviderContextProcessor;
import com.dianping.pigeon.remoting.provider.service.method.ServiceMethod;
import com.dianping.pigeon.remoting.provider.service.method.ServiceMethodFactory;

public class MonitorProcessFilter implements ServiceInvocationFilter<ProviderContext> {

    private static final Logger logger = LoggerLoader.getLogger(MonitorProcessFilter.class);

    private static final Logger accessLogger = LoggerLoader.getLogger(com.dianping.pigeon.util.Constants.ACCESS_LOG_NAME);

    /**
     * 获取Monitor实现，默认使用CompositeMonitor
     * 在Pigeon内部只提供了空调用的SimpleMonitor和监控组的CompositeMonitor
     * Monitor是一个SPI，Pigeon对此开放了自定义扩展的功能，以便我们结合其他监控系统来监控Pigeon应用
     * @see com.dianping.pigeon.monitor.CompositeMonitor
     */
    private static final Monitor monitor = MonitorLoader.getMonitor();

    private static final boolean isAccessLogEnabled = ConfigManagerLoader.getConfigManager()
            .getBooleanValue("pigeon.provider.accesslog.enable", false);

    private static final String KEY_LOG_SERVICE_EXCEPTION = "pigeon.provider.logserviceexception";

    private static final ProviderContextProcessor contextProcessor = ExtensionLoader
            .getExtension(ProviderContextProcessor.class);

    public MonitorProcessFilter() {
        ConfigManagerLoader.getConfigManager().getBooleanValue(KEY_LOG_SERVICE_EXCEPTION, true);
    }

    @Override
    public InvocationResponse invoke(ServiceInvocationHandler handler, ProviderContext invocationContext)
            throws Throwable {
        invocationContext.getTimeline().add(new TimePoint(TimePhase.O));
        InvocationRequest request = invocationContext.getRequest();
        ProviderChannel channel = invocationContext.getChannel();
        MonitorTransaction transaction = null;
        String fromIp = null;
        if (monitor != null) {
            String methodUri = null;
            try {
                ServiceMethod serviceMethod = ServiceMethodFactory.getMethod(request);
                invocationContext.setServiceMethod(serviceMethod);
                methodUri = InvocationUtils.getRemoteCallFullName(request.getServiceName(), request.getMethodName(),
                        serviceMethod.getOriginalParameterClasses());
            } catch (Throwable e) {
            }
            try {
                if (StringUtils.isBlank(methodUri)) {
                    methodUri = InvocationUtils.getRemoteCallFullName(request.getServiceName(), request.getMethodName(),
                            request.getParamClassName());
                }
                invocationContext.setMethodUri(methodUri);

                transaction = monitor.createTransaction("PigeonService", methodUri, invocationContext);
                if (transaction != null) {
                    transaction.setStatusOk();
                    monitor.setCurrentServiceTransaction(transaction);
                    transaction.logEvent("PigeonService.app", request.getApp(), "");
                    String parameters = "";
                    fromIp = channel.getRemoteAddress();
                    if (Constants.LOG_PARAMETERS) {
                        StringBuilder event = new StringBuilder();
                        event.append(InvocationUtils.toJsonString(request.getParameters(), 1000, 50));
                        parameters = event.toString();
                    }
                    transaction.logEvent("PigeonService.client", fromIp, parameters);
                    transaction.logEvent("PigeonService.QPS", "S" + Calendar.getInstance().get(Calendar.SECOND), "");
                    String reqSize = SizeMonitor.getInstance().getLogSize(request.getSize());
                    if (reqSize != null) {
                        transaction.logEvent("PigeonService.requestSize", reqSize, "" + request.getSize());
                    }
                    if (!Constants.PROTOCOL_DEFAULT.equals(channel.getProtocol())) {
                        transaction.addData("Protocol", channel.getProtocol());
                    }

                    if (SerializerType.isThrift(request.getSerialize())) {
                        monitor.logEvent("PigeonService.protocol", request.getApp(), fromIp);
                    }
                }
                ContextUtils.putLocalContext("CurrentServiceUrl",
                        request.getServiceName() + "#" + request.getMethodName());
            } catch (Throwable e) {
                monitor.logError(e);
            }
        }
        InvocationResponse response = null;
        try {
            try {
                response = handler.handle(invocationContext);
            } catch (RuntimeException e) {
                if (transaction != null) {
                    try {
                        transaction.setStatusError(e);
                    } catch (Throwable e2) {
                        monitor.logMonitorError(e2);
                    }
                }
                if (monitor != null) {
                    monitor.logError(e);
                }
            }
            if (transaction != null) {
                try {
                    if (response != null) {
                        String respSize = SizeMonitor.getInstance().getLogSize(response.getSize());
                        if (respSize != null) {
                            transaction.logEvent("PigeonService.responseSize", respSize, "" + response.getSize());
                        }
                    }
                    Map<String, Serializable> globalContext = ContextUtils.getGlobalContext();
                    if (!CollectionUtils.isEmpty(globalContext)) {
                        String sourceApp = (String) globalContext.get(Constants.CONTEXT_KEY_SOURCE_APP);
                        if (sourceApp != null) {
                            transaction.addData("SourceApp", sourceApp);
                        }
                        String sourceIp = (String) globalContext.get(Constants.CONTEXT_KEY_SOURCE_IP);
                        if (sourceIp != null) {
                            transaction.addData("SourceIp", sourceIp);
                        }
                    }
                    String from = (String) ContextUtils.getLocalContext("RequestIp");
                    if (from != null) {
                        transaction.logEvent("PigeonConsole.client", from, "");
                        String app = (String) ContextUtils.getLocalContext("RequestApp");
                        if (app != null) {
                            transaction.logEvent("PigeonConsole.app", app, "");
                        }
                    }
                    transaction.writeMonitorContext();
                } catch (Throwable e) {
                    monitor.logError(e);
                }
            }
        } finally {
            Throwable serviceError = invocationContext.getServiceError();
            if (serviceError != null && monitor != null) {
                monitor.logError(invocationContext.getServiceError());
            }
            Throwable frameworkError = invocationContext.getFrameworkError();
            if (frameworkError != null && monitor != null) {
                monitor.logError(frameworkError);
                transaction.setStatusError(frameworkError);
            }
            if (transaction != null) {
                invocationContext.getTimeline().add(new TimePoint(TimePhase.E, System.currentTimeMillis()));
                try {
                    transaction.complete();
                    if (isAccessLogEnabled) {
                        accessLogger.info(new StringBuilder().append(request.getApp()).append("@").append(fromIp)
                                .append("@").append(request).toString());
                    }
                } catch (Throwable e) {
                    monitor.logMonitorError(e);
                }
                monitor.clearServiceTransaction();
            }
            if (contextProcessor != null) {
                contextProcessor.clearContext();
            }
            ContextUtils.clearLocalContext();
            ContextUtils.clearRequestContext();
            ContextUtils.clearGlobalContext();
        }
        return response;
    }
}
