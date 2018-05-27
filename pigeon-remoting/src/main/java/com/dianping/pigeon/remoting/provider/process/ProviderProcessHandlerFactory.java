/**
 * Dianping.com Inc.
 * Copyright (c) 2003-2013 All Rights Reserved.
 */
package com.dianping.pigeon.remoting.provider.process;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.dianping.pigeon.remoting.provider.process.filter.*;
import com.dianping.pigeon.log.Logger;

import com.dianping.pigeon.log.LoggerLoader;
import com.dianping.pigeon.remoting.common.domain.Disposable;
import com.dianping.pigeon.remoting.common.domain.InvocationContext;
import com.dianping.pigeon.remoting.common.domain.InvocationResponse;
import com.dianping.pigeon.remoting.common.process.ServiceInvocationFilter;
import com.dianping.pigeon.remoting.common.process.ServiceInvocationHandler;
import com.dianping.pigeon.remoting.common.util.Constants;
import com.dianping.pigeon.remoting.provider.domain.ProviderContext;
import com.dianping.pigeon.remoting.provider.process.filter.SecurityFilter;
import com.dianping.pigeon.remoting.provider.process.filter.TraceFilter;

public final class ProviderProcessHandlerFactory {

    private static final Logger logger = LoggerLoader.getLogger(ProviderProcessHandlerFactory.class);

    private static List<ServiceInvocationFilter<ProviderContext>> bizProcessFilters = new LinkedList<ServiceInvocationFilter<ProviderContext>>();

    private static List<ServiceInvocationFilter<ProviderContext>> heartBeatProcessFilters = new LinkedList<ServiceInvocationFilter<ProviderContext>>();

    private static List<ServiceInvocationFilter<ProviderContext>> healthCheckProcessFilters = new LinkedList<ServiceInvocationFilter<ProviderContext>>();

    private static List<ServiceInvocationFilter<ProviderContext>> scannerHeartBeatProcessFilters = new LinkedList<ServiceInvocationFilter<ProviderContext>>();

    private static ServiceInvocationHandler bizInvocationHandler = null;

    private static ServiceInvocationHandler heartBeatInvocationHandler = null;

    private static ServiceInvocationHandler healthCheckInvocationHandler = null;

    private static ServiceInvocationHandler scannerHeartBeatInvocationHandler = null;

    public static ServiceInvocationHandler selectInvocationHandler(int messageType) {
        if (Constants.MESSAGE_TYPE_HEART == messageType) {
            return heartBeatInvocationHandler;
        } else if (Constants.MESSAGE_TYPE_HEALTHCHECK == messageType) {
            return healthCheckInvocationHandler;
        } else if (Constants.MESSAGE_TYPE_SCANNER_HEART == messageType) {
            return scannerHeartBeatInvocationHandler;
        } else {
            return bizInvocationHandler;
        }
    }

    /**
     * 创建调用链：<br>
     * 1. 事件处理器调用链<br>
     * 2. 心跳调用链<br>
     * 3. 健康检查调用链<br>
     * 4. 心跳扫描调用链
     */
    public static void init() {
        // 设置事件处理器的调用链
        // 1. 调用跟踪：记录成功次数和失败次数
        registerBizProcessFilter(new TraceFilter());
        // 2. 调用监控：结合外部监控系统，可以将调用日志发送至监控系统进行分析，比如Cat、MTrace
        // 默认情况下监控是开启的，可以通过配置属性 pigeon.monitor.enabled = false 来关闭
        if (Constants.MONITOR_ENABLE) {
            registerBizProcessFilter(new MonitorProcessFilter());
        }
        // 3. 调用结果回写客户端
        registerBizProcessFilter(new WriteResponseProcessFilter());
        // 4. 设置请求参数和响应结果
        registerBizProcessFilter(new ContextTransferProcessFilter());
        // 5. 异常处理
        registerBizProcessFilter(new ExceptionProcessFilter());
        // 6. 安全处理，黑名单、白名单、请求密钥
        registerBizProcessFilter(new SecurityFilter());
        // 7. 服务网关，主要是服务调用的限流
        registerBizProcessFilter(new GatewayProcessFilter());
        // 8. 业务实现调用
        registerBizProcessFilter(new BusinessProcessFilter());
        // 9. 创建事件处理器调用链
        bizInvocationHandler = createInvocationHandler(bizProcessFilters);

        // 设置心跳处理器的调用链
        // 1. 调用结果回写客户端
        registerHeartBeatProcessFilter(new WriteResponseProcessFilter());
        // 2. 心跳处理器
        registerHeartBeatProcessFilter(new HeartbeatProcessFilter());
        // 3. 创建心跳处理器调用链
        heartBeatInvocationHandler = createInvocationHandler(heartBeatProcessFilters);

        // 设置健康检查的调用链
        // 1. 调用结果回写客户端
        registerHealthCheckProcessFilter(new WriteResponseProcessFilter());
        // 2. 健康检查处理器
        registerHealthCheckProcessFilter(new HealthCheckProcessFilter());
        // 3. 创建健康检查调用链
        healthCheckInvocationHandler = createInvocationHandler(healthCheckProcessFilters);

        // 设置心跳扫描处理器的
        // 1. 调用结果回写客户端
        registerScannerHeartBeatProcessFilter(new WriteResponseProcessFilter());
        // 2. 心跳扫描处理器
        registerScannerHeartBeatProcessFilter(new ScannerHeartBeatProcessFilter());
        // 3. 创建心跳扫描调用链
        scannerHeartBeatInvocationHandler = createInvocationHandler(scannerHeartBeatProcessFilters);
    }

    @SuppressWarnings({ "rawtypes" })
    private static <K, V extends ServiceInvocationFilter> ServiceInvocationHandler createInvocationHandler(
            List<V> internalFilters) {
        ServiceInvocationHandler last = null;
        List<V> filterList = new ArrayList<V>();
        filterList.addAll(internalFilters);
        for (int i = filterList.size() - 1; i >= 0; i--) {
            final V filter = filterList.get(i);
            final ServiceInvocationHandler next = last;
            last = new ServiceInvocationHandler() {
                @SuppressWarnings("unchecked")
                @Override
                public InvocationResponse handle(InvocationContext invocationContext) throws Throwable {
                    return filter.invoke(next, invocationContext);
                }
            };
        }
        return last;
    }

    private static void registerBizProcessFilter(ServiceInvocationFilter<ProviderContext> filter) {
        bizProcessFilters.add(filter);
    }

    private static void registerHeartBeatProcessFilter(ServiceInvocationFilter<ProviderContext> filter) {
        heartBeatProcessFilters.add(filter);
    }

    private static void registerHealthCheckProcessFilter(ServiceInvocationFilter<ProviderContext> filter) {
        healthCheckProcessFilters.add(filter);
    }

    private static void registerScannerHeartBeatProcessFilter(ServiceInvocationFilter<ProviderContext> filter) {
        scannerHeartBeatProcessFilters.add(filter);
    }

    public static void destroy() {
        // 除去自定义的Filter外，bizProcessFilters里实现了Disposable接口的只有GatewayProcessFilter
        for (ServiceInvocationFilter<ProviderContext> filter : bizProcessFilters) {
            if (filter instanceof Disposable) {
                try {
                    ((Disposable) filter).destroy();
                } catch (Exception e) {
                }
            }
        }
        // 除去自定义的Filter外，heartBeatProcessFilters里没有实现Disposable接口的Filter
        for (ServiceInvocationFilter<ProviderContext> filter : heartBeatProcessFilters) {
            if (filter instanceof Disposable) {
                try {
                    ((Disposable) filter).destroy();
                } catch (Exception e) {
                }
            }
        }
        // 清理Filters列表
        bizProcessFilters.clear();
        heartBeatProcessFilters.clear();
    }
}
