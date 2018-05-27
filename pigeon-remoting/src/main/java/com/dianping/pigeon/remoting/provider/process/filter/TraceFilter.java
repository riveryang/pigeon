package com.dianping.pigeon.remoting.provider.process.filter;

import com.dianping.pigeon.remoting.common.monitor.trace.ProviderMonitorData;
import com.dianping.pigeon.remoting.common.domain.InvocationRequest;
import com.dianping.pigeon.remoting.common.domain.InvocationResponse;
import com.dianping.pigeon.remoting.common.process.ServiceInvocationFilter;
import com.dianping.pigeon.remoting.common.process.ServiceInvocationHandler;
import com.dianping.pigeon.remoting.provider.domain.ProviderContext;

/**
 * @author qi.yin
 *         2016/11/14  下午5:45.
 */
public class TraceFilter implements ServiceInvocationFilter<ProviderContext> {

    public TraceFilter() {
    }

    @Override
    public InvocationResponse invoke(ServiceInvocationHandler handler, ProviderContext invocationContext) throws Throwable {

        InvocationRequest request = invocationContext.getRequest();
        /**
         * @see com.dianping.pigeon.remoting.provider.process.threadpool.RequestThreadPoolProcessor#doProcessRequest
         */
        ProviderMonitorData monitorData = (ProviderMonitorData) invocationContext.getMonitorData();

        // 默认情况下monitorData是有值的，如果在注册中心配置了 pigeon.provider.trace.enable = false，则直接调用此步骤
        if (monitorData == null) {
            return handler.handle(invocationContext);
        }

        /**
         * 设置TraceKeys
         * @see com.dianping.pigeon.remoting.common.monitor.trace.MethodKey
         */
        monitorData.trace();

        // 设置请求信息：调用类型、序列化方式、超时时间
        monitorData.setCallType((byte) request.getCallType());
        monitorData.setSerialize(request.getSerialize());
        monitorData.setTimeout(request.getTimeout());
        // 添加到调用跟踪列表
        monitorData.add();

        InvocationResponse response = null;

        try {
            response = handler.handle(invocationContext);
        } finally {
            // 调用是否异常
            // 在后续的ServiceInvocationFilter中调用时如果异常，会在ExceptionProcessFilter中捕获并设置异常信息
            if (invocationContext.getFrameworkError() != null) {
                monitorData.setIsSuccess(false);
            } else {
                monitorData.setIsSuccess(true);
            }
            // 调用跟踪结束，并记录结果
            monitorData.complete();
        }

        return response;

    }

}
