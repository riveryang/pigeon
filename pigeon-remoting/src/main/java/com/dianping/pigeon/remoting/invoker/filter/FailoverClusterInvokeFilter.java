/**
 * Dianping.com Inc.
 * Copyright (c) 2003-2013 All Rights Reserved.
 */
package com.dianping.pigeon.remoting.invoker.filter;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.dianping.dpsf.component.DPSFRequest;
import com.dianping.dpsf.component.DPSFResponse;
import com.dianping.pigeon.exception.PigeonRuntimeException;
import com.dianping.pigeon.remoting.common.exception.NetworkException;
import com.dianping.pigeon.remoting.common.exception.NetworkTimeoutException;
import com.dianping.pigeon.remoting.common.filter.ServiceInvocationHandler;
import com.dianping.pigeon.remoting.invoker.Client;
import com.dianping.pigeon.remoting.invoker.component.InvokerMetaData;
import com.dianping.pigeon.remoting.invoker.component.context.InvokerContext;
import com.dianping.pigeon.remoting.invoker.route.context.ClientContext;

/**
 * 调用出错，则进行该Service剩余Provider的重试 Note：该策略仅适用于只读业务，有写操作的业务不建议使用，可能产生重复数据
 * 
 * @author jianhuihuang
 * @version $Id: FailoverClusterInvokeFilter.java, v 0.1 2013-7-22 下午8:51:55
 *          jianhuihuang Exp $
 */
public class FailoverClusterInvokeFilter extends ClusterInvokeFilter {

	public static final String NAME = "fail-over";

	private static final Logger logger = Logger.getLogger(ClusterInvokeFilter.class);

	@Override
	public DPSFResponse _invoke(ServiceInvocationHandler handler, InvokerContext invocationContext)
			throws Throwable {

		InvokerMetaData metaData = invocationContext.getMetaData();

		List<Client> selectedClients = new ArrayList<Client>();
		Throwable lastError = null;
		int retry = metaData.getRetries();

		int maxInvokeTimes = retry;
		boolean timeoutRetry = metaData.isTimeoutRetry();

		boolean nextInvokeErrorExit = false;
		int invokeTimes = 0;
		for (int index = 0; index < maxInvokeTimes; index++) {
			DPSFRequest request = createRemoteCallRequest(invocationContext, metaData);
			Client clientSelected = null;
			try {
				clientSelected = clientManager.getClient(metaData, request, selectedClients);
			} catch (NetworkException e) {
				if (index > 0) {
					throw new NetworkException("After " + (index + 1) + " times invocation: " + e.getMessage());
				}
			}
			selectedClients.add(clientSelected);
			try {
				invokeTimes++;
				invocationContext.setClient(clientSelected);
				DPSFResponse response = handler.handle(invocationContext);
				if (lastError != null) {
					logger.warn(
							"Retry method[" + invocationContext.getMethod().getName() + "] on service["
									+ metaData.getServiceName() + "] succeed after " + invokeTimes
									+ " times, last failed invoke's error: " + lastError.getMessage(), lastError);
				}
				return response;
			} catch (Throwable e) {
				// 若指定强制调用某机器，则不再重试
				if (ClientContext.getUseClientAddress() != null) {
					throw e;
				}

				lastError = e;
				if (nextInvokeErrorExit) {
					break;
				}
				if (e instanceof NetworkTimeoutException) {
					if (!timeoutRetry) {
						throw e;
					} else {
						nextInvokeErrorExit = true; // 超时最多重试一次
					}
				}
			}
		}
		throw new PigeonRuntimeException("Invoke method[" + invocationContext.getMethod().getName() + "] on service["
				+ metaData.getServiceName() + "] failed with " + invokeTimes + " times, last error: "
				+ (lastError != null ? lastError.getMessage() : ""),
				lastError != null && lastError.getCause() != null ? lastError.getCause() : lastError);
	}

	@Override
	public String name() {
		return NAME;
	}

}
