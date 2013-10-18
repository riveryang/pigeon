/**
 * Dianping.com Inc.
 * Copyright (c) 2003-${year} All Rights Reserved.
 */
package com.dianping.pigeon.remoting.provider.filter;

import org.apache.log4j.Logger;

import com.dianping.dpsf.component.DPSFRequest;
import com.dianping.dpsf.component.DPSFResponse;
import com.dianping.pigeon.remoting.common.filter.ServiceInvocationFilter;
import com.dianping.pigeon.remoting.common.filter.ServiceInvocationHandler;
import com.dianping.pigeon.remoting.common.util.Constants;
import com.dianping.pigeon.remoting.provider.component.ProviderChannel;
import com.dianping.pigeon.remoting.provider.component.context.ProviderContext;
import com.dianping.pigeon.util.ContextUtils;

/**
 * 
 * 
 * @author jianhuihuang
 * @version $Id: WriteResponseProcessFilter.java, v 0.1 2013-6-20 下午5:46:19
 *          jianhuihuang Exp $
 */
public class WriteResponseProcessFilter implements ServiceInvocationFilter<ProviderContext> {

	private static final Logger logger = Logger.getLogger(WriteResponseProcessFilter.class);

	@Override
	public DPSFResponse invoke(ServiceInvocationHandler handler, ProviderContext invocationContext)
			throws Throwable {
		if (logger.isInfoEnabled()) {
			logger.info("invoke the WriteResponseProcessFilter, invocationContext:" + invocationContext);
		}
		try {
			ProviderChannel channel = invocationContext.getChannel();
			DPSFRequest request = invocationContext.getRequest();
			DPSFResponse response = handler.handle(invocationContext);
			if (request.getCallType() == Constants.CALLTYPE_REPLY) {
				channel.write(response);
			}
			return response;
		} finally {
			ContextUtils.clearContext();
			ContextUtils.clearLocalContext();
		}
	}

}
