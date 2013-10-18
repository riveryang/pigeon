/**
 * Dianping.com Inc.
 * Copyright (c) 2003-2013 All Rights Reserved.
 */
package com.dianping.pigeon.remoting.invoker.component.context;

import java.lang.reflect.Method;

import com.dianping.pigeon.remoting.common.component.context.AbstractInvocationContext;
import com.dianping.pigeon.remoting.invoker.Client;
import com.dianping.pigeon.remoting.invoker.component.InvokerMetaData;

public class DefaultInvokerContext extends AbstractInvocationContext implements InvokerContext {

	private InvokerMetaData metaData;
	private Method method;
	private Object[] arguments;
	private Client client;

	public DefaultInvokerContext(InvokerMetaData metaData, Method method, Object[] arguments) {
		super(null);
		this.metaData = metaData;
		this.method = method;
		this.arguments = arguments;
	}

	public InvokerMetaData getMetaData() {
		return metaData;
	}

	public Method getMethod() {
		return method;
	}

	public Object[] getArguments() {
		return arguments;
	}

	@Override
	public Client getClient() {
		return client;
	}

	@Override
	public void setClient(Client client) {
		this.client = client;
	}

}
