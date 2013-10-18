/**
 * Dianping.com Inc.
 * Copyright (c) 2003-2013 All Rights Reserved.
 */
package com.dianping.pigeon.remoting.netty.invoker;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import com.dianping.dpsf.component.DPSFRequest;
import com.dianping.dpsf.component.DPSFResponse;
import com.dianping.pigeon.event.EventManager;
import com.dianping.pigeon.event.RuntimeServiceEvent;
import com.dianping.pigeon.remoting.common.exception.NetworkException;
import com.dianping.pigeon.remoting.common.exception.ServiceException;
import com.dianping.pigeon.remoting.common.util.Constants;
import com.dianping.pigeon.remoting.common.util.ResponseUtils;
import com.dianping.pigeon.remoting.invoker.Client;
import com.dianping.pigeon.remoting.invoker.component.ConnectInfo;
import com.dianping.pigeon.remoting.invoker.component.RpcInvokeInfo;
import com.dianping.pigeon.remoting.invoker.component.async.CallFuture;
import com.dianping.pigeon.remoting.invoker.component.async.Callback;
import com.dianping.pigeon.remoting.invoker.config.InvokerConfigurer;
import com.dianping.pigeon.remoting.invoker.service.ClientManager;
import com.dianping.pigeon.remoting.invoker.util.RpcEventUtils;
import com.dianping.pigeon.threadpool.DefaultThreadFactory;

public class NettyClient implements Client {

	private static final Logger logger = Logger.getLogger(NettyClient.class);

	private ClientBootstrap bootstrap;

	private ClientManager clientManager = ClientManager.getInstance();

	private String serviceName;

	private Channel channel;

	private String host;

	private int port = 4625;

	private String address;

	private static final int connectTimeout = 500;

	private volatile boolean connected = false;

	private volatile boolean closed = false;

	private volatile boolean active = true;
	private volatile boolean activeSetable = false;

	private ConnectInfo connectInfo;

	public static final int CLIENT_CONNECTIONS = Runtime.getRuntime().availableProcessors();

	public NettyClient(ConnectInfo cmd) {
		this.serviceName = cmd.getServiceName();
		this.host = cmd.getHost();
		this.port = cmd.getPort();
		this.connectInfo = cmd;
		this.address = host + ConnectInfo.PLACEHOLDER + port;

		ExecutorService bossExecutor = Executors.newCachedThreadPool(new DefaultThreadFactory(
				Constants.THREADNAME_CLIENT_NETTY_BOSS_EXECUTOR));

		ExecutorService workExecutor = Executors.newCachedThreadPool(new DefaultThreadFactory(
				Constants.THREADNAME_CLIENT_NETTY_WORKER_EXECUTOR));

		this.bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(bossExecutor, workExecutor));
		this.bootstrap.setOption("writeBufferHighWaterMark", InvokerConfigurer.getWriteBufferHighWater());
		this.bootstrap.setOption("writeBufferLowWaterMark", InvokerConfigurer.getWriteBufferLowWater());
		this.bootstrap.setPipelineFactory(new ClientChannelPipelineFactory(this));
	}

	public synchronized void connect() throws NetworkException {

		if (this.connected || this.closed) {
			return;
		}
		ChannelFuture future = bootstrap.connect(new InetSocketAddress(host, port));
		if (future.awaitUninterruptibly(connectTimeout, TimeUnit.MILLISECONDS)) {
			if (future.isSuccess()) {
				logger.warn("Client is connected to " + this.host + ":" + this.port);
				this.connected = true;
			} else {
				logger.error("Client is not connected to " + this.host + ":" + this.port);
				// logger.error("eeeee", new Exception("just for test"));

			}
		}
		this.channel = future.getChannel();
	}

	public CallFuture write(DPSFRequest request, Callback callback) {

		Object[] msg = new Object[] { request, callback };
		ChannelFuture future = null;

		if (channel == null) {
			logger.error("channel:" + null + " ^^^^^^^^^^^^^^");
		} else {
			future = channel.write(msg);
			if (request.getMessageType() == Constants.MESSAGE_TYPE_SERVICE
					|| request.getMessageType() == Constants.MESSAGE_TYPE_HEART) {
				future.addListener(new MsgWriteListener(request));
			}

		}
		// String invokeId = null;
		// String result = "";
		// String caller_appname = "";
		// String caller_host = "";
		// String caller_ip = "";
		// String caller_rqtime = "";
		// String protocol = "dpsf";
		// // String uniqueName = "";
		// String interfaze = request.getServiceName();
		// String methodName = request.getMethodName();
		// long durationMs = System.currentTimeMillis() -
		// request.getCreateMillisTime();
		//
		// if (request.getUniformContextHeaders() != null) {
		// Caller caller = request.getUniformContextHeaders().getCaller();
		// invokeId = request.getUniformContextHeaders().getInvokeId();
		// caller_appname = caller.getAppName();
		// caller_host = caller.getHostName();
		// caller_ip = caller.getIp();
		// caller_rqtime = caller.getRequestTime();
		// }
		//
		// String invokeType = request.getCallType() + "";
		// String targetUrl = "";
		// String client_name = "";
		// ExtensionLoader.getExtension(ServiceLogMonitor.class).invokeLog(invokeId,
		// result, protocol, targetUrl,
		// client_name, interfaze, methodName, invokeType, caller_appname,
		// caller_host, caller_ip, caller_rqtime,
		// durationMs, false);

		// if (callback != null) {
		// return callback.getFuture(future);
		// } else {
		return null;
		// }
	}

	public void write(DPSFRequest message) {
		write(message, null);
	}

	public void connectionException(Object attachment, Throwable e) {
		this.connected = false;
		connectionException(this, attachment, e);
	}

	private void connectionException(Client client, Object attachment, Throwable e) {

		if (attachment == null) {
			return;
		}
		Object[] msg = (Object[]) attachment;
		if (msg[0] instanceof DPSFRequest && ((DPSFRequest) msg[0]).getMessageType() == Constants.MESSAGE_TYPE_SERVICE
				&& msg[1] != null) {

			try {
				DPSFRequest request = (DPSFRequest) msg[0];
				Callback callback = (Callback) msg[2];
				if (client != null) {
					error(request, client);
					client.write(request, callback);
				} else {
					logger.error("no client for use to " + request.getServiceName());
				}
			} catch (NetworkException ne) {
				logger.error(ne.getMessage(), ne);
			}
			logger.error(e.getMessage(), e);
		}
	}

	private void error(DPSFRequest request, Client client) {

		RpcInvokeInfo rpcInvokeInfo = new RpcInvokeInfo();
		rpcInvokeInfo.setServiceName(request.getServiceName());
		rpcInvokeInfo.setAddressIp(client.getAddress());
		rpcInvokeInfo.setRequest(request);
		RuntimeServiceEvent event = new RuntimeServiceEvent(
				RuntimeServiceEvent.Type.RUNTIME_RPC_INVOKE_CONNECT_EXCEPTION, rpcInvokeInfo);

		EventManager.getInstance().publishEvent(event);
	}

	public void doResponse(DPSFResponse response) {
		try {
			this.clientManager.processResponse(response, this);
		} catch (ServiceException e) {
			logger.error("doResponse", e);
		}
	}

	/**
	 * @return the connected
	 */
	public boolean isConnected() {
		return connected;
	}

	public boolean isActive() {
		return active;
	}

	public void setActive(boolean active) {
		if (this.activeSetable) {
			this.active = active;
		}
	}

	public void setActiveSetable(boolean activeSetable) {
		this.activeSetable = activeSetable;
	}

	@Override
	public boolean isWritable() {
		return this.channel.isWritable();
	}

	/**
	 * @return the host
	 */
	public String getHost() {
		return host;
	}

	public int getPort() {

		return this.port;
	}

	/**
	 * @return the address
	 */
	public String getAddress() {
		return address;
	}

	public boolean equals(Object obj) {
		if (obj instanceof NettyClient) {
			NettyClient nc = (NettyClient) obj;
			return this.address.equals(nc.getAddress());
		} else {
			return super.equals(obj);
		}
	}

	@Override
	public int hashCode() {
		return address.hashCode();
	}

	@Override
	public void close() {
		closed = true;
		channel.close();
	}

	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE);
	}

	public class MsgWriteListener implements ChannelFutureListener {

		private DPSFRequest request;

		public MsgWriteListener(DPSFRequest request) {
			this.request = request;
		}

		public void operationComplete(ChannelFuture future) throws Exception {
			if (future.isSuccess()) {
				return;
			}
			logger.error("MsgWriteListener........future" + future.isSuccess());
			if (request.getMessageType() != Constants.MESSAGE_TYPE_HEART) {

				connected = false;
			}

			RpcEventUtils.channelOperationComplete(request, NettyClient.this.address);
			DPSFResponse response = ResponseUtils.createFailResponse(request, future.getCause());
			doResponse(response);
		}

	}

	@Override
	public String getServiceName() {
		return serviceName;
	}

	@Override
	public ConnectInfo getConnectInfo() {
		return connectInfo;
	}
}
