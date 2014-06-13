/**
 * Dianping.com Inc.
 * Copyright (c) 2003-2013 All Rights Reserved.
 */
package com.dianping.pigeon.remoting.common.util;

import com.dianping.pigeon.config.ConfigManagerLoader;

/**
 * Pigeon使用到的静态变量
 * 
 * @author jianhuihuang，saber miao
 * @version $Id: Constants.java, v 0.1 2013-6-17 上午10:59:21 jianhuihuang Exp $
 */
public final class Constants {

	public Constants() {
	}

	// 消息类型----》心跳消息
	public static final int MESSAGE_TYPE_HEART = 1;
	public static final int MESSAGE_TYPE_SERVICE = 2;
	public static final int MESSAGE_TYPE_EXCEPTION = 3;
	public static final int MESSAGE_TYPE_SERVICE_EXCEPTION = 4;
	public static final int MESSAGE_TYPE_HEALTHCHECK = 5;

	public static final int CALLTYPE_REPLY = 1;
	public static final int CALLTYPE_NOREPLY = 2;

	public static final String CALL_SYNC = "sync";
	public static final String CALL_CALLBACK = "callback";
	public static final String CALL_ONEWAY = "oneway";
	public static final String CALL_FUTURE = "future";

	public static final String CLUSTER_FAILFAST = "failfast";
	public static final String CLUSTER_FAILOVER = "failover";
	public static final String CLUSTER_FAILSAFE = "failsafe";

	public static final String SERIALIZE_JAVA = "java";
	public static final String SERIALIZE_HESSIAN = "hessian";
	public static final String SERIALIZE_HESSIAN1 = "hessian1";
	public static final String SERIALIZE_PROTOBUF = "protobuf";
	public static final String SERIALIZE_JSON = "json";

	public static final byte MESSAGE_HEAD_FIRST = 57;
	public static final byte MESSAGE_HEAD_SECOND = 58;
	public static final byte[] MESSAGE_HEAD = new byte[] { MESSAGE_HEAD_FIRST, MESSAGE_HEAD_SECOND };

	public static final byte EXPAND_FLAG_FIRST = 29;
	public static final byte EXPAND_FLAG_SECOND = 30;
	public static final byte EXPAND_FLAG_THIRD = 31;
	public static final byte[] EXPAND_FLAG = new byte[] { EXPAND_FLAG_FIRST, EXPAND_FLAG_SECOND, EXPAND_FLAG_THIRD };

	public static final int ATTACHMENT_RETRY = 1;
	public static final int ATTACHMENT_BYTEBUFFER = 2;
	public static final int ATTACHMENT_REQUEST_SEQ = 11;

	public static final String TRANSFER_NULL = "NULL";

	public static final String REQ_ATTACH_FLOW = "FLOW";
	public static final String REQ_ATTACH_WRITE_BUFF_LIMIT = "WRITE_BUFF_LIMIT";

	public static final int VERSION_150 = 150;

	public static final String REQUEST_CREATE_TIME = "requestCreateTime";
	public static final String REQUEST_TIMEOUT = "requestTimeout";
	public static final String REQUEST_FIRST_FLAG = "requestFirstFlag";

	public static final String ECHO_METHOD = "$echo";

	public static final int DEFAULT_FAILOVER_RETRY = 1;
	public static final boolean DEFAULT_FAILOVER_TIMEOUT_RETRY = false;

	public static final String CONFIG_CLUSTER_CLUSTER = "cluster";
	public static final String CONFIG_CLUSTER_RETRY = "retry";
	public static final String CONFIG_CLUSTER_TIMEOUT_RETRY = "timeout-retry";

	public static final String CONTEXT_FUTURE = "Context-Future";
	public static final String CONTEXT_SERVER_COST = "Context-Server-Cost";

	// Deafult value for the above keys
	public static final String DEFAULT_GROUP = "";

	public static final String KEY_LOADBALANCE = "pigeon.loadbalance.default";
	public static final String KEY_RECONNECT_INTERVAL = "pigeon.reconnect.interval";
	public static final String KEY_HEARTBEAT_INTERVAL = "pigeon.heartbeat.interval";
	public static final String KEY_HEARTBEAT_TIMEOUT = "pigeon.heartbeat.timeout";
	public static final String KEY_HEARTBEAT_DEADTHRESHOLD = "pigeon.heartbeat.dead.threshold";
	public static final String KEY_HEARTBEAT_HEALTHTHRESHOLD = "pigeon.heartbeat.health.threshold";
	public static final String KEY_HEARTBEAT_AUTOPICKOFF = "pigeon.heartbeat.autopickoff";
	public static final String KEY_SERVICE_NAMESPACE = "pigeon.service.namespace";
	public static final String KEY_MONITOR_ENABLED = "pigeon.monitor.enabled";
	public static final String KEY_INVOKER_MAXREQUESTS = "pigeon.invoker.maxrequests";
	public static final String KEY_PROVIDER_COREPOOLSIZE = "pigeon.provider.corePoolSize";
	public static final String KEY_PROVIDER_MAXPOOLSIZE = "pigeon.provider.maxPoolSize";
	public static final String KEY_PROVIDER_WORKQUEUESIZE = "pigeon.provider.workQueueSize";
	public static final String KEY_RESPONSE_COREPOOLSIZE = "pigeon.response.corePoolSize";
	public static final String KEY_RESPONSE_MAXPOOLSIZE = "pigeon.response.maxPoolSize";
	public static final String KEY_RESPONSE_WORKQUEUESIZE = "pigeon.response.workQueueSize";
	public static final String KEY_INVOKER_TIMEOUT = "pigeon.invoker.timeout";
	public static final String KEY_TIMEOUT_INTERVAL = "pigeon.timeout.interval";
	public static final String KEY_WRITE_BUFFER_HIGH_WATER = "pigeon.channel.writebuff.high";
	public static final String KEY_WRITE_BUFFER_LOW_WATER = "pigeon.channel.writebuff.low";
	public static final String KEY_DEFAULT_WRITE_BUFF_LIMIT = "pigeon.channel.writebuff.defaultlimit";
	public static final String KEY_MANAGER_ADDRESS = "pigeon.manager.address";
	public static final String KEY_NOTIFY_ENABLE = "pigeon.notify.enable";
	public static final String KEY_TEST_ENABLE = "pigeon.test.enable";
	public static final String KEY_CONNECT_TIMEOUT = "pigeon.netty.connecttimeout";
	public static final String KEY_WEIGHT_WARMUPPERIOD = "pigeon.weight.warmupperiod";
	public static final String KEY_WEIGHT_STARTDELAY = "pigeon.weight.startdelay";

	public static final int DEFAULT_INVOKER_TIMEOUT = 5000;
	public static final int DEFAULT_PROVIDER_COREPOOLSIZE = 150;
	public static final int DEFAULT_PROVIDER_MAXPOOLSIZE = 300;
	public static final int DEFAULT_PROVIDER_WORKQUEUESIZE = 300;
	public static final int DEFAULT_RESPONSE_COREPOOLSIZE = 30;
	public static final int DEFAULT_RESPONSE_MAXPOOLSIZE = 300;
	public static final int DEFAULT_RESPONSE_WORKQUEUESIZE = 200;
	public static final long DEFAULT_RECONNECT_INTERVAL = 1000;
	public static final long DEFAULT_HEARTBEAT_INTERVAL = 1000;
	public static final long DEFAULT_HEARTBEAT_TIMEOUT = 3000;
	public static final long DEFAULT_HEARTBEAT_DEADCOUNT = 5;
	public static final long DEFAULT_HEARTBEAT_HEALTHCOUNT = 5;
	public static final boolean DEFAULT_HEARTBEAT_AUTOPICKOFF = true;
	public static final String DEFAULT_SERVICE_NAMESPACE = "http://service.dianping.com/";
	public static final int DEFAULT_WRITE_BUFFER_HIGH_WATER = 35 * 1024 * 1024;
	public static final int DEFAULT_WRITE_BUFFER_LOW_WATER = 25 * 1024 * 1024;
	public static final boolean DEFAULT_WRITE_BUFF_LIMIT = false;
	public static final String DEFAULT_PROCESS_TYPE = "threadpool";
	public static final long DEFAULT_TIMEOUT_INTERVAL = 1000;
	public static final String DEFAULT_MANAGER_ADDRESS = "lionapi.dp:8080";
	public static final boolean DEFAULT_NOTIFY_ENABLE = true;
	public static final boolean DEFAULT_TEST_ENABLE = true;
	public static final int DEFAULT_CONNECT_TIMEOUT = 500;
	public static final int DEFAULT_WEIGHT_WAMUPPERIOD = 1000;
	public static final int DEFAULT_WEIGHT_STARTDELAY = 30000;

	public static final String PROTOCOL_HTTP = "http";
	public static final String PROTOCOL_DEFAULT = "default";
	public static final String KEY_UNPUBLISH_WAITTIME = "pigeon.unpublish.waittime";
	public static final int DEFAULT_UNPUBLISH_WAITTIME = 3000;
	public static final String KEY_ONLINE_WHILE_INITIALIZED = "pigeon.online.whileinitialized";
	public static final boolean DEFAULT_ONLINE_WHILE_INITIALIZED = false;
	public static final boolean DEFAULT_TIMEOUT_CANCEL = false;
	public static final String KEY_TIMEOUT_CANCEL = "pigeon.timeout.cancel";
	public static final String KEY_FORCE_CANCEL_MULTIPLE = "pigoen.timeout.forcecancel.multiple";
	public static final int DEFAULT_FORCE_CANCEL_MULTIPLE = 5;
	public static final String KEY_FORCE_CANCEL_MINIMUM = "pigeon.timeout.forcecancel.minimum";
	public static final int DEFAULT_FORCE_CANCEL_MINIMUM = 5000;
	public static final int DEFAULT_STRING_MAXLENGTH = 1000;
	public static final String KEY_STRING_MAXLENGTH = "pigeon.string.maxlength";
	public static final int DEFAULT_STRING_MAXITEMS = 500;
	public static final String KEY_STRING_MAXITEMS = "pigeon.string.maxitems";
	public static final boolean DEFAULT_ONLINE_AUTO = true;
	public static final String KEY_ONLINE_AUTO = "pigeon.online.auto";
	public static final String KEY_WEIGHT_INITIAL = "pigeon.weight.initial";
	public static final int DEFAULT_WEIGHT_INITIAL = 0;
	public static final String KEY_WEIGHT_START = "pigeon.weight.start";
	public static final int DEFAULT_WEIGHT_START = 1;
	public static final String KEY_WEIGHT_DEFAULT = "pigeon.weight.default";
	public static final int DEFAULT_WEIGHT_DEFAULT = 1;
	public static final String KEY_STATUS_CHECKINVOKERAVAILABLE = "pigeon.status.checkinvokeravailable";
	public static final boolean DEFAULT_STATUS_CHECKINVOKERAVAILABLE = true;
	public static final String KEY_SERVICEWARMUP_ENABLE = "pigeon.servicewarmup.enable";
	public static final String KEY_AUTOREGISTER_ENABLE = "pigeon.autoregister.enable";

	public static final int WEIGHT_INITIAL = ConfigManagerLoader.getConfigManager().getIntValue(
			Constants.KEY_WEIGHT_INITIAL, Constants.DEFAULT_WEIGHT_INITIAL);
	
	public static final int WEIGHT_START = ConfigManagerLoader.getConfigManager().getIntValue(Constants.KEY_WEIGHT_START,
			Constants.DEFAULT_WEIGHT_START);

	public static final int WEIGHT_DEFAULT = ConfigManagerLoader.getConfigManager().getIntValue(Constants.KEY_WEIGHT_DEFAULT,
			Constants.DEFAULT_WEIGHT_DEFAULT);

}
