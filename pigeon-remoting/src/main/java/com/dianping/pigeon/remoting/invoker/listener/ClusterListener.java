/**
 * Dianping.com Inc.
 * Copyright (c) 2003-2013 All Rights Reserved.
 */
package com.dianping.pigeon.remoting.invoker.listener;

import com.dianping.pigeon.remoting.invoker.Client;
import com.dianping.pigeon.remoting.invoker.component.ConnectInfo;

public interface ClusterListener {

	void addConnect(ConnectInfo cmd);

	void addConnect(ConnectInfo cmd, Client client);

	void removeConnect(Client client);

	void doNotUse(String serviceName, String host, int port);

}
