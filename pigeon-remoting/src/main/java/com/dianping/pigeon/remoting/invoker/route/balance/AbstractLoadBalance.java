/**
 * Dianping.com Inc.
 * Copyright (c) 2003-2013 All Rights Reserved.
 */
package com.dianping.pigeon.remoting.invoker.route.balance;

import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;

import com.dianping.dpsf.component.DPSFRequest;
import com.dianping.pigeon.registry.cache.WeightCache;
import com.dianping.pigeon.remoting.common.exception.NetworkException;
import com.dianping.pigeon.remoting.common.util.Constants;
import com.dianping.pigeon.remoting.invoker.Client;
import com.dianping.pigeon.remoting.invoker.route.context.ClientContext;

public abstract class AbstractLoadBalance implements LoadBalance {

	private static final Logger logger = Logger.getLogger(AbstractLoadBalance.class);

	protected Random random = new Random();

	@Override
	public Client select(List<Client> clients, DPSFRequest request) {

		if (clients == null || clients.isEmpty()) {
			return null;
		}
		Client selectedClient = null;
		String forceAddress = ClientContext.getUseClientAddress();
		if (forceAddress != null && forceAddress.length() > 0) {
			// 客户端强制路由
			for (Client client : clients) {
				if (forceAddress.equals(client.getAddress())) {
					selectedClient = client;
					break;
				}
			}
			if (selectedClient == null) {
				throw new NetworkException("Force used server[" + forceAddress + "] is not connected for service["
						+ request.getServiceName() + "].");
			}
		} else {
			if (clients.size() == 1) {
				selectedClient = clients.get(0);
			} else {
				try {
					selectedClient = doSelect(clients, request, getWeights(clients, request.getServiceName()));
				} catch (Exception e) {
					logger.error("Failed to do load balance[" + getClass().getName() + "], detail: " + e.getMessage()
							+ ", use random instead.", e);
					selectedClient = clients.get(random.nextInt(clients.size()));
				}
			}
		}
		if (selectedClient != null) {
			int weight = WeightCache.getInstance().getWeightWithDefault(request.getServiceName(),
					selectedClient.getAddress());
			request.setAttachment(Constants.REQ_ATTACH_FLOW, 1.0f / (weight > 0 ? weight : 1));
		}
		return selectedClient;
	}

	/**
	 * [w1, w2, w3, maxWeightIndex]
	 * 
	 * @param clients
	 * @param serviceName
	 * @param weightAccessor
	 * @return
	 */
	private int[] getWeights(List<Client> clients, String serviceName) {
		int clientSize = clients.size();
		int[] weights = new int[clientSize + 1];
		int maxWeightIdx = 0;
		int maxWeight = Integer.MIN_VALUE;
		for (int i = 0; i < clientSize; i++) {
			weights[i] = WeightCache.getInstance().getWeightWithDefault(serviceName, clients.get(i).getAddress());
			if (weights[i] > maxWeight) {
				maxWeight = weights[i];
				maxWeightIdx = i;
			}
		}
		weights[clientSize] = maxWeightIdx;
		return weights;
	}

	protected abstract Client doSelect(List<Client> clients, DPSFRequest request, int[] weights);

}
