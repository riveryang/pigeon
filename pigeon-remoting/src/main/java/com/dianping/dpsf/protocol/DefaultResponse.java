/**
 * Dianping.com Inc.
 * Copyright (c) 2003-2013 All Rights Reserved.
 */
package com.dianping.dpsf.protocol;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.dianping.dpsf.component.DPSFResponse;

/**
 * 不能修改packagename，修改属性需要注意，确保和之前的dpsf兼容。
 * 
 * @author jianhuihuang
 * @version $Id: DefaultResponse.java, v 0.1 2013-7-5 上午8:25:48 jianhuihuang Exp
 *          $
 */
public class DefaultResponse implements DPSFResponse {

	/**
	 * 不能随意修改！
	 */
	private static final long serialVersionUID = 4200559704846455821L;

	private transient byte serialize;

	private long seq;

	private int messageType;

	private String cause;

	private Object returnVal;

	private Object context;

	public DefaultResponse(int messageType, byte serialize) {
		this.messageType = messageType;
		this.serialize = serialize;
	}

	public DefaultResponse(byte serialize, long seq, int messageType, Object returnVal) {
		this.serialize = serialize;
		this.seq = seq;
		this.messageType = messageType;
		this.returnVal = returnVal;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.dianping.dpsf.net.component.DPSFSerializable#getSerializ()
	 */
	public byte getSerializ() {
		return this.serialize;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.dianping.dpsf.net.component.DPSFSerializable#setSequence(long)
	 */
	public void setSequence(long seq) {
		this.seq = seq;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.dianping.dpsf.net.component.DPSFSerializable#getSequence()
	 */
	public long getSequence() {
		return this.seq;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.dianping.dpsf.net.component.DPSFSerializable#getObject()
	 */
	public Object getObject() {
		return this;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.dianping.dpsf.net.component.DPSFResponse#setMessageType(int)
	 */
	public void setMessageType(int messageType) {
		this.messageType = messageType;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.dianping.dpsf.net.component.DPSFResponse#getMessageType()
	 */
	public int getMessageType() {
		return this.messageType;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.dianping.dpsf.component.DPSFResponse#getCause()
	 */
	public String getCause() {
		// TODO Auto-generated method stub
		return this.cause;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.dianping.dpsf.component.DPSFResponse#getReturn()
	 */
	public Object getReturn() {
		return this.returnVal;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.dianping.dpsf.component.DPSFSerializable#getContext()
	 */
	@Override
	public Object getContext() {
		return this.context;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.dianping.dpsf.component.DPSFSerializable#setContext(java.lang.Object)
	 */
	@Override
	public void setContext(Object context) {
		this.context = context;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.dianping.dpsf.component.DPSFResponse#setReturn(java.lang.Object)
	 */
	@Override
	public void setReturn(Object obj) {
		this.returnVal = obj;
	}

	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE);
	}

}
