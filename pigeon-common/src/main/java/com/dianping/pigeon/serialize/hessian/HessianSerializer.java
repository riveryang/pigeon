/**
 * Dianping.com Inc.
 * Copyright (c) 2003-2013 All Rights Reserved.
 */
package com.dianping.pigeon.serialize.hessian;

import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.lang.SerializationException;

import com.caucho.hessian.io.Hessian2Input;
import com.caucho.hessian.io.Hessian2Output;
import com.dianping.pigeon.serialize.Serializer;

/**
 * @author xiangwu
 * @Sep 5, 2013
 * 
 */
public class HessianSerializer implements Serializer {

	@Override
	public Object deserialize(InputStream is) throws SerializationException {
		Hessian2Input h2in = new Hessian2Input(is);
		try {
			try {
				return h2in.readObject();
			} finally {
				h2in.close();
			}
		} catch (Throwable t) {
			throw new SerializationException(t);
		}

	}

	@Override
	public void serialize(OutputStream os, Object obj) throws SerializationException {
		Hessian2Output h2out = new Hessian2Output(os);
		try {
			try {
				h2out.writeObject(obj);
				h2out.flush();
			} finally {
				h2out.close();
			}
		} catch (Throwable t) {
			throw new SerializationException(t);
		}
	}

}
