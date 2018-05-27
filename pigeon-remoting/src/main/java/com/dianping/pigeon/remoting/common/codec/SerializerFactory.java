/**
 * Dianping.com Inc.
 * Copyright (c) 2003-${year} All Rights Reserved.
 */
package com.dianping.pigeon.remoting.common.codec;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.dianping.pigeon.remoting.common.codec.protobuf.Protobuf3Serializer;
import org.apache.commons.lang.ClassUtils;

import com.dianping.pigeon.extension.ExtensionLoader;
import com.dianping.pigeon.log.Logger;
import com.dianping.pigeon.log.LoggerLoader;
import com.dianping.pigeon.remoting.common.codec.fst.FstSerializer;
import com.dianping.pigeon.remoting.common.codec.hessian.Hessian1Serializer;
import com.dianping.pigeon.remoting.common.codec.hessian.HessianSerializer;
import com.dianping.pigeon.remoting.common.codec.java.JavaSerializer;
import com.dianping.pigeon.remoting.common.codec.json.JacksonSerializer;
import com.dianping.pigeon.remoting.common.codec.protostuff.ProtostuffSerializer;
import com.dianping.pigeon.remoting.common.codec.thrift.ThriftSerializer;

/**
 * @author xiangwu
 * @Sep 5, 2013
 */
public final class SerializerFactory {

    private static final Logger logger = LoggerLoader.getLogger(SerializerFactory.class);

    private static volatile boolean isInitialized = false;

    private final static ConcurrentHashMap<String, Byte> serializerTypes = new ConcurrentHashMap<String, Byte>();
    private final static ConcurrentHashMap<Byte, Serializer> serializers = new ConcurrentHashMap<Byte, Serializer>();

    private SerializerFactory() {
    }

    static {
        init();
    }

    /**
     * Pigeon内部提供了一系列的序列化工具，并且提供了SerializerRegister这个SPI方便我们自定义扩展.
     */
    public static void init() {
        if (!isInitialized) {
            synchronized (SerializerFactory.class) {
                if (!isInitialized) {
                    // 初始化及注册序列化工具
                    // 1. 基于JDK的对象流实现的序列化工具
                    registerSerializer(SerializerType.JAVA, new JavaSerializer());
                    // 2. 基于Hessian 2.x的序列化工具
                    registerSerializer(SerializerType.HESSIAN, new HessianSerializer());
                    // 3. 基于Hessian 1.x的序列化工具
                    registerSerializer(SerializerType.HESSIAN1, new Hessian1Serializer());
                    // 4. 基于Protostuff的序列化工具
                    registerSerializer(SerializerType.PROTO, new ProtostuffSerializer());
                    // 5. 基于Thrift的序列化工具
                    registerSerializer(SerializerType.THRIFT, new ThriftSerializer());
                    // 6. 基于Protobuf 3.x的序列化工具
                    registerSerializer(SerializerType.PROTOBUF3, new Protobuf3Serializer());

                    try {
                        // 7. 基于FST的序列化工具
                        registerSerializer(SerializerType.FST, new FstSerializer());
                    } catch (Throwable t) {
                        logger.warn("failed to initialize fst serializer:" + t.getMessage());
                    }

                    boolean supportJackson = true;
                    try {
                        ClassUtils.getClass("com.fasterxml.jackson.databind.ObjectMapper");
                    } catch (ClassNotFoundException e) {
                        supportJackson = false;
                    }
                    if (supportJackson) {
                        try {
                            // 8. 基于Jackson的序列化工具
                            registerSerializer(SerializerType.JSON, new JacksonSerializer());
                        } catch (Throwable t) {
                            logger.warn("failed to initialize jackson serializer:" + t.getMessage());
                        }
                    }

                    // 9. 通过SerializerRegister SPI的自定义扩展，来设置自定义的一些序列化和反序列化的工具
                    List<SerializerRegister> serializerRegisters = ExtensionLoader.getExtensionList(SerializerRegister.class);
                    for (SerializerRegister serializerRegister : serializerRegisters) {
                        if (!serializerRegister.isRegistered()) {
                            serializerRegister.registerSerializer();
                        }
                    }

                    isInitialized = true;
                }
            }
        }
    }

    public static byte getSerialize(String serialize) {
        Byte result = serializerTypes.get(serialize);

        if (result != null) {
            return result;
        } else {
            throw new IllegalArgumentException("Only registered serialize type supported");
        }
    }

    public static void registerSerializer(SerializerType serializerType, Serializer serializer) {
        if (serializer == null) {
            throw new IllegalArgumentException("the serializer is null");
        }

        try {
            serializerTypes.putIfAbsent(serializerType.getName(), serializerType.getCode());
            serializers.putIfAbsent(serializerType.getCode(), serializer);
        } catch (Exception e) {
            logger.error("register serializer failed!", e);
            serializers.remove(serializerType.getCode(), serializer);
            serializerTypes.remove(serializerType.getName(), serializerType);
        }
    }

    @Deprecated
    public static void registerSerializer(String serializeName, byte serializerType, Serializer serializer) {
        if (serializer == null) {
            throw new IllegalArgumentException("the serializer is null");
        }

        try {
            serializerTypes.putIfAbsent(serializeName, serializerType);
            serializers.putIfAbsent(serializerType, serializer);
        } catch (Exception e) {
            logger.error("register serializer failed!", e);
            serializers.remove(serializerType, serializer);
            serializerTypes.remove(serializeName, serializerType);
        }
    }

    public static Serializer getSerializer(byte serializerType) {
        Serializer serializer = serializers.get(serializerType);
        if (serializer == null) {
            throw new IllegalArgumentException("no serializer found for type:" + serializerType);
        } else {
            return serializer;
        }
    }

    public static byte convertToUnifiedSerialize(byte serializerType) {
        if (SerializerType.isThrift(serializerType)) {
            return SerializerType.INTERNAL_THRIFT.getCode();
        } else {
            throw new IllegalArgumentException("Invalid serializer type :" + serializerType);
        }
    }

    public static byte convertToSerialize(byte serializerType) {
        if (SerializerType.isInternalThrift(serializerType)) {
            return SerializerType.THRIFT.getCode();
        } else {
            throw new IllegalArgumentException("Invalid serializer type:" + serializerType);
        }
    }

}
