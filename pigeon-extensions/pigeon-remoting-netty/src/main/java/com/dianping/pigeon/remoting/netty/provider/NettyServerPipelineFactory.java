/**
 * Dianping.com Inc.
 * Copyright (c) 2003-${year} All Rights Reserved.
 */
package com.dianping.pigeon.remoting.netty.provider;

import static org.jboss.netty.channel.Channels.pipeline;

import com.dianping.pigeon.remoting.common.codec.CodecConfig;
import com.dianping.pigeon.remoting.common.codec.CodecConfigFactory;
import com.dianping.pigeon.remoting.netty.codec.CompressHandler;
import com.dianping.pigeon.remoting.netty.codec.Crc32Handler;
import com.dianping.pigeon.remoting.netty.codec.FrameDecoder;
import com.dianping.pigeon.remoting.netty.codec.FramePrepender;
import com.dianping.pigeon.remoting.netty.provider.codec.*;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;


public class NettyServerPipelineFactory implements ChannelPipelineFactory {

    private NettyServer server;

    private static CodecConfig codecConfig = CodecConfigFactory.createClientConfig();

    public NettyServerPipelineFactory(NettyServer server) {
        this.server = server;
    }

    public ChannelPipeline getPipeline() {
        ChannelPipeline pipeline = pipeline();
        // Netty的数据发送处理
        pipeline.addLast("framePrepender", new FramePrepender());
        // Netty的解码器
        // 参考文章： https://blog.csdn.net/xiaolang85/article/details/12621663
        pipeline.addLast("frameDecoder", new FrameDecoder());
        // Netty的消息处理器，用户数据校验
        // 参考文章： https://baike.baidu.com/item/CRC32/7460858?fr=aladdin
        pipeline.addLast("crc32Handler", new Crc32Handler(codecConfig));
        // Netty的消息处理器，用于数据接收的解压缩和数据发送的压缩处理
        pipeline.addLast("compressHandler", new CompressHandler(codecConfig));
        // 初始化反序列化
        pipeline.addLast("providerDecoder", new ProviderDecoder());
        // 初始化序列化
        pipeline.addLast("providerEncoder", new ProviderEncoder());
        // 初始化事件处理器，所有的Netty请求和发送都会通过此Handler进行处理
        pipeline.addLast("serverHandler", new NettyServerHandler(server));
        return pipeline;
    }

}
