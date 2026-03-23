package com.logplatform.collector.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.charset.StandardCharsets;

@Configuration
public class NettyServer {

    private static final Logger log = LoggerFactory.getLogger(NettyServer.class);

    @Value("${netty.port}")
    private int port;

    private final LogHandler logHandler;

    // 两个线程组：boss负责接受连接，worker负责处理读写
    private final NioEventLoopGroup bossGroup = new NioEventLoopGroup(1);
    private final NioEventLoopGroup workerGroup = new NioEventLoopGroup();

    public NettyServer(LogHandler logHandler) {
        this.logHandler = logHandler;
    }

    @Bean
    public ApplicationRunner startNetty() {
        return args -> {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline pipeline = ch.pipeline();
                            // 按行分割，防止粘包
                            pipeline.addLast(new LineBasedFrameDecoder(1024 * 64));
                            pipeline.addLast(new StringDecoder(StandardCharsets.UTF_8));
                            pipeline.addLast(new StringEncoder(StandardCharsets.UTF_8));
                            pipeline.addLast(logHandler);
                        }
                    });

            ChannelFuture future = bootstrap.bind(port).sync();
            log.info("[Netty] started on port {}", port);
            // 不阻塞主线程
            future.channel().closeFuture().addListener(f -> log.info("[Netty] server closed"));
        };
    }

    @PreDestroy
    public void shutdown() {
        log.info("[Netty] shutting down...");
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }
}