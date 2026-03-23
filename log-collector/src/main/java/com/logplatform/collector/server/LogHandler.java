package com.logplatform.collector.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class LogHandler extends SimpleChannelInboundHandler<String> {

    private static final Logger log = LoggerFactory.getLogger(LogHandler.class);

    private final KafkaProducerService kafkaProducerService;

    public LogHandler(KafkaProducerService kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) {
        if (msg == null || msg.isBlank()) return;
        log.info("[Collector] received: {}", msg);
        kafkaProducerService.send(msg);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        log.info("[Collector] client connected: {}", ctx.channel().remoteAddress());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.info("[Collector] client disconnected: {}", ctx.channel().remoteAddress());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("[Collector] error: {}", cause.getMessage());
        ctx.close();
    }
}