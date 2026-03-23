package com.logplatform.dashboard.redis;

import com.logplatform.dashboard.websocket.AlertWebSocketHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

@Configuration
public class RedisAlertSubscriber {

    private static final String ALERT_CHANNEL      = "alerts";
    private static final String LOG_STREAM_CHANNEL = "log-stream";
    private static final Logger log = LoggerFactory.getLogger(RedisAlertSubscriber.class);

    private final AlertWebSocketHandler webSocketHandler;

    public RedisAlertSubscriber(AlertWebSocketHandler webSocketHandler) {
        this.webSocketHandler = webSocketHandler;
    }

    @Bean
    public RedisMessageListenerContainer redisListenerContainer(
            RedisConnectionFactory connectionFactory) {

        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);

        // 直接实现MessageListener接口，不用MessageListenerAdapter
        MessageListener listener = new MessageListener() {
            @Override
            public void onMessage(Message message, byte[] pattern) {
                String body = new String(message.getBody());
                log.debug("[Redis] received, broadcasting to WS");
                webSocketHandler.broadcast(body);
            }
        };

        container.addMessageListener(listener, new PatternTopic(ALERT_CHANNEL));
        container.addMessageListener(listener, new PatternTopic(LOG_STREAM_CHANNEL));
        return container;
    }
}