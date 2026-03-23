package com.logplatform.dashboard.websocket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.*;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

@Component
public class AlertWebSocketHandler extends TextWebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(AlertWebSocketHandler.class);

    private final CopyOnWriteArraySet<WebSocketSession> sessions = new CopyOnWriteArraySet<>();
    // 每个session独立锁，防止并发写入同一session
    private final ConcurrentHashMap<String, Object> sessionLocks = new ConcurrentHashMap<>();

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        sessions.add(session);
        sessionLocks.put(session.getId(), new Object());
        log.info("[WS] client connected: {}, total: {}", session.getId(), sessions.size());
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        sessions.remove(session);
        sessionLocks.remove(session.getId());
        log.info("[WS] client disconnected: {}, total: {}", session.getId(), sessions.size());
    }

    public void broadcast(String message) {
        sessions.removeIf(s -> !s.isOpen());
        for (WebSocketSession session : sessions) {
            Object lock = sessionLocks.computeIfAbsent(session.getId(), k -> new Object());
            synchronized (lock) {
                try {
                    if (session.isOpen()) {
                        session.sendMessage(new TextMessage(message));
                    }
                } catch (Exception e) {
                    log.error("[WS] send failed: {}", e.getMessage());
                }
            }
        }
    }
}