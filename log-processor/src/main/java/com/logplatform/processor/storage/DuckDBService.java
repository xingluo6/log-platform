package com.logplatform.processor.storage;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;

/**
 * DuckDB 存储服务
 *
 * 时区方案：
 *   存入：UTC Instant → 上海 LocalDateTime → 存 TIMESTAMP（无时区）
 *   查询：用 Java 计算上海时间的时间窗口，作为参数传给 DuckDB
 *         strftime 格式化已是上海时间的 TIMESTAMP，直接输出正确时间
 *   不依赖 DuckDB SET TimeZone（对 TIMESTAMP 类型行为不稳定）
 *   不在 SQL 里做任何时区运算，彻底消除 %s 残留问题
 */
@Service
public class DuckDBService {

    private static final Logger log = LoggerFactory.getLogger(DuckDBService.class);
    private static final String DB_PATH = "./data/logs.duckdb";
    private static final ZoneId SHANGHAI = ZoneId.of("Asia/Shanghai");

    private Connection connection;

    @PostConstruct
    public synchronized void init() {
        try {
            new java.io.File("./data").mkdirs();
            Class.forName("org.duckdb.DuckDBDriver");
            connection = DriverManager.getConnection("jdbc:duckdb:" + DB_PATH);
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("""
                    CREATE TABLE IF NOT EXISTS logs (
                        id          VARCHAR DEFAULT gen_random_uuid(),
                        timestamp   TIMESTAMP,
                        level       VARCHAR,
                        service     VARCHAR,
                        message     VARCHAR,
                        logger_name VARCHAR,
                        thread_name VARCHAR,
                        created_at  TIMESTAMP DEFAULT now()
                    )
                """);
                stmt.execute("""
                    CREATE TABLE IF NOT EXISTS alerts (
                        id           VARCHAR DEFAULT gen_random_uuid(),
                        rule_name    VARCHAR,
                        service      VARCHAR,
                        message      VARCHAR,
                        triggered_at TIMESTAMP DEFAULT now()
                    )
                """);
            }
            log.info("[DuckDB] initialized: {}", DB_PATH);
        } catch (Exception e) {
            log.error("[DuckDB] init failed: {}", e.getMessage(), e);
        }
    }

    // ── 工具：当前上海时间往前 N 分钟的 Timestamp ──────────

    private Timestamp shanghaiMinus(int minutes) {
        return Timestamp.valueOf(
                ZonedDateTime.now(SHANGHAI).minusMinutes(minutes).toLocalDateTime()
        );
    }

    // ── 写入方法 ────────────────────────────────────────────

    public synchronized void insertLog(String utcTimestamp, String level, String service,
                                       String message, String loggerName, String threadName) {
        String sql = """
            INSERT INTO logs (timestamp, level, service, message, logger_name, thread_name)
            VALUES (?, ?, ?, ?, ?, ?)
        """;
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            // UTC → 上海本地时间，存为无时区 TIMESTAMP
            Timestamp ts = Timestamp.valueOf(
                    Instant.parse(utcTimestamp).atZone(SHANGHAI).toLocalDateTime()
            );
            ps.setTimestamp(1, ts);
            ps.setString(2, level);
            ps.setString(3, service);
            ps.setString(4, message);
            ps.setString(5, loggerName);
            ps.setString(6, threadName);
            ps.execute();
        } catch (Exception e) {
            log.error("[DuckDB] insertLog failed: {}", e.getMessage());
        }
    }

    public synchronized void insertAlert(String ruleName, String service, String message) {
        String sql = "INSERT INTO alerts (rule_name, service, message) VALUES (?, ?, ?)";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, ruleName);
            ps.setString(2, service);
            ps.setString(3, message);
            ps.execute();
            log.info("[DuckDB] alert saved: rule={}, service={}", ruleName, service);
        } catch (Exception e) {
            log.error("[DuckDB] insertAlert failed: {}", e.getMessage());
        }
    }

    public synchronized long countRecentErrors(String service, int minutes) {
        String sql = """
            SELECT COUNT(*) FROM logs
            WHERE service = ? AND level = 'ERROR' AND timestamp >= ?
        """;
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, service);
            ps.setTimestamp(2, shanghaiMinus(minutes));
            ResultSet rs = ps.executeQuery();
            if (rs.next()) return rs.getLong(1);
        } catch (Exception e) {
            log.error("[DuckDB] countRecentErrors failed: {}", e.getMessage());
        }
        return 0;
    }

    // ── 查询方法 ────────────────────────────────────────────

    public synchronized List<Map<String, Object>> queryErrorRate(int minutes) {
        String sql = """
            SELECT
                service,
                strftime(date_trunc('minute', timestamp), '%H:%M') AS minute,
                COUNT(*) FILTER (WHERE level = 'ERROR') AS error_count,
                COUNT(*) AS total_count,
                ROUND(COUNT(*) FILTER (WHERE level = 'ERROR') * 100.0 / COUNT(*), 2) AS error_rate
            FROM logs
            WHERE timestamp >= ?
            GROUP BY service, date_trunc('minute', timestamp)
            ORDER BY date_trunc('minute', timestamp) ASC
        """;
        return queryList(sql, shanghaiMinus(minutes));
    }

    public synchronized List<Map<String, Object>> queryAlerts(int limit) {
        String sql = """
            SELECT rule_name, service, message,
                   strftime(triggered_at, '%H:%M:%S') AS triggered_at
            FROM alerts
            ORDER BY triggered_at DESC
            LIMIT ?
        """;
        return queryList(sql, limit);
    }

    public synchronized List<Map<String, Object>> queryServiceStatus() {
        // 最近1分钟内有日志的服务
        String sql = """
            SELECT
                service,
                COUNT(*) FILTER (WHERE level = 'ERROR') AS recent_errors,
                strftime(MAX(timestamp), '%H:%M:%S') AS last_seen,
                CASE
                    WHEN COUNT(*) FILTER (WHERE level = 'ERROR') > 0 THEN 'ERROR'
                    ELSE 'OK'
                END AS status
            FROM logs
            WHERE timestamp >= ?
            GROUP BY service
        """;
        return queryList(sql, shanghaiMinus(1));
    }

    public synchronized Map<String, Object> queryStats() {
        Map<String, Object> stats = new HashMap<>();
        try {
            ResultSet rs1 = connection.createStatement()
                    .executeQuery("SELECT COUNT(*) FROM logs");
            if (rs1.next()) stats.put("totalLogs", rs1.getLong(1));

            // 今日开始（上海时间）
            Timestamp todayStart = Timestamp.valueOf(
                    ZonedDateTime.now(SHANGHAI).toLocalDate().atStartOfDay()
            );
            try (PreparedStatement ps = connection.prepareStatement(
                    "SELECT COUNT(*) FROM logs WHERE level = 'ERROR' AND timestamp >= ?")) {
                ps.setTimestamp(1, todayStart);
                ResultSet rs2 = ps.executeQuery();
                if (rs2.next()) stats.put("todayErrors", rs2.getLong(1));
            }

            ResultSet rs3 = connection.createStatement()
                    .executeQuery("SELECT COUNT(*) FROM alerts");
            if (rs3.next()) stats.put("totalAlerts", rs3.getLong(1));

        } catch (Exception e) {
            log.error("[DuckDB] queryStats failed: {}", e.getMessage());
        }
        return stats;
    }

    public synchronized List<Map<String, Object>> queryServiceHistory() {
        String sql = """
            SELECT service,
                   strftime(MAX(timestamp), '%H:%M:%S') AS last_seen,
                   COUNT(*) AS total_logs
            FROM logs
            GROUP BY service
            ORDER BY MAX(timestamp) DESC
        """;
        return queryList(sql);
    }

    public synchronized Map<String, Object> cleanupByRange(String range) {
        Map<String, Object> result = new HashMap<>();
        try {
            String whereLog, whereAlert;
            if ("all".equals(range)) {
                whereLog = whereAlert = "";
            } else {
                int minutes = switch (range) {
                    case "1h" -> 60;
                    case "1d" -> 60 * 24;
                    case "1w" -> 60 * 24 * 7;
                    case "4w" -> 60 * 24 * 28;
                    default   -> 60 * 24;
                };
                String since = shanghaiMinus(minutes).toString();
                whereLog   = "WHERE timestamp >= '" + since + "'";
                whereAlert = "WHERE triggered_at >= '" + since + "'";
            }
            try (Statement stmt = connection.createStatement()) {
                int logs   = stmt.executeUpdate("DELETE FROM logs "   + whereLog);
                int alerts = stmt.executeUpdate("DELETE FROM alerts " + whereAlert);
                result.put("success", true);
                result.put("message", "已清除 " + logs + " 条日志，" + alerts + " 条告警");
                log.info("[DuckDB] cleanup: logs={}, alerts={}, range={}", logs, alerts, range);
            }
        } catch (Exception e) {
            log.error("[DuckDB] cleanup failed: {}", e.getMessage());
            result.put("success", false);
            result.put("message", e.getMessage());
        }
        return result;
    }

    public synchronized List<Map<String, Object>> queryRecentLogs(String service, int limit) {
        String sql = """
            SELECT strftime(timestamp, '%H:%M:%S') AS time,
                   level, service, message
            FROM logs
            WHERE service = ?
            ORDER BY timestamp DESC
            LIMIT ?
        """;
        return queryList(sql, service, limit);
    }

    public synchronized List<Map<String, Object>> queryAllServices() {
        return queryList("SELECT DISTINCT service FROM logs ORDER BY service");
    }

    public synchronized List<Map<String, Object>> queryLogLevelStats(int minutes) {
        String sql = """
            SELECT level, COUNT(*) AS cnt
            FROM logs
            WHERE timestamp >= ?
            GROUP BY level
            ORDER BY cnt DESC
        """;
        return queryList(sql, shanghaiMinus(minutes));
    }

    public synchronized List<Map<String, Object>> queryLogVolume(int minutes) {
        String sql = """
            SELECT service, COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE level = 'ERROR') AS errors,
                   COUNT(*) FILTER (WHERE level = 'WARN')  AS warns,
                   COUNT(*) FILTER (WHERE level = 'INFO')  AS infos
            FROM logs
            WHERE timestamp >= ?
            GROUP BY service
            ORDER BY total DESC
        """;
        return queryList(sql, shanghaiMinus(minutes));
    }

    // ── 通用查询 ────────────────────────────────────────────

    private List<Map<String, Object>> queryList(String sql, Object... params) {
        List<Map<String, Object>> result = new ArrayList<>();
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) ps.setObject(i + 1, params[i]);
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData meta = rs.getMetaData();
            int cols = meta.getColumnCount();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= cols; i++)
                    row.put(meta.getColumnName(i), rs.getObject(i));
                result.add(row);
            }
        } catch (Exception e) {
            log.error("[DuckDB] queryList failed: {}", e.getMessage());
        }
        return result;
    }
}