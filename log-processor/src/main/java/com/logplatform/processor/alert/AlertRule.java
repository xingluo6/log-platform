package com.logplatform.processor.alert;

/**
 * 告警规则定义
 * 当前支持两种规则类型：
 *   LEVEL_MATCH  - 匹配特定日志级别时触发
 *   ERROR_RATE   - N分钟内ERROR数量超过阈值时触发
 */
public class AlertRule {

    public enum RuleType {
        LEVEL_MATCH,   // 出现指定level就触发（如ERROR）
        ERROR_RATE     // 时间窗口内错误数超过阈值触发
    }

    private final String ruleName;
    private final RuleType ruleType;
    private final String targetLevel;   // LEVEL_MATCH用
    private final int threshold;        // ERROR_RATE用：阈值数量
    private final int windowMinutes;    // ERROR_RATE用：时间窗口（分钟）

    // LEVEL_MATCH规则构造
    public AlertRule(String ruleName, String targetLevel) {
        this.ruleName = ruleName;
        this.ruleType = RuleType.LEVEL_MATCH;
        this.targetLevel = targetLevel;
        this.threshold = 0;
        this.windowMinutes = 0;
    }

    // ERROR_RATE规则构造
    public AlertRule(String ruleName, int threshold, int windowMinutes) {
        this.ruleName = ruleName;
        this.ruleType = RuleType.ERROR_RATE;
        this.targetLevel = "ERROR";
        this.threshold = threshold;
        this.windowMinutes = windowMinutes;
    }

    public String getRuleName() { return ruleName; }
    public RuleType getRuleType() { return ruleType; }
    public String getTargetLevel() { return targetLevel; }
    public int getThreshold() { return threshold; }
    public int getWindowMinutes() { return windowMinutes; }
}