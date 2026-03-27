package com.example.rtwarehouse.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 常量配置类
 */
public class Constants {
    
    private static final Properties props = new Properties();
    
    static {
        // 加载配置文件
        try (InputStream in = Constants.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (in != null) {
                props.load(in);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    
    // ========== Flink配置 ==========
    
    /** 默认并行度 */
    public static final int DEFAULT_PARALLELISM = getIntProperty("flink.parallelism", 2);
    
    /** Checkpoint间隔(毫秒) */
    public static final long CHECKPOINT_INTERVAL = getLongProperty("flink.checkpoint.interval", 60000L);
    
    // MySQL配置
    public static final String MYSQL_HOST = getProperty("mysql.host", "localhost");
    public static final int MYSQL_PORT = getIntProperty("mysql.port", 3306);
    public static final String MYSQL_DATABASE = getProperty("mysql.database", "game_db");
    public static final String MYSQL_USERNAME = getProperty("mysql.username", "root");
    public static final String MYSQL_PASSWORD = getProperty("mysql.password", "");
    
    // Kafka配置
    public static final String KAFKA_BOOTSTRAP_SERVERS = getProperty("kafka.bootstrap.servers", "localhost:9092");
    
    // ClickHouse配置
    public static final String CLICKHOUSE_URL = getProperty("clickhouse.url", "jdbc:clickhouse://localhost:8123");
    public static final String CLICKHOUSE_USERNAME = getProperty("clickhouse.username", "default");
    public static final String CLICKHOUSE_PASSWORD = getProperty("clickhouse.password", "");
    
    // JDBC配置(ADS层写入)
    public static final String JDBC_URL = getProperty("jdbc.url", "jdbc:mysql://localhost:3306/analytics?rewriteBatchedStatements=true");
    public static final String JDBC_USERNAME = getProperty("jdbc.username", "root");
    public static final String JDBC_PASSWORD = getProperty("jdbc.password", "");
    
    // ========== 窗口配置 ==========
    
    /** 登录统计窗口大小(分钟) */
    public static final int LOGIN_WINDOW_MINUTES = getIntProperty("window.login.minutes", 1);
    
    /** 订单统计窗口大小(小时) */
    public static final int ORDER_WINDOW_HOURS = getIntProperty("window.order.hours", 1);
    
    // ========== 工具方法 ==========
    
    /**
     * 获取配置属性
     */
    private static String getProperty(String key, String defaultValue) {
        // 优先从环境变量获取
        String value = System.getenv(key.toUpperCase().replace('.', '_'));
        if (value != null) {
            return value;
        }
        // 从配置文件获取
        return props.getProperty(key, defaultValue);
    }
    
    /**
     * 获取整型配置属性
     */
    private static int getIntProperty(String key, int defaultValue) {
        String value = getProperty(key, null);
        return value != null ? Integer.parseInt(value) : defaultValue;
    }
    
    /**
     * 获取长整型配置属性
     */
    private static long getLongProperty(String key, long defaultValue) {
        String value = getProperty(key, null);
        return value != null ? Long.parseLong(value) : defaultValue;
    }
    
    // ========== 枚举类 ==========
    
    /**
     * 游戏ID枚举
     */
    public enum GameId {
        KING_GLORY(1001),
        GENSHIN(1002),
        PUBG(1003);
        
        private final int value;
        
        GameId(int value) {
            this.value = value;
        }
        
        public int getValue() {
            return value;
        }
        
        public static GameId fromValue(int value) {
            for (GameId gameId : GameId.values()) {
                if (gameId.value == value) {
                    return gameId;
                }
            }
            throw new IllegalArgumentException("Invalid game ID: " + value);
        }
    }
    
    /**
     * 设备类型枚举
     */
    public enum DeviceType {
        IOS(1),
        ANDROID(2),
        PC(3);
        
        private final int value;
        
        DeviceType(int value) {
            this.value = value;
        }
        
        public int getValue() {
            return value;
        }
        
        public static DeviceType fromValue(int value) {
            for (DeviceType deviceType : DeviceType.values()) {
                if (deviceType.value == value) {
                    return deviceType;
                }
            }
            throw new IllegalArgumentException("Invalid device type: " + value);
        }
    }
    
    /**
     * 订单状态枚举
     */
    public enum OrderStatus {
        PENDING(0),
        PAID(1),
        CANCELLED(2),
        REFUNDED(3);
        
        private final int value;
        
        OrderStatus(int value) {
            this.value = value;
        }
        
        public int getValue() {
            return value;
        }
        
        public static OrderStatus fromValue(int value) {
            for (OrderStatus status : OrderStatus.values()) {
                if (status.value == value) {
                    return status;
                }
            }
            throw new IllegalArgumentException("Invalid order status: " + value);
        }
    }
    
    private Constants() {
        // 工具类不允许实例化
    }
}
