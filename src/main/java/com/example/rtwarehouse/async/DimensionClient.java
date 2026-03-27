package com.example.rtwarehouse.async;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * 维度客户端
 * TODO: 后续可添加连接池管理，提高数据库连接效率
 * TODO: 可增加缓存机制，减少重复查询
 */
public class DimensionClient implements AutoCloseable {
    private final ExecutorService executorService;
    private final String jdbcUrl;
    private final String username;
    private final String password;

    public DimensionClient() {
        this.executorService = Executors.newFixedThreadPool(10);
        this.jdbcUrl = "jdbc:mysql://8.222.156.182:1001/statics?useSSL=false&serverTimezone=UTC";
        this.username = "root";
        this.password = System.getenv("DB_PASSWORD") != null ? System.getenv("DB_PASSWORD") : "";
    }

    /**
     * 异步查询维度数据
     * @param userId
     * @param callback
     */
    public void queryAsync(Long userId, Consumer<DimensionResult> callback) {
        executorService.submit(() -> {
            try (Connection conn = getConnection()) {
                DimensionResult result = queryDimensionData(conn, userId);
                callback.accept(result);
            } catch (Exception e) {
                e.printStackTrace();
                callback.accept(new DimensionResult(userId, "unknown"));
            }
        });
    }

    /**
     * 获取数据库连接
     * @return
     * @throws SQLException
     */
    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, username, password);
    }

    /**
     * 查询维度数据
     * @param conn
     * @param userId
     * @return
     * @throws SQLException
     * TODO: 可扩展为查询更多维度信息
     */
    private DimensionResult queryDimensionData(Connection conn, Long userId) throws SQLException {
        String sql = "SELECT device_id FROM device_users_relations_flat WHERE user_id = ? LIMIT 1";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, userId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    String deviceId = rs.getString("device_id");
                    return new DimensionResult(userId, "device_id: " + deviceId);
                } else {
                    return new DimensionResult(userId, "no_device");
                }
            }
        }
    }

    /**
     * 关闭资源
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        executorService.shutdown();
    }
}
