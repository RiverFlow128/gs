package com.example.rtwarehouse;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * 实时数仓主启动程序
 * 用于本地开发和测试
 * 
 * 执行流程：ODS -> DIM -> DWD -> DWS -> ADS
 */
public class RealtimeWarehouseApplication {
    
    private static final Logger LOG = LoggerFactory.getLogger(RealtimeWarehouseApplication.class);
    
    public static void main(String[] args) throws Exception {
        LOG.info("========================================");
        LOG.info("游戏业务实时数仓启动中...");
        LOG.info("========================================");
        
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        // 启用 Checkpoint（可选）
        // env.enableCheckpointing(60000);
        
        // 2. 创建 Table 环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        
        TableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        
        // 3. 加载配置
        String sqlBasePath = "src/main/resources/sql";
        
        // 4. 按层执行 SQL
        LOG.info("开始执行 ODS 层...");
        executeSqlFile(tableEnv, sqlBasePath + "/ods/tables.sql");
        
        LOG.info("开始执行 DIM 层...");
        executeSqlFile(tableEnv, sqlBasePath + "/dim/tables.sql");
        
        LOG.info("开始执行 DWD 层...");
        executeSqlFile(tableEnv, sqlBasePath + "/dwd/tables.sql");
        
        LOG.info("开始执行 DWS 层...");
        executeSqlFile(tableEnv, sqlBasePath + "/dws/tables.sql");
        
        LOG.info("开始执行 ADS 层...");
        executeSqlFile(tableEnv, sqlBasePath + "/ads/tables.sql");
        
        // 5. 打印执行计划（可选）
        LOG.info("========================================");
        LOG.info("所有 SQL 已加载完成，开始执行作业...");
        LOG.info("========================================");
        
        // 6. 执行 - 注意：实际执行时会一直运行，直到手动停止
        env.execute();
    }
    
    /**
     * 执行 SQL 文件
     */
    private static void executeSqlFile(TableEnvironment tableEnv, String filePath) throws Exception {
        Path path = Paths.get(filePath);
        if (!Files.exists(path)) {
            LOG.error("SQL 文件不存在：{}", filePath);
            throw new RuntimeException("SQL 文件不存在：" + filePath);
        }
        
        String sqlContent = Files.readString(path);
        
        // 处理 @include 指令
        sqlContent = processIncludeDirectives(sqlContent, path.getParent());
        
        // 分割 SQL 语句（按分号分隔）
        String[] statements = sqlContent.split(";");
        
        for (String statement : statements) {
            statement = statement.trim();
            if (statement.isEmpty() || statement.startsWith("--")) {
                continue;
            }
            
            try {
                LOG.debug("执行 SQL: {}", statement.substring(0, Math.min(100, statement.length())));
                tableEnv.executeSql(statement);
            } catch (Exception e) {
                LOG.error("执行 SQL 失败：{}", statement.substring(0, Math.min(200, statement.length())), e);
                throw e;
            }
        }
    }
    
    /**
     * 处理 @include 指令
     */
    private static String processIncludeDirectives(String content, Path basePath) throws Exception {
        String[] lines = content.split("\n");
        StringBuilder result = new StringBuilder();
        
        for (String line : lines) {
            if (line.trim().startsWith("@include")) {
                // 提取文件路径
                String includePath = line.trim()
                        .replace("@include", "")
                        .trim()
                        .replace("'", "")
                        .replace(";", "");
                
                Path fullPath = basePath.resolve(includePath);
                if (Files.exists(fullPath)) {
                    String includedContent = Files.readString(fullPath);
                    result.append(includedContent).append("\n");
                    LOG.debug("已包含文件：{}", fullPath);
                } else {
                    LOG.warn("包含的文件不存在：{}", fullPath);
                }
            } else {
                result.append(line).append("\n");
            }
        }
        
        return result.toString();
    }
}
