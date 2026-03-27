package com.example.rtwarehouse;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 数仓分层测试程序
 * 用于逐步验证各层是否正常工作
 * 
 * 使用方法：
 * 1. 先测试 ODS 层
 * 2. 再测试 DWD 层
 * 3. 依次测试 DWS、ADS 层
 */
public class WarehouseLayerTest {
    
    private static final Logger LOG = LoggerFactory.getLogger(WarehouseLayerTest.class);
    
    public static void main(String[] args) throws Exception {
        LOG.info("========================================");
        LOG.info("实时数仓分层测试开始");
        LOG.info("========================================");
        
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        
        TableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        
        String sqlBasePath = "src/main/resources/sql";
        
        // ========== 第一步：测试 ODS 层 ==========
        LOG.info("");
        LOG.info("========== 测试 ODS 层 ==========");
        testOdsLayer(tableEnv, sqlBasePath);
        
        // 询问用户是否继续
        LOG.info("");
        LOG.info("ODS 层测试完成，是否继续测试下一层？(y/n)");
        // 实际运行时会自动继续，这里只是提示
        
        // ========== 第二步：测试 DIM 层 ==========
        LOG.info("");
        LOG.info("========== 测试 DIM 层 ==========");
        testDimLayer(tableEnv, sqlBasePath);
        
        // ========== 第三步：测试 DWD 层 ==========
        LOG.info("");
        LOG.info("========== 测试 DWD 层 ==========");
        testDwdLayer(tableEnv, sqlBasePath);
        
        // ========== 第四步：测试 DWS 层 ==========
        LOG.info("");
        LOG.info("========== 测试 DWS 层 ==========");
        testDwsLayer(tableEnv, sqlBasePath);
        
        // ========== 第五步：测试 ADS 层 ==========
        LOG.info("");
        LOG.info("========== 测试 ADS 层 ==========");
        testAdsLayer(tableEnv, sqlBasePath);
        
        LOG.info("");
        LOG.info("========================================");
        LOG.info("所有层级测试完成！");
        LOG.info("========================================");
        
        // 执行（会一直运行）
        env.execute("Warehouse Layer Test");
    }
    
    /**
     * 测试 ODS 层
     */
    private static void testOdsLayer(TableEnvironment tableEnv, String basePath) throws Exception {
        LOG.info("加载 ODS 层 SQL...");
        executeSqlFile(tableEnv, basePath + "/ods/tables.sql");
        LOG.info("✓ ODS 层加载成功");
    }
    
    /**
     * 测试 DIM 层
     */
    private static void testDimLayer(TableEnvironment tableEnv, String basePath) throws Exception {
        LOG.info("加载 DIM 层 SQL...");
        executeSqlFile(tableEnv, basePath + "/dim/tables.sql");
        LOG.info("✓ DIM 层加载成功");
    }
    
    /**
     * 测试 DWD 层
     */
    private static void testDwdLayer(TableEnvironment tableEnv, String basePath) throws Exception {
        LOG.info("加载 DWD 层 SQL...");
        executeSqlFile(tableEnv, basePath + "/dwd/tables.sql");
        LOG.info("✓ DWD 层加载成功");
    }
    
    /**
     * 测试 DWS 层
     */
    private static void testDwsLayer(TableEnvironment tableEnv, String basePath) throws Exception {
        LOG.info("加载 DWS 层 SQL...");
        executeSqlFile(tableEnv, basePath + "/dws/tables.sql");
        LOG.info("✓ DWS 层加载成功");
    }
    
    /**
     * 测试 ADS 层
     */
    private static void testAdsLayer(TableEnvironment tableEnv, String basePath) throws Exception {
        LOG.info("加载 ADS 层 SQL...");
        executeSqlFile(tableEnv, basePath + "/ads/tables.sql");
        LOG.info("✓ ADS 层加载成功");
    }
    
    /**
     * 执行 SQL 文件（简化版，不处理@include）
     */
    private static void executeSqlFile(TableEnvironment tableEnv, String filePath) throws Exception {
        java.nio.file.Path path = java.nio.file.Paths.get(filePath);
        if (!java.nio.file.Files.exists(path)) {
            throw new RuntimeException("SQL 文件不存在：" + filePath);
        }
        
        String content = java.nio.file.Files.readString(path);
        
        // 处理 @include 指令
        content = processIncludeDirectives(content, path.getParent());
        
        // 分割并执行 SQL
        String[] statements = content.split(";");
        for (String stmt : statements) {
            stmt = stmt.trim();
            if (stmt.isEmpty() || stmt.startsWith("--")) {
                continue;
            }
            
            try {
                tableEnv.executeSql(stmt);
            } catch (Exception e) {
                LOG.error("SQL 执行失败：{}", stmt.substring(0, Math.min(100, stmt.length())));
                throw e;
            }
        }
    }
    
    /**
     * 处理 @include 指令
     */
    private static String processIncludeDirectives(String content, java.nio.file.Path basePath) throws Exception {
        String[] lines = content.split("\n");
        StringBuilder result = new StringBuilder();
        
        for (String line : lines) {
            if (line.trim().startsWith("@include")) {
                String includePath = line.trim()
                        .replace("@include", "")
                        .trim()
                        .replace("'", "")
                        .replace(";", "");
                
                java.nio.file.Path fullPath = basePath.resolve(includePath);
                if (java.nio.file.Files.exists(fullPath)) {
                    String includedContent = java.nio.file.Files.readString(fullPath);
                    result.append(includedContent).append("\n");
                }
            } else {
                result.append(line).append("\n");
            }
        }
        
        return result.toString();
    }
}
