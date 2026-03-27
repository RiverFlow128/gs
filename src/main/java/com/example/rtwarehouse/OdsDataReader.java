package com.example.rtwarehouse;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * ODS层数据读取 - 从MySQL CDC读取3张表
 * 直接运行即可看到数据
 */
public class OdsDataReader {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==========================================
        // MySQL CDC Source - 从 MySQL 读取原始数据
        // 连接信息来自 application.properties
        // ==========================================
        MySqlSource<String> source = MySqlSource.<String>builder()
                // MySQL 连接配置
                .hostname("8.222.156.182")    // 生产环境 MySQL 地址
                .port(1001)                    // MySQL 端口
                .databaseList("statics")       // 数据库名
                
                // 监控的表列表（支持正则）
                .tableList(
                        "statics.device_users_relations_flat",  // 用户关系表
                        "statics.game_record",                   // 游戏记录表
                        "statics.user_online_status_record"      // 用户在线状态表
                )
                
                // 认证信息 - 使用环境变量，避免硬编码密码
                .username(System.getenv("DB_USERNAME") != null ? 
                          System.getenv("DB_USERNAME") : "super")  // 优先使用环境变量，否则默认值
                .password(System.getenv("DB_PASSWORD") != null ?
                          System.getenv("DB_PASSWORD") : "")  // 生产环境密码必须从环境变量注入
                
                // CDC 配置
                .serverId("5400-5500")         // CDC 客户端 ID 范围，避免与业务冲突
                .serverTimeZone("Asia/Bangkok")  // MySQL 服务器实际时区是 UTC+7（曼谷时间）
                
                // 启动模式
                .startupOptions(StartupOptions.latest())  // 只读增量数据，若改成 initial() 可以读全量
                
                // 反序列化器
                .deserializer(new JsonDebeziumDeserializationSchema())  // 将 CDC 数据转为 JSON 格式
                .build();

        // 读取数据并打印        
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "MySQL CDC Source").print("ODS");

        // 执行
        env.execute("ODS Data Reader");
    }
}
