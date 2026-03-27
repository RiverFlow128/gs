# 本地运行指南

## 📋 前置要求

1. **MySQL 8.0+**（已配置 CDC）
2. **Java 11+**
3. **Maven 3.6+**
4. **Flink 1.17.1**（用于执行 SQL）

## 🚀 快速开始

### 方案一：使用 Dinky/Flink Web UI（推荐）

这是最简单的本地测试方式，**不需要 Kafka 和 HDFS**！

#### 步骤：

1. **准备 MySQL**
   - 确保可以连接到 `8.222.156.182:1001`
   - 或者修改为本地 MySQL：`localhost:3306`

2. **设置环境变量**（可选，推荐）
   ```powershell
   # PowerShell
   $env:DB_USERNAME="super"
   $env:DB_PASSWORD="your_password_here"
   ```

3. **在 Dinky 中执行本地测试 SQL**
   - 打开 Dinky Web UI
   - 上传或粘贴 `src/main/resources/sql/local/game_record_local_test.sql`
   - 点击执行
   - 在 Flink Web UI 查看输出（Print Sink 会打印到控制台）

**优势：**
- ✅ 不依赖 Kafka
- ✅ 不依赖 HDFS
- ✅ 实时看到数据输出
- ✅ 完全模拟生产环境

---

### 方案二：本地运行 Java 程序

```bash
# 1. 编译项目
mvn clean package -DskipTests

# 2. 运行 ODS 数据读取器（直接看 MySQL CDC 数据）
mvn exec:java -Dexec.mainClass="com.example.rtwarehouse.OdsDataReader"
```

**效果：** 会在控制台打印 MySQL 中的数据变更

---

## 🔧 配置说明

### 1. 数据库连接配置

**方式 A：使用环境变量（推荐）**
```bash
$env:DB_USERNAME="super"
$env:DB_PASSWORD="your_password_here"
```

**方式 B：修改 application.properties**
```properties
mysql.username=你的用户名
mysql.password=你的密码
```

### 2. 时区统一

所有 SQL 文件和 Java 代码已统一使用 `Asia/Shanghai` 时区。

---

## ⚠️ 常见问题

### Q1: "Connection refused" 错误
**原因：** MySQL 无法连接  
**解决：** 
- 检查网络连接
- 确认 MySQL 服务已启动
- 验证用户名密码是否正确

### Q2: "CDC cannot find table" 错误
**原因：** 表名不匹配  
**解决：** 检查 `game_record_.*` 正则是否匹配实际表名

### Q3: Checkpoint 失败
**原因：** 使用了 HDFS 但本地没有  
**解决：** 使用本地测试版 SQL（`sql/local/` 目录），已改用内存状态后端

---

## 📝 下一步

当你搭建好虚拟机 Kafka 后：

1. 修改 `application-local.properties` 配置 Kafka 地址
2. 使用生产环境的 SQL 文件（`sql/ods/`, `sql/dws/` 等）
3. 数据会写入你本地的 Kafka

---

## 🎯 本地测试 vs 生产环境

| 特性 | 本地测试 | 生产环境 |
|------|---------|---------|
| Kafka | ❌ 不需要（用 Print Sink） | ✅ 必须（阿里云 Kafka） |
| HDFS | ❌ 不需要（用内存后端） | ✅ 必须（HDFS Checkpoint） |
| MySQL | ✅ 必须（CDC 源） | ✅ 必须（生产数据库） |
| 状态后端 | hashmap（内存） | RocksDB + HDFS |
| 用途 | 开发调试、功能验证 | 正式运行 |
