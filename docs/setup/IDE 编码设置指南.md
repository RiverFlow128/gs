# IDE 编码设置指南

## ⚠️ 重要提示

为避免 SQL 文件中文注释乱码问题，请务必按以下步骤设置 IDE 和 Git 配置。

---

## ✅ 已自动配置（无需手动操作）

### 1. .gitattributes 文件
项目根目录已创建 `.gitattributes` 文件，强制使用 UTF-8 编码：

```gitattributes
*.sql text eol=lf working-tree-encoding=UTF-8
*.java text eol=lf working-tree-encoding=UTF-8
*.properties text eol=lf working-tree-encoding=UTF-8
*.xml text eol=lf working-tree-encoding=UTF-8
*.json text eol=lf working-tree-encoding=UTF-8
*.md text eol=lf working-tree-encoding=UTF-8
```

**作用**：
- Git 提交时自动转换为 LF 换行
- Git 检出时自动使用 UTF-8 编码
- 从源头杜绝编码不一致问题

### 2. Maven 配置
`pom.xml` 已配置：

```xml
<project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
```

---

## 🔧 IDEA 手动设置（必须）

### 步骤 1: 设置 File Encodings

1. 打开 **Settings/Preferences** (Ctrl+Alt+S)
2. 导航到 **Editor → File Encodings**
3. 按以下设置：

```
Global Encoding:    UTF-8
Project Encoding:   UTF-8
Default encoding for properties files: UTF-8
```

4. 勾选 **"Transparent native-to-ascii conversion"**（如果有）

### 步骤 2: 验证设置

1. 打开任意 SQL 文件
2. 查看右下角状态栏，应显示 **UTF-8**
3. 如果不是，点击它并选择 UTF-8

### 步骤 3: 检查现有文件编码

如果之前已有文件是 GBK 编码，需要转换：

1. 打开乱码的 SQL 文件
2. 点击右下角编码显示（可能显示 GBK）
3. 选择 **UTF-8**
4. 保存文件

或者批量转换：

```bash
# Windows PowerShell 批量转换
Get-ChildItem -Recurse -Include *.sql,*.java,*.properties | 
ForEach-Object {
    $content = Get-Content $_.FullName -Encoding Default
    [System.IO.File]::WriteAllText($_.FullName, $content, [System.Text.Encoding]::UTF8)
}
```

---

## 🚀 Git 操作注意事项

### 首次设置后重新拉取代码

设置完 `.gitattributes` 后，建议重新拉取代码以确保编码正确：

```bash
# 1. 提交当前所有修改
git add .
git commit -m "save changes before reclone"

# 2. 删除本地目录（备份好修改）
cd ..
rmdir /s test-main

# 3. 重新克隆
git clone <your-repo-url> test-main
cd test-main

# 4. 验证编码
file src/main/resources/sql/ods/*.sql
# 应该显示：UTF-8 Unicode text
```

### 检查文件编码

```bash
# Linux/Mac
file src/main/resources/sql/ods/tables.sql

# Windows PowerShell
[System.IO.File]::ReadAllText("src/main/resources/sql/ods/tables.sql")
```

---

## ❌ 禁止行为

以下操作可能导致编码问题复发：

- ❌ 用记事本（Notepad）直接保存 SQL 文件
- ❌ 在 IDEA 中手动切换文件编码为 GBK/ANSI
- ❌ 使用 `git checkout --force` 忽略 .gitattributes
- ❌ 通过微信/QQ 传输 SQL 文件（可能改变编码）

---

## ✅ 推荐工具

### Windows 用户

- ✅ **VS Code**: 默认 UTF-8，自动识别
- ✅ **IntelliJ IDEA**: 按上述设置后完美支持
- ✅ **Notepad++**: 编码菜单 → 选择 UTF-8

### 避免使用

- ❌ Windows 记事本（可能保存为 GBK/BOM）
- ❌ UltraEdit（默认可能是 ANSI）

---

## 🔍 验证编码是否正确

### 方法 1: 查看中文注释

打开任意 SQL 文件，查看中文注释：

```sql
-- 如果显示正常中文：✅ 编码正确
-- 如果显示乱码：❌ 编码有问题
```

### 方法 2: Git 检查

```bash
# 查看 Git 追踪的编码设置
git check-attr encoding src/main/resources/sql/ods/tables.sql
# 应输出：encoding: UTF-8
```

### 方法 3: 运行检查脚本

```bash
powershell -ExecutionPolicy Bypass -File .\check_encoding.ps1
```

---

## 🛠️ 故障排查

### 问题 1: SQL 文件仍然乱码

**解决步骤**：

1. 确认 `.gitattributes` 存在且已提交
2. 在 IDEA 中重新设置 File Encodings
3. 关闭 IDEA，删除 `.idea` 文件夹，重新打开项目
4. 如果还不行，重新克隆仓库

### 问题 2: 新人拉代码还是乱码

**原因**：`.gitattributes` 是在文件之后添加的

**解决**：

```bash
# 方案 1: 重新克隆（推荐）
git clone <repo> new-folder
cd new-folder
# 此时 Git 会自动应用 .gitattributes

# 方案 2: 重置索引
git rm --cached -r .
git reset --hard
```

---

## 📝 团队规范建议

如果是团队开发，建议：

1. ✅ 将本指南加入团队文档
2. ✅ 新人入职培训时强调编码设置
3. ✅ CI/CD 流程中加入编码检查
4. ✅ Code Review 时检查文件编码

---

## 🎯 一劳永逸

完成以上设置后：

- ✅ 再也不不用担心乱码问题
- ✅ `fix_encoding.py` 可以删除了
- ✅ 团队协作更顺畅
- ✅ 跨平台（Windows/Linux/Mac）编码一致

**从此告别乱码！** 🎉

---

如有问题，请查阅：
- Git 官方文档：https://git-scm.com/docs/gitattributes
- IDEA 官方文档：https://www.jetbrains.com/help/idea/settings-file-encodings.html
