# 贡献指南

感谢您对量化交易系统 v2.0 的关注和贡献！

## 🚀 如何贡献

### 1. Fork 项目
- 点击项目页面右上角的 "Fork" 按钮
- 将项目 Fork 到你的 GitHub 账户

### 2. 克隆项目
```bash
git clone https://github.com/你的用户名/quant_v2.git
cd quant_v2
```

### 3. 创建分支
```bash
git checkout -b feature/你的功能名称
```

### 4. 开发功能
- 编写代码
- 添加测试
- 更新文档

### 5. 提交更改
```bash
git add .
git commit -m "feat: 添加新功能描述"
```

### 6. 推送分支
```bash
git push origin feature/你的功能名称
```

### 7. 创建 Pull Request
- 在 GitHub 上创建 Pull Request
- 详细描述你的更改
- 等待代码审查

## 📝 代码规范

### Python 代码规范
- 使用 PEP 8 代码风格
- 函数和类需要添加文档字符串
- 变量名使用下划线命名法
- 常量使用大写字母

### 提交信息规范
- `feat:` 新功能
- `fix:` 修复问题
- `docs:` 文档更新
- `style:` 代码格式调整
- `refactor:` 代码重构
- `test:` 测试相关
- `chore:` 构建过程或辅助工具的变动

## 🧪 测试

在提交代码前，请确保：
1. 运行所有测试：`python -m pytest tests/`
2. 代码通过 lint 检查
3. 新功能有对应的测试用例

## 📚 文档

- 更新相关文档
- 添加使用示例
- 更新 README.md（如需要）

## ❓ 问题反馈

如果你发现 bug 或有功能建议，请：
1. 查看现有的 Issues
2. 创建新的 Issue
3. 详细描述问题或建议

## 📄 许可证

本项目采用 MIT 许可证，详见 [LICENSE](LICENSE) 文件。
