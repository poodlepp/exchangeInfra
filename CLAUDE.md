# 项目协作约定

## 知识沉淀

项目中遇到的技术要点、面试题、踩坑总结，统一沉淀到 `question/` 目录，按主题分类整理为 Markdown 文档：

- 文件按主题分类，例如：
  - `question/wallet-nonce.md` —— nonce 管理、加速取消、并发分配
  - `question/wallet-reconcile.md` —— 双账法、链上对账、对账差异处置
  - `question/wallet-fee.md` —— EIP-1559、UTXO selection、TRON energy
  - `question/wallet-reorg.md` —— 区块重组检测、回滚、反向冲账
  - `question/mq-outbox.md` —— Kafka 事务发送、幂等消费、DLQ
  - `question/spring-statemachine.md` —— 状态机配置、事件驱动
- 每篇文档结构：**问题/题面 → 标准答案要点 → 在本项目中的体现 → 延伸追问**
- 触发时机：每完成一个核心机制（§6.x）或解决一个非平凡问题，立刻沉淀对应 question 文档
- 已存在的主题文档继续追加，不要新建重复主题
