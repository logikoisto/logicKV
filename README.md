# logicKV
纯golang实现的分布式持久化KV数据库

用来验证我从某些论文中得到的新思想的学习型项目

1. 实现 raft 论文
  * 基于纯golang实现的KV存储引擎 [!badger](https://github.com/dgraph-io/badger)开发
  * 完全以raft论文实现,选举/复制(心跳)/角色变更/配置热更新,并解决一些工程实现的问题。
2. 实现 [!jump](https://arxiv.org/pdf/1406.2294) 一致性哈希算法
  * 基于 jump 一致性哈希进行分片存储数据
  * 通过 节点标记的方法 解决jump哈希无法应对剔除节点导致的大面积key迁移问题
3. 实现了较为完整的分片/复制的kv存储逻辑

