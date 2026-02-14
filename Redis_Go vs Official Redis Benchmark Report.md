# Redis_Go 性能测试

> 目标：观察 Go 实现的 Redis 服务在 `redis-benchmark` 下的吞吐与延迟表现，并记录高并发与随机 Key 负载下的特征。  
## 摘要：

Redis_Go 在随机 keyspace（-r 10000）的写入场景下表现稳定：

c=50：吞吐约 10 万 rps，p99 ~1ms

c=100：吞吐提升至 ~128k–135k rps，p99 ~1.1–1.34ms

---

## 1. 实验对象与基本信息

- **Redis_Go服务（Go）**：监听 `172.31.0.1:6666`
  - 开启 AOF

> 说明：本报告仅基于 benchmark 输出做归纳与分析；未引入额外假设（如是否容器/WSL、是否有业务流量干扰等）。

**测试环境：**

操作系统
- OS: Windows 11 家庭中文版
- 版本: 10.0.26200 (Windows 11 Build 26200)
- 架构: 64位

处理器 (CPU)
- 型号: 13th Gen Intel(R) Core(TM) i7-13700H
- 物理核心: 14核
- 逻辑处理器: 20线程

内存 (RAM)
- 总容量: 16 GB

---

## 2. 测试方法与口径

### 2.1 工具与常用参数
- 工具：`redis-benchmark`
- 连接：TCP，`keep alive: 1`
- 客户端多线程：`multi-thread: no`（benchmark 客户端不启用多线程）

### 2.2 已执行测试集
本次使用随机占位符生成请求：
- SET：`set key:__rand_int__ value:__rand_int__`（payload 61 bytes）
- HSET：`hset myhash field:__rand_int__ value:__rand_int__`（payload 76 bytes）
- keyspace：`-r 10000`

执行了两组场景：
1) **场景 A（短跑/冒烟）**：`n=20000, c=50, r=10000`
2) **场景 B（更高并发/更稳定统计）**：`n=100000, c=100, r=10000`

---

## 3. 结果汇总

### 3.1 场景 A：n=20000, c=50, r=10000

#### 3.1.1 HSET（payload=76B）
| 指标 | Redis_Go(6666) |
|---|---:|
| 吞吐 (req/s) | **106,951.87** |
| avg (ms) | 0.371 |
| p50 (ms) | 0.343 |
| p95 (ms) | 0.559 |
| p99 (ms) | 1.095 |
| max (ms) | 7.767 |

#### 3.1.2 SET（payload=61B）
| 指标 | Redis_Go(6666) |
|---|---:|
| 吞吐 (req/s) | **100,000.00** |
| avg (ms) | 0.383 |
| p50 (ms) | 0.359 |
| p95 (ms) | 0.559 |
| p99 (ms) | 0.943 |
| max (ms) | 7.831 |

**结论（场景 A）**
- 在 `c=50` 下，SET/HSET 均达到 **10万 rps** 量级。
- 延迟分布集中：p50 在 **0.34–0.36ms**，p99 在 **~0.94–1.10ms**。
- max 约 **7–8ms**，属于少量长尾事件（可能来自瞬时排队/调度/GC 等）。

---

### 3.2 场景 B：n=100000, c=100, r=10000

#### 3.2.1 HSET（payload=76B）
| 指标 | Redis_Go(6666) |
|---|---:|
| 吞吐 (req/s) | **128,205.13** |
| avg (ms) | 0.539 |
| p50 (ms) | 0.495 |
| p95 (ms) | 0.847 |
| p99 (ms) | 1.343 |
| max (ms) | 4.615 |

#### 3.2.2 SET（payload=61B）
| 指标 | Redis_Go(6666) |
|---|---:|
| 吞吐 (req/s) | **134,770.89** |
| avg (ms) | 0.509 |
| p50 (ms) | 0.479 |
| p95 (ms) | 0.783 |
| p99 (ms) | 1.055 |
| max (ms) | 8.175 |

**结论（场景 B）**
- 并发提高到 `c=100` 后，吞吐进一步提升：
  - SET：**134,770.89 rps**
  - HSET：**128,205.13 rps**
- 延迟随并发上升略有抬升，但仍保持在亚毫秒到 1.x 毫秒的区间：
  - SET p99：**1.055ms**
  - HSET p99：**1.343ms**

---

## 5. 关键现象与分析

### 5.1 并发从 50 → 100 的收益与代价
- SET 吞吐：100,000.00 → 134,770.89（**+34.8%**）
- HSET 吞吐：106,951.87 → 128,205.13（**+19.9%**）
- 同时 p50/p99 有一定抬升（更高并发带来更高排队概率/调度开销），但仍保持在低毫秒以内。

### 5.2 SET vs HSET 的表现
- 在 `c=100` 场景下，SET 吞吐略高于 HSET（134.8k vs 128.2k）。
- HSET payload 更大（76B vs 61B），且涉及 hash 结构路径，p99 也略高（1.343ms vs 1.055ms），符合预期。

---

## 6. 总结

- 在 `-r 10000` 的随机 key/field/value 写入口径下，Redis_Go（6666）在两组场景中表现稳定：
  - `c=50`：SET/HSET 均在 **10万 rps** 左右，p99 约 **1ms**。
  - `c=100`：吞吐提升至 **~128k–135k rps**，p99 约 **1.1–1.34ms**。

---

## 附录：本次使用的关键命令

### 场景 A（n=20000, c=50, r=10000）
```bash
redis-benchmark -h 172.31.0.1 -p 6666 -n 20000 -c 50 -r 10000 hset myhash field:__rand_int__ value:__rand_int__
redis-benchmark -h 172.31.0.1 -p 6666 -n 20000 -c 50 -r 10000 set key:__rand_int__ value:__rand_int__
```
### 场景 B（n=100000, c=100, r=10000）
```bash
redis-benchmark -h 172.31.0.1 -p 6666 -n 100000 -c 100 -r 10000 hset myhash field:__rand_int__ value:__rand_int__
redis-benchmark -h 172.31.0.1 -p 6666 -n 100000 -c 100 -r 10000 set key:__rand_int__ value:__rand_int__
```
