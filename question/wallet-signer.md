# Wallet 签名隔离与 HD 密钥管理

> 写给 Java 后端：如果你只写过 CRUD、没接触过加密和区块链，先把下面"零基础前置"看完，后面的章节就不绕了。

---

## 零、零基础前置概念

### 0.1 私钥到底是什么？为什么这么宝贵？

把"链上账户"想象成银行账户，但**没有银行**。
- 传统银行：你忘密码可以挂失补办，因为银行有客服。
- 区块链：没人管你，**谁掌握私钥谁就是账户主人**。私钥泄露 = 钱被偷且永远追不回。

所以代码里只要碰到"私钥"两个字，你就该想：**这是一段绝对不能让人看见的二进制数据**——不能写日志、不能 toString、不能进异常 message、内存里用完立刻清零。

### 0.2 公钥 / 私钥 / 地址 / 签名 怎么串起来？

类比**公司公章**：
| 区块链概念 | 类比 | 作用 |
|---|---|---|
| 私钥 | 公章本体 | 盖章用，不能给别人看 |
| 公钥 | 公章的高清照片 | 公开发出去都行，别人能用照片**验**章是不是真的，但不能用照片**盖**章 |
| 地址 | 公司的门牌号（公章照片的指纹） | 别人转钱时填你的门牌号 |
| 签名 | 盖了章的合同 | 用私钥盖章 → 别人用公钥（照片）验真 |

**关键单向性**：私钥能算出公钥，公钥**算不回**私钥（数学上 secp256k1 椭圆曲线保证）。所以公开公钥/地址完全安全。

业务里"签名"= 用私钥对一笔交易盖章，盖完就把章收回保险柜。本项目里"提币"流程：
1. 业务组装"从地址 A 转 100 USDT 到 B"这条 RawTx
2. 让 `Signer` 用 A 的私钥盖章 → SignedTx
3. 把 SignedTx 广播到链上节点

### 0.3 助记词 / 种子 / 私钥 三者啥关系？

```
12 个英文单词 (助记词，给人记的)
       │ PBKDF2 哈希算法（抗暴力破解）
       ▼
64 字节 seed (机器用的种子)
       │ HMAC + 路径派生
       ▼
N 把私钥 (每个地址一把)
```

**为什么这么绕？**
- 直接让用户记 64 字节十六进制串？记不住、容易抄错。
- 12 个英文单词从 2048 词库里选，有校验位，抄错了能发现。
- 一颗种子能"长"出无穷多把私钥（HD 钱包），交易所给每个用户分配独立充币地址，背后都是同一颗种子派生的。

### 0.4 为什么不直接把私钥存 DB？要"加密"是加密啥？

直接 `INSERT INTO key_material(priv_key) VALUES (...)` 的问题：
- DBA 能看到、备份能看到、SQL 注入能拖走。

所以套两层壳（**信封套信封**）：

```
真正的私钥
  ▲ 用「数据密钥 DEK」加密  ← AES-GCM 算法
密文 + IV + tag → 存 DB
                                   
数据密钥 DEK 自己也是秘密
  ▲ 用「主密钥 MK」加密     ← KMS 服务
加密后的 DEK → 存 DB（或不存，每次找 KMS 要）

主密钥 MK 放哪？
  - 开发期：配置文件里（明文，只能 dev 用）
  - 生产期：AWS KMS / HashiCorp Vault，永远不出硬件
```

**为什么搞两层而不是一层？** 主密钥极少改（轮换成本高），数据密钥可以一条 key 一个，泄漏一条不连累其他。这叫**信封加密 (envelope encryption)**，是云厂商标准做法。

### 0.5 AES / GCM / CBC / IV 是啥？

- **AES** = 一种对称加密算法（加密和解密用同一把钥匙）。Java JDK 自带，不用装库。
- **GCM / CBC** = AES 的两种"工作模式"，决定怎么把长数据切片加密。
  - **CBC**：老牌模式，**只加密**，要不要校验完整性自己想办法（容易写错）
  - **GCM**：现代模式，**加密 + 完整性校验一步到位**，密文被改一个字节解密就直接失败
- **IV (Initialization Vector，初始向量)** = 一个 12 字节的"随机数盐"。同样的明文 + 同样的钥匙 + **不同的 IV** = 完全不同的密文，攻击者看不出规律。

记住一条铁律：**同一把 key 永远不能重用同一个 IV**，否则 GCM 直接被破解。所以代码里每次加密都 `RNG.nextBytes(iv)` 现生成。

### 0.6 这里说的"链"指什么？

BTC（比特币）/ ETH（以太坊）/ TRON（波场）—— 三条不同的区块链网络。
- 算签名时，三家虽然底层都用 secp256k1 椭圆曲线（巧合），但**交易格式完全不同**：
  - BTC：UTXO 模型 + PSBT 字节布局
  - ETH：账户模型 + RLP 编码
  - TRON：账户模型 + Protobuf
- 所以代码里要给每条链写一个独立的 `XxxChainSigner`，业务侧用统一的 `Signer` 接口包起来——这就是第五节讲的"路由模式"。

### 0.7 BC / Bouncy Castle 是什么？

Java 标准库不带椭圆曲线 secp256k1 实现（JDK 自带的是 NIST P-256，币圈不用）。所以引入第三方库 **Bouncy Castle (BC)**——业界事实标准的 Java 加密库，专门补 JDK 缺的算法。

代码里看到 `org.bouncycastle.*` 就是它。

### 0.8 看完前置，再看主体

下面 7 节都是**深入技术细节**，每节有：
- **题面**：面试官可能怎么问
- **白话理解**：用类比讲清直觉
- **要点**：技术上的硬干货
- **项目中**：本仓库具体哪个类
- **延伸追问**：面试展开

读不下去就先回头看 0.1~0.7。

---

## 一、私钥生命周期闭环

### 题面
钱包私钥从生成到使用要怎么管理？业务代码能直接拿到私钥字节吗？

### 白话理解
把私钥想象成保险柜里的钥匙：
- **派生**：照着主钥匙图纸（seed）刻一把子钥匙（priv）
- **加密**：钥匙锁进保险柜（KMS 加密）
- **持久化**：保险柜放进金库（DB）
- **解密**：要签字时打开保险柜把钥匙拿出来
- **清零**：用完立刻把钥匙熔掉（`Arrays.fill(buf, 0)`）

业务代码（提币、归集）= 委托人，只能让保管员（`Signer`）帮你签字，**永远不能亲手摸到钥匙**。所有"取钥匙→签字→熔钥匙"动作都封死在 `wallet.signer` 包内一个方法栈里。

### 要点
- 生命周期五步：派生 → 加密 → 持久化 → 解密 → 清零。
- 每一步都封闭在 `wallet.signer` 包内，业务代码（withdraw/sweep 等）**永远不接触 byte[] 私钥**。
- 业务调 `Signer.sign(RawTx, KeyRef)` 拿 SignedTx；私钥的取出、签名、清零在 `SignerImpl` 内的同一个方法栈内完成。
- 异常 message 严禁含私钥；`@Data` 生成的 toString 必须 `@ToString.Exclude` 含私钥的字段。

### 项目中
- `wallet.signer.SignerImpl#sign`：try/finally 块，finally 内 `wipe(seed)` + `wipe(hd.privateKey)` + `wipe(priv)`
- 单测断言 `priv.containsOnly((byte)0)` + `seed.containsOnly((byte)0)` 验证清零

### 延伸追问
- 为什么三处都 wipe？— `priv` 是 `hd.privateKey` 的引用，看似一次清两次，但保险起见——重构时谁动了哪个引用都不至于漏。
- BC 内部把 byte[] 转成 BigInteger 怎么办？— BigInteger 不可清零，是 BC API 设计的妥协；只能尽快让对它的引用脱离作用域，依赖 GC。已知不完美。

---

## 二、AES-256-GCM 为什么不用 CBC

### 题面
加密私钥为什么选 AES-GCM 不选 AES-CBC？

### 白话理解
对比快递包裹的两种封装：
- **CBC 模式**：纸箱封口（加密）+ 另贴防伪标（HMAC 校验完整性）。两步，标贴漏了或者校验代码写错，包裹被掉包都不知道。
- **GCM 模式**：带防伪锁的纸箱（AEAD），开锁的同时验证完整性，**一步搞定**，少一处出错可能。

**IV（初始向量）** = 每个包裹的独立编号。
- 同一把锁（key）+ 同一个编号（IV）加密两个不同包裹 = 攻击者把两份密文 XOR 一下就能逆推出明文关系，**直接破密**。
- 所以每次加密必须用 `SecureRandom` 生成新 IV，绝不能写死或全 0。

### 要点
- **GCM 自带认证（AEAD）**：tag 校验防篡改，CBC 必须额外配 HMAC，多一步出错可能。
- **GCM 标准参数**：12B IV + 16B (128-bit) tag。8B tag 也能用但安全余量低。
- **IV 必须 SecureRandom 12B**：严禁固定 IV，**同 key 重用 IV = AES-GCM 灾难，可被逆推 key**（见 NIST SP 800-38D §8.3）。
- 全 0 IV 同样不安全：同 key 不同 plaintext 仍泄漏 XOR 关系。

### 项目中
`wallet.signer.kms.AesGcmCipher`：
- `IV_BYTES = 12; TAG_BITS = 128;`
- `encrypt`：每次调用 `RNG.nextBytes(iv)` 生成新 IV
- `decrypt` 失败抛 `IllegalStateException` 包装 `AEADBadTagException`

### 延伸追问
- 为什么不用 ChaCha20-Poly1305？— Java 17+ 自带 AES-GCM 硬件加速（AES-NI），性能更好；BC 也支持但默认走 JDK provider 即可。
- 用 BC 的 AES 实现行不行？— 行，但项目里只在 secp256k1 等链特定算法用 BC，AES 走 JDK 默认 provider 性能更好。

---

## 三、KmsProvider 抽象

### 题面
本地 keystore 和云 KMS 怎么切换？

### 白话理解
把"加密私钥"这件事再套一层壳：
- **私钥（要保护的东西）** —— 用 **数据密钥 DEK** 加密 → 存 DB
- **数据密钥 DEK** —— 用 **主密钥 MK** 加密保护 → 存哪？由 KmsProvider 决定

`KmsProvider` 就是个**插座**：业务代码插上去（`resolveDataKey(alias)`）就能拿到 DEK，不关心墙后面接的是哪根电线。
- 开发期：插座后面是本地配置文件（`LocalKeystoreKmsProvider`）
- 生产期：换成 AWS KMS / Vault，**业务代码一行不改，只换 Bean**

`kms_alias` 字段记每条 key 用哪把主钥加密，相当于贴标签。轮换主密钥时新数据贴 `v2`、老数据贴 `v1`，慢慢迁移，不用停机。

### 要点
- 接口先于实现：`KmsProvider.resolveDataKey(alias) → byte[32]` + `defaultAlias()`。
- 本地实现：`LocalKeystoreKmsProvider` 从 `wallet.signer.kms.local-master-key-base64` 读 32B 主密钥；未配置则 dev 用随机生成。
- 云 KMS 实现（生产）：调 AWS KMS / HashiCorp Vault 的 Decrypt API，加密的数据密钥（DEK）从 `key_material.kms_alias` 解出。**业务代码零改动，只换 Bean**。
- `kms_alias` 字段记录每条 key 用哪个主密钥加密，支持滚动轮换：新数据用新 alias，老数据沿用旧 alias 渐进迁移。

### 项目中
- `wallet.signer.kms.KmsProvider` 接口
- `wallet.signer.kms.LocalKeystoreKmsProvider`：`@Component` + `ConcurrentHashMap` 缓存
- `key_material.kms_alias` 字段在 V2 schema 里

### 延伸追问
- 主密钥放配置文件 dev 行不行？— dev 行（用 base64 注入），生产**严禁**。生产应该让运维启动时从 vault 注入到环境变量，或者直接走云 KMS（连主密钥都不出 HSM）。
- 怎么轮换主密钥？— 新主密钥 alias=`v2`，新写入用 v2，老 v1 数据保留；定时 job 用 v1 解 + v2 重加密迁移；全部迁完再废 v1。

---

## 四、BIP-32/39/44 派生路径

### 题面
HD 钱包怎么从一个种子派生出 N 个地址？路径 `m/44'/60'/0'/0/0` 各段什么意思？

### 白话理解
**HD = Hierarchical Deterministic（分层确定性）**——一颗种子能"长"出无穷多把钥匙，且能复现。

类比组织架构图：
```
助记词 (12个英文单词，人能记住)
   ↓ PBKDF2 哈希 2048 次
seed (64 字节，机器用)
   ↓ HMAC
master key (m，根节点)
   ↓ 按路径分叉
m / 44' / 60' / 0'  /  0  /  0
    │     │    │      │    │
    │     │    │      │    └─ 第几个地址（0,1,2... 用一个换一个）
    │     │    │      └────── 0=收款地址 1=找零地址
    │     │    └───────────── 第几个账户（多账户隔离）
    │     └────────────────── 哪条链：BTC=0 ETH=60 TRX=195
    └──────────────────────── 固定写 44，表示遵循 BIP-44 规范
```

**`'` 撇号 = hardened（强化派生）**：
- 不带撇号：父公钥就能算出子公钥（观察钱包能看余额但不能花钱）
- 带撇号：必须父私钥才能派生子节点（多一道墙，泄露 xpub 不会连累上层）

设计取舍：前 3 段加撇号（保护账户），后 2 段不加（让交易所/钱包能批量预生成收款地址）。

### 要点
- BIP-39：助记词 (12/24 词) → PBKDF2-HMAC-SHA512 2048 iter → 64B seed。
- BIP-32：seed → master key (HMAC-SHA512 with `"Bitcoin seed"` constant) → 树状派生（chain code + private key）。
- BIP-44 路径模板：`m / purpose' / coin_type' / account' / change / address_index`
  - `purpose=44`：BIP-44 标准
  - `coin_type` 按 SLIP-44：BTC=0, ETH=60, TRX=195
  - `account`：账户隔离（多账户）
  - `change`：0 外部链（收款），1 内部链（找零）
  - `address_index`：递增整数，每个地址递增
- **撇号 `'` = hardened derivation**（索引 + 0x80000000）：从父私钥派生子私钥，**无法从 xpub 反推**。前 3 段 hardened，后 2 段 non-hardened（让观察钱包能从 xpub 算出所有地址）。

### 项目中
- `wallet.signer.hd.Bip39MnemonicService`：12 词 + 64B seed
- `wallet.signer.hd.Bip32HdKeyDeriver.parsePath`：处理撇号 → `n | 0x80000000`
- `leftPad32`：BC 的 `BigInteger.toByteArray()` 偶发首字节 = 0 被裁，要补回 32B

### 延伸追问
- 为什么 BTC/ETH 都能用 secp256k1？— 巧合（中本聪选了它），但 EdDSA、Sr25519 等链就不能共用此派生。
- 同 seed 派生 ETH 地址 1 和 BTC 地址 1 之间有关系吗？— 路径不同（coinType 不同），不同密钥，不同地址，相互独立。
- ED25519 链（Solana 等）能这么派生吗？— 不能。ED25519 用 SLIP-0010 派生（变种），所有段都 hardened。

---

## 五、ChainSpecificSigner 路由模式

### 题面
BTC/ETH/TRON 签名算法都不同，业务怎么不写 if-else？

### 白话理解
类比快递分拣：业务代码 = 寄件人，只把"包裹（RawTx）+ 地址（KeyRef）"丢给前台（`Signer`）。前台看一眼地址上的国家代码（`Chain`），扔进对应的传送带（`BtcChainSigner` / `EthChainSigner` / `TronChainSigner`），出来就是签好字的包裹。

技术实现：
- 启动时 Spring 把所有实现 `ChainSpecificSigner` 的 Bean 收集成 `List`
- `SignerImpl` 把 list 转成 `Map<Chain, ChainSpecificSigner>`，按 `chain()` 自动分桶
- 业务调 `signer.sign(rawTx, keyRef)`，路由表里查 → 转给对应实现

新增一条公链 = 写一个 `@Component class XxxChainSigner`，**其他代码一行不动**——这就是所谓"开闭原则"的真实落地。

### 要点
- 业务依赖一个 `Signer` Bean（接口），调用 `sign(RawTx, KeyRef)`。
- `SignerImpl` 内部 `Map<Chain, ChainSpecificSigner>` 路由（按 `chain()` 自动分桶）。
- 各链实现一个 `@Component class XxxChainSigner implements ChainSpecificSigner`，启动期被 Spring 收集进 `List<ChainSpecificSigner>` 注入到 `SignerImpl`。
- 路由 miss 时抛 `IllegalStateException`——启动期 / 集成测试就能发现，不是运行期。
- `SignerImpl.sign` 入口校验 `rawTx.chain == keyRef.chain`，否则抛 `IllegalArgumentException`。

### 项目中
- `wallet.signer.ChainSpecificSigner` 接口（内部 SPI）
- `wallet.signer.SignerImpl`：`chainSigners.stream().collect(toMap(chain, identity))` 路由
- 各链实现在 `wallet.chain.btc/eth/tron`（Plan 2/4/5 落地）

### 延伸追问
- 为什么不用 `ServiceLoader`？— Spring 上下文里直接用 Bean + Map，可控可测；ServiceLoader 不能注入依赖。
- 多链共用同一种算法（BTC/ETH/TRON 都 secp256k1），还要 3 份实现吗？— 是的。签名算法虽同，但 raw tx 字节布局完全不同（PSBT vs RLP vs Tron Tx），DER 编码也有差异。

---

## 六、为什么不用 String 持有私钥

### 题面
`String` 不也是字节数组吗，为什么必须 byte[]？

### 白话理解
`String` 在 JVM 里像**水泥铸进墙**：一旦放进 String pool 就不可变，你赋值 `s = null` 只是松开了引用，**水泥块还在墙里**，等 GC 哪天高兴才清理；期间内存 dump、core dump、swap 文件都能挖出来。

`byte[]` 像**白板写字**：用完拿橡皮擦立刻擦掉（`Arrays.fill(buf, (byte)0)`），下一秒别人 dump 内存只看到一片 0。

同理 Java 标准库密码 API 是 `char[]` 不是 `String`——这是 Java 安全编码的常识。封一层 `AesGcmCipher.wipe()` 命名清晰，code review 一眼能看到"这里在销毁"。

**已知妥协**：BC 库内部把 byte[] 转 BigInteger 做 secp256k1 运算，BigInteger 不可清零，只能让引用尽快出作用域靠 GC——这是上游 API 设计的缺陷，做不到完美。

### 要点
- **String 不可变**：值进 String pool 后无法清零，直到 GC（甚至 GC 后仍可能在 dump 里）。
- **byte[] 可变**：用完立刻 `Arrays.fill(buf, (byte)0)` 清零。
- 同理用 `char[]` 而非 `String` 装密码（这是 Java 安全编程基础）。
- 工具类 `AesGcmCipher.wipe(byte[])` 包一层语义化命名，让 review 一眼看到"这里在清零"。

### 项目中
- 全链路 byte[]：`KeyMaterialService.loadSeed → byte[] seed → derive → byte[] priv → sign → wipe`
- 不出现 hex-encoded String 形式的私钥

### 延伸追问
- BigInteger 怎么处理？— 妥协：BC API 必须 BigInteger，让它尽快脱离作用域，已知不完美。
- 日志会泄漏吗？— 严禁打私钥；异常 message 用通用文案；`@Data` 字段加 `@ToString.Exclude`。

---

## 七、容器化下的 SecureRandom 熵池

### 题面
BIP-39 生成助记词用 SecureRandom，容器里熵不够怎么办？

### 白话理解
**熵 = 随机性的"水位"**。Linux 把它当水池管理：
- `/dev/random` 像**纯净水**：水不够就阻塞等接雨（鼠标键盘网络包扰动）。`SecureRandom.getInstanceStrong()` 走这条，**容器刚启动几乎没熵，会卡死几十秒**。
- `/dev/urandom` 像**自来水**：水位低就用算法循环利用现有熵。`new SecureRandom()` 走这条，不阻塞。Linux 4.8+ 后这两个的密码学强度其实等价。

容器场景的痛：宿主机的中断、IO 都被虚拟化层"吞"了，pod 自己几乎没东西扰动熵池——所以装 `haveged` 给它"造水"，或者直接挂宿主机的 `/dev/urandom`。

**生产环境真正生成助记词不靠应用进程**——用专用安全设备（HSM）现场生成，应用只见加密后的种子。本项目的 `Bip39MnemonicService` 用 `new SecureRandom()` 是开发/测试便利。

### 要点
- `SecureRandom.getInstanceStrong()` 在 Linux 走 `/dev/random`，熵不足时 block。
- 默认 `new SecureRandom()` 在 Linux 走 `/dev/urandom`，不 block 但熵质量与 random 等价（Linux kernel 4.8+）。
- 容器化建议装 `haveged` / `rng-tools` 补熵，或者依赖宿主机的 `/dev/urandom`。
- **生产环境助记词由专用安全设备生成**，不依赖应用进程的 RNG。

### 项目中
`Bip39MnemonicService` 用 `new SecureRandom()`（不阻塞），仅作开发/测试。

### 延伸追问
- TLS 握手用的随机数也是这个吗？— 是。如果熵不够，TLS 握手都会异常。补熵是基础设施级问题。
