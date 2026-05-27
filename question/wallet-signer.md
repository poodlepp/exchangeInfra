# Wallet 签名隔离与 HD 密钥管理

## 一、私钥生命周期闭环

### 题面
钱包私钥从生成到使用要怎么管理？业务代码能直接拿到私钥字节吗？

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

### 要点
- `SecureRandom.getInstanceStrong()` 在 Linux 走 `/dev/random`，熵不足时 block。
- 默认 `new SecureRandom()` 在 Linux 走 `/dev/urandom`，不 block 但熵质量与 random 等价（Linux kernel 4.8+）。
- 容器化建议装 `haveged` / `rng-tools` 补熵，或者依赖宿主机的 `/dev/urandom`。
- **生产环境助记词由专用安全设备生成**，不依赖应用进程的 RNG。

### 项目中
`Bip39MnemonicService` 用 `new SecureRandom()`（不阻塞），仅作开发/测试。

### 延伸追问
- TLS 握手用的随机数也是这个吗？— 是。如果熵不够，TLS 握手都会异常。补熵是基础设施级问题。
