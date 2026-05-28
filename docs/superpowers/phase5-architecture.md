# Phase 5 — Wallet Signer 架构全景

> 用图说话。每张图配一句话点睛，看不懂图再看下面的文字。

---

## 一、Phase 5 在整个 Plan 1 中的位置

```
┌──────────────────────────────────────────────────────────────────┐
│                        业务层（链无关）                           │
│   wallet.withdraw  │  wallet.sweep  │  wallet.scanner            │
└────────────────────────┬─────────────────────────────────────────┘
                         │ 只 import "接口"，不 import 任何具体链
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│  Phase 4 chain.api    │  Phase 5 signer  │  Phase 6/7 nonce/fee  │
│  ─────────────────    │  ──────────────  │  ──────────────────   │
│  ChainClient (SPI)    │  ★ Signer (★)   │  NonceAllocator       │
│  TxBuilder   (SPI)    │  KmsProvider     │  FeeStrategy          │
│  TxBroadcaster(SPI)   │  HD Deriver      │                       │
│  TxParser    (SPI)    │  KeyMaterial     │                       │
│  AddressDeriv(SPI)    │  AesGcmCipher    │                       │
└──────────────────────────────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│  各链实现（Plan 2/4/5 落地）                                     │
│  wallet.chain.btc   │  wallet.chain.eth   │  wallet.chain.tron   │
└──────────────────────────────────────────────────────────────────┘
```

**点睛**：Phase 5 给业务层提供"一个签名服务"——业务调 `Signer.sign(rawTx, keyRef)`，链无关。

---

## 二、Phase 5 内部分层（一眼看清楚谁依赖谁）

```
                ┌─────────────────────────────────┐
                │   业务层（withdraw/sweep）      │ ← 只看到 Signer 接口
                └──────────────┬──────────────────┘
                               │ inject
                               ▼
        ┌─────────────────────────────────────────────────┐
        │                SignerImpl                       │
        │  （单例，按 chain 路由 + try/finally 清零）      │
        └──┬─────────────┬─────────────┬─────────────┬────┘
           │             │             │             │
           ▼             ▼             ▼             ▼
  ┌──────────────┐  ┌─────────┐  ┌──────────┐  ┌──────────────┐
  │KeyMaterial   │  │ Bip32   │  │ Chain    │  │ AesGcmCipher │
  │Service       │  │ Hd      │  │ Specific │  │ .wipe()      │
  │              │  │ Deriver │  │ Signer   │  │              │
  │loadSeed/save │  │ derive  │  │ sign     │  │ 内存清零      │
  └──────┬───────┘  └─────────┘  └──────────┘  └──────────────┘
         │
         ▼
  ┌──────────────┐  ┌─────────────────┐
  │ AesGcmCipher │  │ KmsProvider     │ ← 抽象接口
  │ encrypt/     │  │ resolveDataKey  │
  │ decrypt      │  └────────┬────────┘
  └──────────────┘           │ implements
                             ▼
                  ┌────────────────────────┐
                  │ LocalKeystoreKms       │ ← 本期实现
                  │ Provider (dev only)    │
                  │                        │
                  │ 生产换成 AwsKmsProvider │
                  └────────────────────────┘
```

**点睛**：每一层只关心自己的事——`SignerImpl` 编排、`KeyMaterialService` 管落库、`Deriver` 管派生、`Cipher` 管加密。

---

## 三、私钥生命周期（这是 Phase 5 的灵魂）

```
   ┌────────────────────────────── 写入阶段（一次性）──────────────────────────────┐
   │                                                                              │
   │  ① 生成助记词           ② 助记词 → seed         ③ AES-GCM 加密落库            │
   │  ┌───────────┐         ┌─────────────┐          ┌─────────────────┐         │
   │  │ 12 单词   │ PBKDF2  │  64B seed   │ AES-GCM  │ key_material 表 │         │
   │  │ entropy  ├────────► │ (明文,内存) ├────────► │ iv|ciphertext|tag│         │
   │  │ 128 bit  │ 2048iter │             │ wipe后   │ + kms_alias     │         │
   │  └───────────┘         └─────────────┘          └─────────────────┘         │
   │                                                                              │
   └──────────────────────────────────────────────────────────────────────────────┘

      ⏬ 时间流逝（DB 里只存密文，进程内无明文）⏬

   ┌────────────────────── 签名阶段（每次提现都跑一遍）─────────────────────────┐
   │                                                                            │
   │  ④ 解密             ⑤ HD 派生            ⑥ 签名             ⑦ 立即清零    │
   │ ┌─────────────┐    ┌──────────────┐    ┌──────────────┐    ┌────────────┐│
   │ │ DB 取密文   │    │ seed +       │    │ priv +       │    │ wipe(seed) ││
   │ │ KMS 拿 DEK  ├──► │ m/44'/60'/.. ├──► │ rawTx        ├──► │ wipe(hd)   ││
   │ │ 解出 seed   │    │ → 子私钥     │    │ → SignedTx   │    │ wipe(priv) ││
   │ └─────────────┘    └──────────────┘    └──────────────┘    └────────────┘│
   │       内存            内存                内存               byte 全 0   │
   │                                                                            │
   │  全程在  SignerImpl.sign()  方法的同一个 try/finally 栈帧内               │
   │                                                                            │
   └────────────────────────────────────────────────────────────────────────────┘
```

**点睛**：明文私钥**只在 ④⑤⑥ 三个步骤的几十毫秒内存在**，⑦ 立即清零。这就是「签名隔离」。

---

## 四、信封加密（KEK-DEK）—— 为什么叫"信封"

```
           ┌───────────────────────────────────────────────┐
           │                  最外层信封                   │
           │  ┌─────────────────────────────────────────┐  │
           │  │           中间层信封                    │  │
           │  │  ┌──────────────────────────────────┐   │  │
           │  │  │      最内层（要保护的）          │   │  │
           │  │  │       明文 seed (64B)            │   │  │
           │  │  └──────────────┬───────────────────┘   │  │
           │  │     用 DEK 加密 │                        │  │
           │  │                 ▼                        │  │
           │  │       密文 + IV + Tag → 存 DB            │  │
           │  │     (key_material.cipher_blob)           │  │
           │  └──────────────┬───────────────────────────┘  │
           │   用 MK 加密 DEK│                              │
           │                 ▼                              │
           │       加密后的 DEK → 存 DB                     │
           │     (key_material.encrypted_dek)               │
           └──────────────┬────────────────────────────────┘
              MK 永远不出 │
                          ▼
              ┌─────────────────────────┐
              │   主密钥 MK 存哪？      │
              ├─────────────────────────┤
              │ 开发期：配置文件 base64  │
              │ 生产期：AWS KMS / Vault  │
              │   （MK 永不出 HSM）      │
              └─────────────────────────┘
```

**点睛**：DB 拖库 → 只拿到密文 + 加密的 DEK，没 MK 就是乱码。MK 不在 DB，攻击者拿 DB 不拿 MK = 解不开。

---

## 五、SignerImpl 核心代码骨架（连图带字一起读）

```
public SignedTx sign(RawTx rawTx, KeyRef keyRef) {
    
    ┌─────────────── 启动期 Spring 注入 ───────────────┐
    │ chainSigners = [BtcSigner, EthSigner, TronSigner]│
    │ registry = Map.of(BTC→Btc, ETH→Eth, TRON→Tron)   │
    └──────────────────────────────────────────────────┘
    
    ChainSpecificSigner cs = registry.get(rawTx.chain);  ← 路由
    
    byte[] seed=null, priv=null;
    Bip32HdKeyDeriver.HdKey hd=null;
    
    ╔══════════════════════════════════════════════════╗
    ║ try {                                            ║
    ║   seed = keyMaterialService.loadSeed(keyId);     ║  ← 步骤④ 解密
    ║   hd   = deriver.derive(seed, hdPath);           ║  ← 步骤⑤ 派生
    ║   priv = hd.getPrivateKey();                     ║
    ║   return cs.sign(rawTx, priv);                   ║  ← 步骤⑥ 签名
    ║                                                  ║
    ║ } finally {                                      ║
    ║   AesGcmCipher.wipe(seed);                       ║  ← 步骤⑦ 清零
    ║   AesGcmCipher.wipe(hd.getPrivateKey());         ║
    ║   AesGcmCipher.wipe(priv);                       ║
    ║ }                                                ║
    ╚══════════════════════════════════════════════════╝
    
    ↑ 哪怕 cs.sign() 抛异常，finally 也保证清零
}
```

**点睛**：`try/finally` 是私钥安全的最后一道防线——异常路径也清零。

---

## 六、ChainSpecificSigner 路由模式（为什么业务不用写 if-else）

```
                    ┌─────────────────────────┐
                    │  业务代码                │
                    │  signer.sign(rawTx, ref) │  ← 只认一个 Signer Bean
                    └──────────┬──────────────┘
                               ▼
                    ┌─────────────────────────┐
                    │  SignerImpl             │
                    │  (按 rawTx.chain 路由)  │
                    └──────────┬──────────────┘
                  ┌────────────┼─────────────┐
                  ▼            ▼             ▼
          ┌──────────┐  ┌──────────┐  ┌──────────┐
          │BtcSigner │  │EthSigner │  │TronSigner│  ← 各链独立实现
          │chain=BTC │  │chain=ETH │  │chain=TRX │
          │PSBT 编码 │  │RLP 编码  │  │Protobuf  │
          └──────────┘  └──────────┘  └──────────┘
                ▲
                │ 启动期被 Spring 收集成 List<ChainSpecificSigner>
                │ SignerImpl 构造时 toMap(chain, identity) 转成 Map
                │
          @Component class BtcSigner implements ChainSpecificSigner {
              public Chain chain() { return BTC; }
              public SignedTx sign(RawTx, byte[] priv) { ... }
          }
```

**点睛**：新加一条链 = `@Component` 写个新类，**业务代码一行不动**。这就是开闭原则的真实落地。

---

## 七、HD 派生路径（BIP-32/39/44 在一张图里）

```
       ┌──────────────────────────────────────┐
       │   12 个英文单词 (BIP-39，给人记)     │
       │   abandon ability able about ...     │
       └──────────────┬───────────────────────┘
                      │ PBKDF2-HMAC-SHA512
                      │ (2048 iter, "mnemonic"+passphrase)
                      ▼
       ┌──────────────────────────────────────┐
       │   64 字节 Seed (BIP-39 输出)         │
       └──────────────┬───────────────────────┘
                      │ HMAC-SHA512("Bitcoin seed", seed)
                      ▼
       ┌──────────────────────────────────────┐
       │   Master Key m (32B priv + 32B chain code)│  BIP-32 根
       └──────────────┬───────────────────────┘
                      │ 按 BIP-44 路径分叉
                      │
       m / 44'   /  60'      /  0'      / 0      / 0
           │         │           │        │        │
           │         │           │        │        └─ address_index 第几个地址
           │         │           │        └────────── change 0=外部 1=找零
           │         │           └─────────────────── account 多账户隔离
           │         └─────────────────────────────── coin_type ETH=60 BTC=0 TRX=195
           └────────────────────────────────────────── purpose 固定 44 (BIP-44)
                              │
                              │
       带  '  撇号 = hardened（强化派生）
              ↓
       前 3 段加撇号：保护账户层不被反推
       后 2 段不加：观察钱包能用 xpub 批量算地址（不需要私钥）
```

**点睛**：一颗 seed → 无穷多把私钥（每个用户一个 hdPath），**灾备只需备一份助记词**。

---

## 八、Phase 5 文件清单（已落地）

```
wallet/src/main/java/com/exchange/wallet/signer/
│
├── SignerImpl.java                     ★ 入口 + 路由 + 清零
├── ChainSpecificSigner.java            内部 SPI（链特定）
│
├── KeyMaterialEntity.java              key_material 表 ORM
├── KeyMaterialMapper.java              MyBatis-Plus mapper
├── KeyMaterialService.java             加密存 / 解密取
│
├── kms/
│   ├── AesGcmCipher.java               AES-256-GCM + wipe()
│   ├── KmsProvider.java                抽象接口
│   └── LocalKeystoreKmsProvider.java   本地实现（dev）
│
└── hd/
    ├── Bip39MnemonicService.java       助记词↔seed
    └── Bip32HdKeyDeriver.java          路径派生
```

---

## 九、面试官最爱问的 5 个问题（一行答案）

| 问题 | 一行答 |
|---|---|
| **私钥怎么不让业务接触？** | 业务依赖 `Signer` 接口，参数是 `KeyRef(keyId+hdPath)`，`SignerImpl.sign()` 内部全栈完成 fetch→derive→sign→wipe |
| **AES-GCM 为什么不选 CBC？** | GCM 自带 AEAD 认证（防篡改一步到位），CBC 必须额外配 HMAC，多一处出错可能 |
| **String 不能装私钥？** | String 不可变，进 String pool 后 GC 也清不掉，dump 能挖出；byte[] 可 `Arrays.fill(0)` 立即清零 |
| **KMS 抽象有什么用？** | 接口 + Spring Bean，dev 用 `LocalKeystoreKmsProvider`，生产换 `AwsKmsProvider` 业务零改动 |
| **新增一条公链要改多少？** | 写个 `@Component class XxxChainSigner implements ChainSpecificSigner`，其他代码零改动 |

---

## 十、最后再背一遍核心约束

> ✋ 红线：
> - 业务代码（`wallet.core` / `wallet.withdraw` / `wallet.scanner`）**禁止 import** `wallet.signer.*` 包内任何类——只能调 `Signer` 接口
> - 私钥**严禁**进 toString / 异常 message / 日志
> - 全链路 `byte[]`，不准 hex String
> - `wipe()` 调用必须在 `finally` 块内
