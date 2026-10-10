# dsh-zhixiaohang-guard

一个宿主插件，承担两件事：

- **A. 智小航专属节流**：只对 `provider = zhixiaohang`（兜底按主机 `token.nuaa.edu.cn`）做 10 次/分钟滑动窗口排队；其它 provider（如 `deepseek-account`）**零延迟直通**。
- **B. 校外乱码入路屏蔽**：没连校园网/VPN 时网关返回的整页 HTML（含巨型 base64）在**落盘之前**被换成一句 <200 字符短提示；原文一个字节都不进会话记录、模型上下文与日志。

纯 JS、零依赖、单文件 ESM（`index.mjs`），不需要 python、不需要构建。

---

## 可观测性（v1.1.0：怎么确认插件真的激活了）

宿主**主进程日志不落盘**（`%APPDATA%\@deepseek-ai\dsh-desktop\logs` 只有崩溃日志），
`dsh --profile desktop --dump-config` 也会被 `profile "desktop" is managed exclusively by the Electron application` 拒绝——
所以从外面**看不到**任何加载记录。为此插件自带信号：

1. **状态文件**：挂载时写 `<DSH_HOME>/zhixiaohang-guard/status.json`
   （默认 `C:\Users\<你的用户名>\.dsh\zhixiaohang-guard\status.json`），含 `mountedAt` / `pid` /
   `module`（实际加载的文件路径）/ `listeners`（两个钩子是否注册）/ `selfTest`（自检结果）/
   `stats` / `lastEvents`。
2. **只读状态页**：<http://127.0.0.1:19387/zhixiaohang-guard>（JSON 在 `/zhixiaohang-guard/api/status`）。

重启后判断"到底装上了没有"，只看两条：

- 状态文件存在，且 `selfTest.ok === true`、`listeners['llm/stream'] === true`、`listeners['agent/request-error'] === true`；
- `module` 指向 `C:/Users/<你的用户名>/.dsh/plugins/zhixiaohang-guard/index.mjs`（而不是工作区里的那份）。

`selfTest` 是插件在**自己进程内**拿一段"校外乱码"样本跑一遍真实净化路径：`detectedOffCampus: true`、
`noticeChars: 97`、`noticeLeakFree: true` —— 说明净化链路在该进程内确实工作，而不只是"文件被读进去了"。

计数器（`stats` / `lastEvents`）在命中时刷新：`paced`（节流排队次数）、`sanitized`（乱码屏蔽次数）、
`claimed`（认领次数）、`emptyStreams` / `emptyStreamRetries`、`retryAfterEnriched`。

---

## 为什么挂在 `llm/stream`（实测依据，不是猜的）

宿主 0.2.0-rc.2 的请求路径（源码位置均已核对）：

```
agent loop step()
  └─ preparedCall.stream(request)                    dsh-agent-loop/lib/index.js:1072
       └─ LlmRuntime.streamWithRegistration()
            └─ ctx.waterfall(this, 'llm/stream', …)  dsh-llm/lib/index.js:2371   ← 本插件在这里
                 └─ adapterStream() → openai SDK → 网关
  └─ live.push(chunk)  →  assistant/attempt 落盘      dsh-agent-loop/lib/index.js:1119-1123
  └─ dispatch.waterfall('agent/request-error', …)     dsh-agent-loop/lib/index.js:1124  ← 已经太晚
```

关键事实：

1. `llm/stream` 是**官方公开**的 waterfall，签名 `(options, next) => AsyncIterable<StreamChunk>`，官方 invariant 自己就这么注册（带 `{ global: true, prepend: true }`）。
2. 失败**先落盘再进** `agent/request-error`：`finish` 分片被 `assistant-stream` 原样记入紧凑流（`dsh-llm/lib/types/assistant-stream.js:105`）。所以只改 `request-error` 里的消息**拦不住落盘**——必须在 `llm/stream` 处就改。
3. 跨 fiber 监听服务事件必须 `global: true`，否则被 context filter 丢掉（`cordis/src/events.ts:116,173`）。

### 校外乱码的两条真实形态

| 网关响应 | 走到哪里 | 表现 |
|---|---|---|
| 非 2xx + HTML | openai SDK 异常消息里**装着整页 HTML** | `code = SERVER`（5xx）或 `AUTH`（401/403）；pi-ai 的 4000 字符上限**不生效**（它只对"body 未被 SDK 折进 message"的错误生效，见 `pi-ai/dist/utils/error-body.js:23`） |
| 200 + HTML | SSE 解析不出任何事件 | pi-ai 抛 `Stream ended without finish_reason`（34 字节）→ `code = TRANSPORT`；**不带乱码**，但用户只看到看不懂的英文 |

本机 4 个会话日志全量扫描（2026-10-10）实证：

| 失败 | 次数 | 消息长度 | 是否携乱码 |
|---|---|---|---|
| `SERVER` | 50 | 最长 **316,850 字节** | 是（PNG 魔数 + base64 + HTML + "仅限校内访问"） |
| `TRANSPORT` `Stream ended without finish_reason` | 60 | 34 字节 | 否 |
| `RATE_LIMIT` `429 upstream_capacity_exhausted` | 264 | 197 字节 | 否（含 `retry_after_seconds`） |

---

## 行为

### 节流（A）
- 滑动窗口（默认 `rpm: 10, windowMs: 60000`）：窗口内满额时**排队等待**到有名额再发，不报错。
- 等待可取消：`signal` 中止时不再发起请求，改为产出协议里规范的 `aborted` finish。
- 队列打满（默认 512）时直接放行——宁可少节流，也不卡死会话。
- 非目标 provider 完全不进这个分支，不建定时器，延迟恒为 0。

### 屏蔽（B）
- 改写终止 `finish.reason.failure`：`message` → 短提示，`code` → `ZHIXIAOHANG_OFF_CAMPUS`，其余字段（如 `status`）保留。
- 提示 = 默认文案 +（能从原文清理出"仅限校内访问"整句时）附上该句，总长截到 `maxNoticeChars`。
- 同时在 `agent/request-error` 用 `{ global: true, prepend: true }` **抢先认领**该失败且不调用 `next()`：
  判定为"需要用户操作、不可重试"，避免 `dsh-llm-error-retry` 按 429 规则做 60s×N 空转。
- 防御性兜底：若乱码以 `text-delta` / `reasoning-delta` 正文形式出现（实测未出现），也会被换成短提示。

### 空白流（真机 60 次的那种）
- 默认先**自动重发 1 次**（仅当这一轮还没吐出任何内容时；已有正文则不重发，避免重复输出）。
- 重发成功 → 用户完全看不到错误；仍失败 → 换成中性短提示，保留 `TRANSPORT` code。
- 与已装的 `dsh-llm-finish-reason-tolerance` **互补不冲突**：那个插件只把"已吐内容"的空白流提升为成功，一个内容都没吐时它明确放行错误——正是本插件处理的部分。

### 空流其实是"网关上游容量不足"的另一副面孔（v1.2.0，真机复现）

2026-10-10 直接对网关做流式复现（`tools/probe-stream.mjs`，4 次全中）：

```
#1 429 {"message":"All upstream providers are cooling down. Please retry after 44 seconds.",
        "type":"upstream_capacity_exhausted","retry_after_seconds":44,"code":"circuit.upstream_capacity_exhausted"}
#2 429 retry_after_seconds: 43      #3 429 :42      #4 429 :42
```

同一时刻 `GET /v1/models`（带 key）返回 200 + 23,877 字节模型表 —— **链路与鉴权都正常**，是网关自己的上游在冷却。
也就是说：**校外的 200+HTML 与上游容量的 200+空流是两种不同的东西**，旧文案把后者说成"没连 VPN"是误导，v1.2.0 已改。

因此 v1.2.0 增加 `emptyStreamAsRateLimit`（默认开）：只要本插件**最近**在 429 失败消息里解析到过 `retry_after_seconds`（窗口 `capacityHintTtlMs`，默认 180s），
就把空流改写成

```
code: RATE_LIMIT, providerRetryAfterMs: <服务端提示>, message: "429: 网关上游容量不足（由空白流识别，服务端提示约 42s）…"
```

于是这次失败会**流回 `dsh-llm-error-retry`**（本插件只在"校外乱码"时认领失败，RATE_LIMIT 一律放行），按冷却自动重试，而不是丢一句死胡同提示让人干等。
没有任何容量证据时（例如纯粹的流被掐断），才走"重发 1 次 → 中性提示"的老路。

### 429 的 `retry_after_seconds` 修复
网关把 `retry_after_seconds` 写在消息 JSON 里，宿主却没填进 `failure.providerRetryAfterMs`。本插件（`honorRetryAfterSeconds: true`，默认开）从消息里解析并补上该字段，让重试插件尊重服务端提示（封顶 1 小时）。

---

## 配置

写在 `cordis.patch.yml` 那一行的 `config` 里，全部有默认值：

```yaml
- insert:
    - id: zhixiaohang-guard
      name: 'file:///C:/Users/<你的用户名>/.dsh/plugins/zhixiaohang-guard/index.mjs'
      config:
        pace: true
        rpm: 10
        windowMs: 60000
        providers: [zhixiaohang]
        hosts: [token.nuaa.edu.cn]
        sanitize: true
        notice: '⚠️ 智小航：当前不在校园网/VPN，校内系统拒绝访问。请连接校园 VPN 后重试。服务热线 (025)84890123'
        reminderKeys: [仅限校内访问, 校内访问, 综合服务门户, 校园网]
        weakReminderKeys: [VPN, vpn, 校外]
        weakKeysRequireCorroboration: true
        maxNoticeChars: 200
        offCampusCode: ZHIXIAOHANG_OFF_CAMPUS
        maxWaiters: 512
        emptyStreamRetries: 1
        emptyStreamBackoffMs: 1500
        emptyStreamNotice: '⚠️ 智小航：网关流式响应被中断（未返回结束原因），已自动重试仍未恢复。若持续如此请稍后重试；若在校外请先连接校园 VPN。'
        emptyStreamAsRateLimit: true
        capacityHintTtlMs: 180000
        honorRetryAfterSeconds: true
        precheck: false
        precheckTtlMs: 60000
        forceIpv4: true
        forceIpv4Hosts: [token.nuaa.edu.cn]
        forceIpv4Mode: auto
        forceIpv4Probe: true
```

### 强制 IPv4（v1.3.0）

需求是"调用智小航时让 DSH 强制走 IPv4"。踩点结论与实测：

- 请求通道：pi-ai 的 `profileOptions()` **不注入自定义 fetch**，openai SDK 用的是 Node 内置 `fetch`
  → 而宿主自带的 `dsh-http-proxy` 正是靠"替换 undici 全局派发器"来接管它（`dsh-http-proxy/lib/index.js:403-450`），
  因为 Node 内置 fetch 读的是同一个 `Symbol.for('undici.globalDispatcher.1')`。
- 因此本插件用**同一套路**：包一层派发器，**只**把 `forceIpv4Hosts` 里的主机换成 `connect.family = 4` 的连接池，
  其余主机原样 `dispatch` 给原派发器 —— 代理链路、其它 provider 全不受影响（`dsh-http-proxy` 用自己的 Agent 时也照旧）。
- 有代理时（`HTTPS_PROXY` 等且未命中 `NO_PROXY`）改走 `ProxyAgent`，并让到代理的连接也走 IPv4，保留代理。
- `undici` 从 profile 的 `node_modules` 解析（插件目录里没有 node_modules）；解析或安装失败时按 `forceIpv4Mode: auto`
  退回 `dns.setDefaultResultOrder('ipv4first')` + `net.setDefaultAutoSelectFamily(false)`（这一步是进程级、影响面更大，日志会写明）。

**实测（`node tools/test-ipv4.mjs`，同端口同时监听 127.0.0.1 与 ::1）：**

| 观测 | 结果 |
|---|---|
| 安装前 `http://localhost` | 落在 **IPv6**（本机确实优先 IPv6） |
| 安装后同一请求 | 落在 **IPv4** ✅ |
| 未列入名单的主机 / `[::1]` 直连 | 不受影响（IPv6 仍可用）✅ |
| 卸载后 | 恢复原派发器 ✅ |
| `token.nuaa.edu.cn` 的 A / AAAA | `10.0.241.133` / `2001:da8:1006:1001::101`（**有 AAAA**） |
| `token.nuaa.edu.cn` 的 getaddrinfo（fetch 真正用的那条） | **只返回 IPv4** |

⚠️ **诚实的结论**：机制已验证有效，但在当前网络状态下 `getaddrinfo` 对智小航只返回 IPv4，
所以对"今天的空流问题"它是**无害的空操作**（不是解药）；当某天解析器同时给出 AAAA 时它才会真正起作用。
空流的根因仍是网关上游容量（见上一节），这条只是把 IPv6 这条潜在不稳定路径提前堵上。
状态页里会记录 `ipv4.mode / previousDispatcher / dnsRecords / connectProbe`，可直接核对当时是否真的需要它。

几个需要解释的**偏离字面需求**的设计判断：

- `weakReminderKeys`（`VPN`/`vpn`/`校外`）默认必须与页面特征（HTML/base64/data URI）**同时**出现才算命中（`weakKeysRequireCorroboration: true`）。否则模型正常回答里提到"VPN"就会被整段替换掉。要退回"任一关键词命中即判定"的字面规则，把这一项设为 `false`。
- `precheck` 默认关（按你的要求）；打开后，识别到校外会在 TTL 内直接快速失败，不再白跑请求。
- 空白流默认重发 1 次。若你更希望"校外就不重发"，设 `emptyStreamRetries: 0`（仍会换成中文短提示）。

---

## 验证（可复跑）

```powershell
$node = 'C:\Users\<你的用户名>\.dsh\dsh-runtimes\dsh-primary-runtime\dependencies\node\bin\node.exe'
$py   = 'C:\Users\<你的用户名>\.dsh\dsh-runtimes\dsh-primary-runtime\dependencies\python\python.exe'

# 1) 假时钟节流单测：第 11 次必须等待、取消可中断、配置回落
& $node tools\test-pacer.mjs

# 2) 入路实测：本地假网关用真实校外 HTML，真 openai SDK 取真实失败消息，喂进插件监听器
& $node tools\test-ingress.mjs

# 3) 与 python clean_zxh.py 的逐行等价性
& $py clean_zxh.py tests\sample_full.txt -o verify\py_full_reminder.txt --reminder
& $node tools\gen-js-clean.mjs tests\sample_full.txt --out verify\js_full_reminder.txt --reminder
& $node tools\compare-clean.mjs verify\py_full_reminder.txt verify\js_full_reminder.txt
```

结果（本轮实测）：节流单测与入路实测全绿；等价性 **10/10 组（5 个样例 × 默认/--reminder）逐行完全一致**。

---

## 安装（desktop profile）

1. 把本目录整个拷到稳定位置：`C:\Users\<你的用户名>\.dsh\plugins\zhixiaohang-guard\`
   （**不要**放进 `node_modules`，任何重装/更新都会覆盖它。）
2. 在 `C:\Users\<你的用户名>\.dsh\profiles\desktop\cordis.patch.yml` 末尾追加：

   ```yaml
   - insert:
       - id: zhixiaohang-guard
         name: 'file:///C:/Users/<你的用户名>/.dsh/plugins/zhixiaohang-guard/index.mjs'
         config: {}
   ```

   **`name` 必须写成 `file:///` URL**（或相对补丁文件目录的 `../../plugins/zhixiaohang-guard/index.mjs`）。
   实测（`node tools/test-specifier.mjs`，复刻加载器 `tree.ts:112-128` 的解析分支，
   baseUrl = 补丁文件所在目录 `file:///C:/Users/<你的用户名>/.dsh/profiles/desktop/`）：

   | `name` 写法 | 结果 |
   |---|---|
   | `C:/Users/<你的用户名>/.dsh/plugins/.../index.mjs` | ❌ `ERR_UNSUPPORTED_ESM_URL_SCHEME`（被当成 `c:` 协议） |
   | `C:\Users\<你的用户名>\.dsh\plugins\...\index.mjs` | ❌ 同上 |
   | `file:///C:/Users/<你的用户名>/.dsh/plugins/.../index.mjs` | ✅ 通过（两种 baseUrl 假设都通过） |
   | `../../plugins/zhixiaohang-guard/index.mjs` | ✅ 通过（依赖 baseUrl = profile 目录） |
   | `./plugins/zhixiaohang-guard/index.mjs` | ❌ `ERR_MODULE_NOT_FOUND`（那是 `~/.dsh/` 下的相对写法） |

3. **重启桌面应用**（单文件宿主插件在启动时加载）。
4. 确认加载：日志里应出现
   `zhixiaohang-guard: mounted（pace=10/60000ms，providers=[zhixiaohang]，…）`
   且**没有** `did not activate` 警告。

若发现该行被应用保存设置时抹掉（profile 文件由 Electron 应用托管），把这行改放到
`C:\Users\<你的用户名>\.dsh\cordis.patch.yml`（根补丁层，`dsh-llm-error-retry` 的 README 用的就是这个位置）。

## 不做什么

- 不改、不删已装的 `dsh-llm-error-retry`、`dsh-throttle`、`dsh-llm-finish-reason-tolerance` 等插件，只做协作。
- 不碰 `dsh-throttle` 的 `call_api` 令牌桶——那条路径管不到模型请求。
- 不依赖 python，日志里不打印任何原文（只打长度与机器码）。
