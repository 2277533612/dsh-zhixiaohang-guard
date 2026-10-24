/**
 * dsh-zhixiaohang-guard —— 一个插件承担两件事（宿主插件，单文件 ESM，无依赖）：
 *
 *  A. 智小航专属节流：只对匹配的 provider（默认 zhixiaohang / token.nuaa.edu.cn）
 *     做滑动窗口 30 次/分钟排队等待；其它 provider 零延迟直通。
 *  B. 校外乱码屏蔽：没连校园网/VPN 时网关返回整页 HTML（含巨型 base64 + data:image），
 *     它在真实宿主里表现为**终止失败**（不是 assistant 正文）：
 *       - 非 2xx + HTML：openai SDK 把整页 HTML 折进异常消息。pi-ai 的 4000 字符上限
 *         **只对"body 未被折进 message"的错误生效**（error-body.js:23 messageCarriesBody），
 *         所以这条路径会把整页原文一路带进 failure.message；
 *       - 200 + HTML：SSE 解析不出任何事件，pi-ai 抛 "Stream ended without finish_reason"
 *         （34 字节，不带乱码），用户只看到一句看不懂的英文。
 *     两个都成为 finish 分片的 reason.failure.message，并**先被 assistant-stream 落盘**
 *     （assistant-stream.js:105）再进 agent/request-error，所以屏蔽点必须早于落盘 —— 本插件
 *     挂在官方公开的 `llm/stream` waterfall 上就地改写，原文一个字节都不进入会话记录、
 *     模型上下文与日志；再在 `agent/request-error` 抢先认领该失败（不调用 next()），阻止
 *     llm-error-retry 去做无意义的 60s×N 休眠重试。空白流则先自动重发一次、仍失败才换文案。
 *
 * 真实宿主证据（本机 4 个会话日志 session.v4.jsonl.zstd 全量扫描，2026-10-10）：
 *   - [SERVER] 失败消息最长 316,850 字节，同时含 PNG 魔数 / base64 / HTML / "仅限校内访问"，
 *     出现在 50 条 assistant/attempt + llm/retry 记录里（会话语义上等于 15MB 乱码落盘）；
 *   - [TRANSPORT] "Stream ended without finish_reason" 出现 60 次（34 字节，无乱码）；
 *   - [RATE_LIMIT] "429: {…retry_after_seconds:47…}" 出现 264 次，宿主没填 providerRetryAfterMs。
 *
 * 完整依据（版本 0.2.0-rc.2 源码，均已实测核对）：
 *   - dsh-llm/lib/index.js:2371        streamWithRegistration → ctx.waterfall(this, 'llm/stream', options, …)
 *   - dsh-llm/lib/index.js:2223-2233   agent loop 的 preparedCall.stream 同样走该 waterfall
 *   - dsh-llm/lib/types/assistant-stream.js:105  finish 分片被原样记入紧凑流（会落盘）
 *   - dsh-agent-loop/lib/index.js:1117-1134      落盘后才 dispatch agent/request-error
 *   - pi-ai/dist/utils/error-body.js:15,23,111-118  4000 字符上限 + messageCarriesBody 例外
 *   - dsh-llm-pi-ai/lib/index.js:1380-1389       401/403 → AUTH、5xx → SERVER、Stream ended… → TRANSPORT
 *   - cordis/src/events.ts:116,173               服务事件跨 fiber 监听需 { global: true }
 *
 * 用法：export const name / inject；export function apply(ctx, config)。
 * 纯净化函数与节流器额外导出，供 tools/ 下的测试脚本直接导入（不影响宿主加载）。
 *
 * 可观测性（v1.1.0）：宿主主进程日志不落盘，外部无法确认插件是否真的激活，所以本插件
 * 挂载时会把状态写到 <DSH_HOME>/zhixiaohang-guard/status.json（含自检结果、监听器注册
 * 情况、计数器），并在 web 服务上注册只读状态路由 —— 打开
 * http://127.0.0.1:19387/zhixiaohang-guard 即可确认。
 */
import { mkdirSync, writeFileSync } from 'node:fs'
import { homedir } from 'node:os'
import { join } from 'node:path'
import { createRequire } from 'node:module'
import { pathToFileURL } from 'node:url'
import dns from 'node:dns'
import net from 'node:net'

export const VERSION = '1.4.0'

export const name = 'zhixiaohang-guard'

/** timer 服务用于可取消的等待（缺省会退化为 setTimeout）。 */
export const inject = ['timer']

//#region ------------------------------------------------------------------ 默认配置

export const DEFAULT_CONFIG = Object.freeze({
  /** 是否启用智小航专属节流 */
  pace: true,
  /** 窗口内允许的请求数（智小航官方配额 30 RPM） */
  rpm: 30,
  /** 滑动窗口长度（毫秒） */
  windowMs: 60_000,
  /** 需要节流的 provider id（优先匹配 payload/provider 的 id） */
  providers: ['zhixiaohang'],
  /** provider id 取不到时的兜底：按 baseURL 主机名匹配 */
  hosts: ['token.nuaa.edu.cn'],
  /** 是否启用校外乱码屏蔽 */
  sanitize: true,
  /** 替换后的短提示 */
  notice: '⚠️ 智小航：当前不在校园网/VPN，校内系统拒绝访问。请连接校园 VPN 后重试。服务热线 (025)84890123',
  /** 强关键词：命中即可判定为校外提示（页面特征） */
  reminderKeys: ['仅限校内访问', '校内访问', '综合服务门户', '校园网'],
  /** 弱关键词：只有同时出现页面特征（HTML/base64/data URI）时才算命中，避免误伤正常回答 */
  weakReminderKeys: ['VPN', 'vpn', '校外'],
  /** 弱关键词是否必须与页面特征同时出现（设为 false 即退回"任一命中"的字面规则） */
  weakKeysRequireCorroboration: true,
  /** 短提示长度上限（字符） */
  maxNoticeChars: 200,
  /** 屏蔽后替换 failure 使用的稳定机器码 */
  offCampusCode: 'ZHIXIAOHANG_OFF_CAMPUS',
  /** 等待队列上限，防止无界增长 */
  maxWaiters: 512,
  /**
   * 空白流（"Stream ended without finish_reason"）处理：真机扫描到 60 次。
   * 默认先自动重发 1 次（只在这一轮还没吐出任何内容时才重发，避免重复内容），
   * 仍失败则换成可操作的中文短提示。设 0 = 不重发、只换文案。
   */
  emptyStreamRetries: 1,
  /** 空白流重发前的退避（毫秒） */
  emptyStreamBackoffMs: 1500,
  /** 空白流最终替换文案（真机证据：网关上游容量不足时也以 200+空流出现，并非校园网问题） */
  emptyStreamNotice: '⚠️ 智小航：网关流式响应被中断（未返回结束原因），已自动重试仍未恢复。若持续如此请稍后重试；若在校外请先连接校园 VPN。',
  /**
   * 把"空流"当作"网关上游容量不足"来重试：只要本插件最近解析到过 429 的
   * retry_after_seconds（窗口内），就把空流改写成带 retry_after 的 RATE_LIMIT 失败，
   * 交给重试插件按冷却重试，而不是丢下一句死胡同提示。
   */
  emptyStreamAsRateLimit: true,
  /** 上面那份"容量不足"证据的有效期（毫秒） */
  capacityHintTtlMs: 180_000,
  /**
   * 网关 429 会把 retry_after_seconds 写在消息 JSON 里，宿主却没填进
   * failure.providerRetryAfterMs（重试插件因此只按自己的固定秒数等待）。
   * 打开后由本插件解析并补上该字段，让重试插件尊重服务端提示。
   */
  honorRetryAfterSeconds: true,
  /** 可选增强（默认关）：识别到校外后在该 TTL 内直接快速失败，避免每次白跑一个请求 */
  precheck: false,
  precheckTtlMs: 60_000,
  /**
   * 对指定主机强制 IPv4。背景：token.nuaa.edu.cn 同时有 A 与 AAAA 记录
   * （CERNET IPv6 2001:da8:1006:1001::101），而 Node 20+ 默认开启 Happy Eyeballs
   * 会并发尝试 IPv6；校园网 IPv6 路径不稳时就会表现为"流被中断"。
   * 实现方式与 dsh-http-proxy 同源：包一层 undici 全局派发器，只把目标主机
   * 换成 connect.family=4 的连接池，其它主机原样交给原派发器（不影响代理）。
   */
  forceIpv4: true,
  /** 需要强制 IPv4 的主机 */
  forceIpv4Hosts: ['token.nuaa.edu.cn'],
  /** auto = 先试 undici 派发器，失败再退回 DNS 层；也可写 dispatcher / dns / off */
  forceIpv4Mode: 'auto',
  /** 挂载时做一次 v4/v6 连接探测并写进状态页（便于判断 IPv6 是否真的有问题） */
  forceIpv4Probe: true,
})

export function normalizeConfig(raw) {
  const input = raw && typeof raw === 'object' ? raw : {}
  const pickString = (value, fallback) => (typeof value === 'string' && value.length > 0 ? value : fallback)
  const pickBool = (value, fallback) => (typeof value === 'boolean' ? value : fallback)
  const pickNumber = (value, fallback, min) => {
    const n = Number(value)
    return Number.isFinite(n) && n >= min ? n : fallback
  }
  const pickList = (value, fallback) => {
    if (!Array.isArray(value)) return fallback
    const out = value.filter((item) => typeof item === 'string' && item.trim().length > 0).map((item) => item.trim())
    return out.length > 0 ? out : fallback
  }
  return {
    pace: pickBool(input.pace, DEFAULT_CONFIG.pace),
    rpm: Math.max(1, Math.floor(pickNumber(input.rpm, DEFAULT_CONFIG.rpm, 1))),
    windowMs: Math.max(1, Math.floor(pickNumber(input.windowMs, DEFAULT_CONFIG.windowMs, 1))),
    providers: pickList(input.providers, DEFAULT_CONFIG.providers),
    hosts: pickList(input.hosts, DEFAULT_CONFIG.hosts),
    sanitize: pickBool(input.sanitize, DEFAULT_CONFIG.sanitize),
    notice: pickString(input.notice, DEFAULT_CONFIG.notice),
    reminderKeys: pickList(input.reminderKeys, DEFAULT_CONFIG.reminderKeys),
    weakReminderKeys: pickList(input.weakReminderKeys, DEFAULT_CONFIG.weakReminderKeys),
    weakKeysRequireCorroboration: pickBool(input.weakKeysRequireCorroboration, DEFAULT_CONFIG.weakKeysRequireCorroboration),
    maxNoticeChars: Math.max(32, Math.floor(pickNumber(input.maxNoticeChars, DEFAULT_CONFIG.maxNoticeChars, 32))),
    offCampusCode: pickString(input.offCampusCode, DEFAULT_CONFIG.offCampusCode),
    maxWaiters: Math.max(1, Math.floor(pickNumber(input.maxWaiters, DEFAULT_CONFIG.maxWaiters, 1))),
    emptyStreamRetries: Math.max(0, Math.floor(pickNumber(input.emptyStreamRetries, DEFAULT_CONFIG.emptyStreamRetries, 0))),
    emptyStreamBackoffMs: Math.max(0, Math.floor(pickNumber(input.emptyStreamBackoffMs, DEFAULT_CONFIG.emptyStreamBackoffMs, 0))),
    emptyStreamNotice: pickString(input.emptyStreamNotice, DEFAULT_CONFIG.emptyStreamNotice),
    emptyStreamAsRateLimit: pickBool(input.emptyStreamAsRateLimit, DEFAULT_CONFIG.emptyStreamAsRateLimit),
    capacityHintTtlMs: Math.max(0, Math.floor(pickNumber(input.capacityHintTtlMs, DEFAULT_CONFIG.capacityHintTtlMs, 0))),
    honorRetryAfterSeconds: pickBool(input.honorRetryAfterSeconds, DEFAULT_CONFIG.honorRetryAfterSeconds),
    precheck: pickBool(input.precheck, DEFAULT_CONFIG.precheck),
    precheckTtlMs: Math.max(1, Math.floor(pickNumber(input.precheckTtlMs, DEFAULT_CONFIG.precheckTtlMs, 1))),
    forceIpv4: pickBool(input.forceIpv4, DEFAULT_CONFIG.forceIpv4),
    forceIpv4Hosts: pickList(input.forceIpv4Hosts, DEFAULT_CONFIG.forceIpv4Hosts),
    forceIpv4Mode: pickString(input.forceIpv4Mode, DEFAULT_CONFIG.forceIpv4Mode),
    forceIpv4Probe: pickBool(input.forceIpv4Probe, DEFAULT_CONFIG.forceIpv4Probe),
  }
}

//#endregion
//#region ------------------------------------------------------------------ 净化规则（clean_zxh.py 的纯 JS 移植）

const RE = {
  // 连续 ≥24 位 base64 字符，两端不能再接 base64 字符
  base64: /(?<![A-Za-z0-9+/])[A-Za-z0-9+/]{24,}={0,2}(?![A-Za-z0-9+/=])/g,
  dataUri: /data:[\w.+-]+\/[\w.+-]+;base64,[A-Za-z0-9+/=\s]*/gi,
  script: /<(script|style)\b[^>]*>[\s\S]*?<\/\1\s*>/gi,
  comment: /<!--[\s\S]*?-->/g,
  tag: /<[^<>]{0,20000}?>/g,
  inlineTag: /<\/?(?:a|abbr|b|bdi|bdo|big|cite|code|del|em|font|i|ins|kbd|label|mark|q|s|samp|small|span|strong|sub|sup|time|tt|u|var|wbr)\b[^<>]{0,2000}?>/gi,
  danglingTag: /<[/!?A-Za-z][^<>\n]*$/,
  trailingJunk: /["'/>=`\\]+$/,
  cjk: /[\u3400-\u9fff\uf900-\ufaff]/,
  pureSymbols: /^[\W_]+$/,
  tokenOnly: /^[A-Za-z0-9+/=]{24,}$/,
  htmlTag: /<[a-zA-Z][^>]*>|<\/?[a-zA-Z][^<>]*/,
  pngMagic: 'iVBORw0KGgo',
}

const INLINE_TAG_NAMES = 'a|abbr|b|bdi|bdo|big|cite|code|del|em|font|i|ins|kbd|label|mark|q|s|samp|small|span|strong|sub|sup|time|tt|u|var|wbr'
void INLINE_TAG_NAMES

const NAMED_ENTITIES = {
  amp: '&', lt: '<', gt: '>', quot: '"', apos: "'", nbsp: ' ', ensp: ' ', emsp: ' ', thinsp: ' ',
  copy: '©', reg: '®', trade: '™', deg: '°', plusmn: '±', times: '×', divide: '÷', frac12: '½',
  frac14: '¼', frac34: '¾', sup2: '²', sup3: '³', micro: 'µ', middot: '·', bull: '•', hellip: '…',
  mdash: '—', ndash: '–', lsquo: '‘', rsquo: '’', ldquo: '“', rdquo: '”', laquo: '«', raquo: '»',
  larr: '←', rarr: '→', uarr: '↑', darr: '↓', ne: '≠', le: '≤', ge: '≥', infin: '∞', minus: '−',
  sect: '§', para: '¶', dagger: '†', Dagger: '‡', prime: '′', Prime: '″', permil: '‰', euro: '€',
  pound: '£', yen: '¥', cent: '¢', iexcl: '¡', iquest: '¿', szlig: 'ß', agrave: 'à', eacute: 'é',
  uuml: 'ü', ouml: 'ö', auml: 'ä', ccedil: 'ç', ntilde: 'ñ', shy: '', zwj: '', zwnj: '',
}

/** 解码 HTML 实体（数字 + 常用命名；python html.unescape 的常用子集）。 */
export function decodeEntities(text) {
  if (typeof text !== 'string' || text.indexOf('&') < 0) return text
  return text.replace(/&(#x?[0-9a-fA-F]+|[a-zA-Z][a-zA-Z0-9]{1,31});/g, (match, body) => {
    if (body[0] === '#') {
      const hex = body[1] === 'x' || body[1] === 'X'
      const code = parseInt(hex ? body.slice(2) : body.slice(1), hex ? 16 : 10)
      if (!Number.isFinite(code) || code < 0 || code > 0x10ffff) return match
      try { return String.fromCodePoint(code) } catch { return match }
    }
    return Object.prototype.hasOwnProperty.call(NAMED_ENTITIES, body) ? NAMED_ENTITIES[body] : match
  })
}

/** base64 块能还原成 HTML 或含中文的文字时返回它，否则 null（对应 python 的 _try_base64_text）。 */
export function tryBase64Text(blob) {
  try {
    if (typeof blob !== 'string' || blob.length === 0) return null
    if (!/^[A-Za-z0-9+/]*={0,2}$/.test(blob)) return null
    if (blob.length % 4 === 1) return null
    const buf = Buffer.from(blob, 'base64')
    if (buf.length === 0) return null
    for (const encoding of ['utf-8', 'gb18030', 'utf-16']) {
      let text
      try { text = new TextDecoder(encoding, { fatal: true }).decode(buf) } catch { continue }
      if (!text) continue
      if (text.includes('<') && text.includes('>')) return text
      if (RE.cjk.test(text)) return text
      return null
    }
    return null
  } catch {
    return null
  }
}

/** 整段是 JSON（例如 {"msg": "..."}）时把里面的字符串抽出来。 */
export function maybeJsonText(text) {
  const trimmed = typeof text === 'string' ? text.trim() : ''
  if (!trimmed || (trimmed[0] !== '{' && trimmed[0] !== '[')) return text
  let data
  try { data = JSON.parse(trimmed) } catch { return text }
  const parts = []
  const walk = (node) => {
    if (typeof node === 'string') parts.push(node)
    else if (Array.isArray(node)) for (const item of node) walk(item)
    else if (node && typeof node === 'object') for (const value of Object.values(node)) walk(value)
  }
  walk(data)
  return parts.length > 0 ? parts.join('\n') : text
}

/** 单行判定：不含中文、且像令牌/超长无空格串/纯符号 → 垃圾（对应 _looks_like_garbage）。 */
export function looksLikeGarbage(line) {
  if (RE.cjk.test(line)) return false
  const core = line.replace(/\s+/g, '')
  if (core.length >= 200) return true
  if (core.length >= 24 && RE.tokenOnly.test(core)) return true
  if (RE.pureSymbols.test(core)) return true
  return false
}

/** 一轮数据/标签清理，返回 { text, base64, tags }。 */
export function stripOnce(text, keepTokens = false) {
  let value = text
  let base64 = 0
  let tags = 0

  value = value.replace(RE.dataUri, (match) => { base64 += 1; return ' ' })
  value = value.replace(RE.comment, ' ')
  value = value.replace(RE.script, ' ')

  if (!keepTokens) {
    value = value.replace(RE.base64, (match) => {
      base64 += 1
      const decoded = tryBase64Text(match)
      return decoded ? ` ${decoded} ` : ' '
    })
  }

  value = value.replace(RE.inlineTag, (match) => { tags += 1; return '' })
  value = value.replace(RE.tag, (match) => { tags += 1; return '\n' })
  return { text: value, base64, tags }
}

/**
 * 完整清理：与 python clean_zxh.py 的 clean() 等价。
 * @returns {{ text: string, base64: number, tags: number }}
 */
export function cleanGarbled(text, options = {}) {
  const keepTokens = options.keepTokens === true
  let value = typeof text === 'string' ? text : ''
  value = value.replace(/\r\n/g, '\n').replace(/\r/g, '\n')
  value = maybeJsonText(value)

  let base64 = 0
  let tags = 0
  for (let round = 0; round < 3; round += 1) {
    const next = stripOnce(value, keepTokens)
    base64 += next.base64
    tags += next.tags
    const changed = next.text !== value
    value = next.text
    if (!changed) break
  }

  value = decodeEntities(value)
  value = value.replace(/\xa0/g, ' ').replace(/\u200b/g, '').replace(/\ufeff/g, '').replace(/\u3000/g, ' ')

  const lines = []
  for (const rawLine of value.split('\n')) {
    let line = rawLine.replace(/[ \t]+/g, ' ').trim()
    if (!line) continue
    if (RE.danglingTag.test(line)) {
      tags += 1
      line = line.replace(RE.danglingTag, '').trim().replace(RE.trailingJunk, '').trim()
    }
    if (!line) continue
    if (!keepTokens && looksLikeGarbage(line)) continue
    lines.push(line)
  }
  return { text: lines.join('\n'), base64, tags }
}

/** 抽出“仅限校内访问”那一句提示（对应 python 的 pick_reminder）。 */
export function pickReminder(cleaned, config = DEFAULT_CONFIG) {
  const keys = [...config.reminderKeys, ...config.weakReminderKeys]
  if (typeof cleaned !== 'string' || cleaned.length === 0) return ''
  return cleaned.split('\n').filter((line) => keys.some((key) => line.includes(key))).join('\n')
}

/** 页面特征：data URI / PNG 魔数 / HTML 标签 / base64 块组合。 */
export function hasPageShape(text) {
  if (typeof text !== 'string' || text.length < 8) return false
  if (/data:image\/[\w.+-]+\/[\w.+-]+;base64,/i.test(text)) return true
  if (text.includes(RE.pngMagic)) return true
  const html = RE.htmlTag.test(text)
  RE.base64.lastIndex = 0
  const b64 = RE.base64.test(text)
  RE.base64.lastIndex = 0
  return html && b64
}

/** 判定这段文本是不是“校外乱码”（对应需求里的签名规则）。 */
export function looksOffCampus(text, config = DEFAULT_CONFIG) {
  if (typeof text !== 'string' || text.length === 0) return false
  if (/data:image\/[\w.+-]+\/[\w.+-]+;base64,/i.test(text)) return true
  if (text.includes(RE.pngMagic)) return true
  const strong = config.reminderKeys.some((key) => text.includes(key))
  if (strong) return true
  const weak = config.weakReminderKeys.length > 0 && config.weakReminderKeys.some((key) => text.includes(key))
  if (weak && !config.weakKeysRequireCorroboration) return true
  const page = hasPageShape(text)
  if (weak && page) return true
  return page
}

/** 组装对外短提示：默认文案 + 能提取到的“仅限校内访问”整句，并截到上限。 */
export function composeNotice(rawText, config = DEFAULT_CONFIG) {
  const cleaned = cleanGarbled(rawText, { keepTokens: false }).text
  const sentence = pickReminder(cleaned, config)
  let notice = config.notice
  if (sentence && !notice.includes(sentence)) notice = `${notice}\n${sentence}`
  if (notice.length > config.maxNoticeChars) notice = notice.slice(0, config.maxNoticeChars)
  return notice
}

/**
 * 空白流签名：网关 200 + HTML 时 pi-ai 的终止错误（34 字节、不带任何乱码签名）。
 * 真机扫描到 60 次，是最容易被用户看见却最看不懂的那类失败。
 */
export function looksEmptyStream(message) {
  if (typeof message !== 'string' || message.length === 0) return false
  return /stream ended (?:before|without)\b/i.test(message)
    || /ended pending/i.test(message)
    || /^pi-ai stream error$/i.test(message)
}

/** 从失败消息里解析服务端给出的 retry_after_seconds / Retry-After（毫秒，封顶 1 小时）。 */
export function parseRetryAfterMs(message) {
  if (typeof message !== 'string' || message.length === 0) return undefined
  const patterns = [
    /"retry_after_seconds"\s*:\s*(\d+(?:\.\d+)?)/i,
    /"retry_after"\s*:\s*(\d+(?:\.\d+)?)/i,
    /\bretry[-_ ]?after(?:-ms)?\b["'\s:=]{0,6}(\d+)/i,
  ]
  for (const pattern of patterns) {
    const match = pattern.exec(message)
    if (!match) continue
    const seconds = Number(match[1])
    if (!Number.isFinite(seconds) || seconds <= 0) continue
    return Math.min(Math.round(seconds * 1000), 3_600_000)
  }
  return undefined
}

//#endregion
//#region ------------------------------------------------------------------ 滑动窗口节流器

/**
 * 创建节流器。时钟与 sleep 可注入，便于假时钟单测。
 * @param options {{ rpm, windowMs, maxWaiters, now?, sleep? }}
 * @returns {{ acquire(signal?): Promise<boolean>, admitted(): number, waiting(): number, reset(): void }}
 */
export function createPacer(options = {}) {
  const rpm = Math.max(1, Math.floor(Number(options.rpm) || 1))
  const windowMs = Math.max(1, Math.floor(Number(options.windowMs) || 1))
  const maxWaiters = Math.max(1, Math.floor(Number(options.maxWaiters) || 1))
  const now = typeof options.now === 'function' ? options.now : () => Date.now()
  const sleep = typeof options.sleep === 'function' ? options.sleep : (ms) => new Promise((resolve) => setTimeout(resolve, ms))
  const stamps = []
  let waiting = 0

  function trim(nowMs) {
    while (stamps.length > 0 && nowMs - stamps[0] >= windowMs) stamps.shift()
  }

  return {
    async acquire(signal) {
      if (waiting >= maxWaiters) return true // 队列打满时直接放行，宁可少节流也不能卡死会话
      waiting += 1
      try {
        for (;;) {
          if (signal?.aborted) return false
          const nowMs = now()
          trim(nowMs)
          if (stamps.length < rpm) {
            stamps.push(nowMs)
            return true
          }
          const waitMs = Math.max(1, windowMs - (nowMs - stamps[0]))
          const slept = await sleep(waitMs, signal)
          if (slept === false || signal?.aborted) return false
        }
      } finally {
        waiting -= 1
      }
    },
    admitted: () => stamps.length,
    waiting: () => waiting,
    reset: () => { stamps.length = 0 },
  }
}

//#endregion
//#region ------------------------------------------------------------------ 强制 IPv4（只对指定主机）

/**
 * 背景（实测）：token.nuaa.edu.cn 同时有 A 与 AAAA 记录
 *   A    10.0.241.133（校内解析）/ 218.94.136.180
 *   AAAA 2001:da8:1006:1001::101（CERNET IPv6）
 * Node 20+ 默认 `autoSelectFamily`（Happy Eyeballs）会并发尝试 IPv6，校园网 IPv6 路径
 * 不稳时表现为"流被中断 / Stream ended without finish_reason"。
 *
 * 实现与宿主自带的 dsh-http-proxy 同源：它替换 undici 全局派发器，而 Node 内置 fetch
 * 读取同一个全局符号，所以进程内所有 `fetch()` 都被覆盖。这里包一层派发器，
 * **只**把目标主机换成 connect.family=4 的连接池，其余主机原样转发给原派发器
 * （从而不影响代理、也不影响其它 provider）。
 */

const IPV4_STATE = Symbol.for('dsh-zhixiaohang-guard.ipv4-dispatcher')
const PROXY_ENV = ['https_proxy', 'HTTPS_PROXY', 'http_proxy', 'HTTP_PROXY', 'all_proxy', 'ALL_PROXY']
const NO_PROXY_ENV = ['no_proxy', 'NO_PROXY']

/** 从 origin / URL 对象取主机名。 */
export function hostnameOf(origin) {
  try {
    if (typeof origin === 'string') return new URL(origin).hostname.toLowerCase()
    if (origin && typeof origin === 'object' && typeof origin.hostname === 'string') return origin.hostname.toLowerCase()
  } catch { /* 非法 URL 交给原派发器 */ }
  return ''
}

/** 该主机是否应走代理（读 undici 同名环境变量规则）。返回代理 URL 或空串。 */
export function proxyForHost(host, env = process.env) {
  try {
    const noProxy = NO_PROXY_ENV.map((name) => env[name]).find((value) => typeof value === 'string' && value.trim().length > 0)
    if (noProxy) {
      const hit = noProxy.split(',').map((item) => item.trim().toLowerCase()).filter(Boolean)
        .some((item) => item === '*' || host === item || host.endsWith(item.startsWith('.') ? item : `.${item}`))
      if (hit) return ''
    }
    for (const name of PROXY_ENV) {
      const value = env[name]
      if (typeof value === 'string' && value.trim().length > 0) return value.trim()
    }
  } catch { /* 环境变量异常按无代理处理 */ }
  return ''
}

/** 解析 undici：先按普通说明符，再按 profile 目录解析（插件目录里没有 node_modules）。 */
export async function loadUndici(profileDir) {
  try { return await import('undici') } catch { /* 继续尝试 profile 目录 */ }
  const dirs = [profileDir, process.env.DSH_PROFILE_DIR, join(homedir(), '.dsh', 'profiles', 'desktop')].filter(Boolean)
  for (const dir of dirs) {
    try {
      const resolved = createRequire(join(dir, 'package.json')).resolve('undici')
      return await import(pathToFileURL(resolved).href)
    } catch { /* 试下一个目录 */ }
  }
  return null
}

/** 解析该主机的 A / AAAA 记录，以及 getaddrinfo（fetch 真正走的那条）结果。 */
export async function resolveHostRecords(host) {
  const safe = async (fn) => { try { return await fn() } catch { return [] } }
  const [a, aaaa, lookup] = await Promise.all([
    safe(() => dns.promises.resolve4(host)),
    safe(() => dns.promises.resolve6(host)),
    safe(() => dns.promises.lookup(host, { all: true })),
  ])
  return { a, aaaa, lookup }
}

/** 分别用 IPv4 / IPv6 连一次 443，看哪条路径真的通（用于判断该不该强制）。 */
export async function probeConnectFamilies(host, port = 443, timeoutMs = 4000) {
  const attempt = (family) => new Promise((resolve) => {
    let settled = false
    let socket
    const finish = (result) => {
      if (settled) return
      settled = true
      try { socket?.destroy() } catch {}
      resolve({ family, ...result })
    }
    try {
      socket = net.connect({ host, port, family })
    } catch (error) {
      finish({ ok: false, reason: String(error?.code ?? error?.message ?? error) })
      return
    }
    socket.setTimeout(timeoutMs, () => finish({ ok: false, reason: 'timeout' }))
    socket.once('connect', () => finish({ ok: true, remoteAddress: socket.remoteAddress, remoteFamily: socket.remoteFamily }))
    socket.once('error', (error) => finish({ ok: false, reason: String(error?.code ?? error?.message ?? error) }))
  })
  const [v4, v6] = await Promise.all([attempt(4), attempt(6)])
  return { v4, v6 }
}

/**
 * 安装"只对指定主机强制 IPv4"的 undici 全局派发器；失败时按模式退回 DNS 层。
 * @returns {Promise<{mode:string, installed:boolean, previous:string, error:string, dispose?:Function}>}
 */
export async function installHostScopedIpv4(options = {}) {
  const hosts = (options.hosts ?? []).map((host) => String(host).toLowerCase()).filter(Boolean)
  const mode = options.mode ?? 'auto'
  const log = (level, message) => { try { options.logger?.[level]?.(message) } catch {} }
  const result = { mode: 'off', installed: false, previous: '', error: '', skippedByProxy: [] }
  if (hosts.length === 0 || mode === 'off') return result
  const hostSet = new Set(hosts)

  // 先拆掉本插件上一次装的派发器，避免 HMR 重复套娃
  try {
    const existing = globalThis[IPV4_STATE]
    if (existing?.dispose) existing.dispose()
    delete globalThis[IPV4_STATE]
  } catch { /* 忽略 */ }

  if (mode === 'dispatcher' || mode === 'auto') {
    const undici = await loadUndici(options.profileDir)
    if (undici?.Dispatcher && undici?.setGlobalDispatcher && undici?.getGlobalDispatcher) {
      try {
        const { Dispatcher, Pool, ProxyAgent, getGlobalDispatcher, setGlobalDispatcher } = undici
        const previous = getGlobalDispatcher()
        result.previous = previous?.constructor?.name ?? String(previous)
        const pools = new Map()
        const dispatcherFor = (origin, host) => {
          let pool = pools.get(host)
          if (pool) return pool
          const proxy = proxyForHost(host, options.env ?? process.env)
          if (proxy && typeof ProxyAgent === 'function') {
            // 有代理时保留代理链路（并让到代理的连接也走 IPv4）
            try {
              pool = new ProxyAgent({ uri: proxy, connect: { family: 4 } })
            } catch (error) {
              result.skippedByProxy.push(`${host}(${String(error?.message ?? error)})`)
              pool = previous
            }
          } else {
            pool = new Pool(origin, { connect: { family: 4 }, connections: 8 })
          }
          pools.set(host, pool)
          return pool
        }
        class HostScopedIpv4Dispatcher extends Dispatcher {
          dispatch(params, handler) {
            try {
              const host = hostnameOf(params?.origin)
              if (host && hostSet.has(host)) return dispatcherFor(params.origin, host).dispatch(params, handler)
            } catch (error) {
              log('warn', `zhixiaohang-guard: IPv4 派发回落（${String(error?.message ?? error)}）`)
            }
            return previous.dispatch(params, handler)
          }
          close() {
            for (const pool of pools.values()) { try { pool.close?.() } catch {} }
            pools.clear()
            return previous.close?.() ?? Promise.resolve()
          }
          destroy(error, callback) {
            for (const pool of pools.values()) { try { pool.destroy?.(error) } catch {} }
            pools.clear()
            return previous.destroy?.(error, callback) ?? Promise.resolve()
          }
        }
        setGlobalDispatcher(new HostScopedIpv4Dispatcher())
        result.mode = 'dispatcher'
        result.installed = true
        result.dispose = () => {
          try { setGlobalDispatcher(previous) } catch { /* 恢复失败也不抛 */ }
          for (const pool of pools.values()) { try { pool.destroy?.() } catch {} }
          pools.clear()
        }
        try { globalThis[IPV4_STATE] = result } catch { /* 忽略 */ }
        return result
      } catch (error) {
        result.error = String(error?.message ?? error)
        log('warn', `zhixiaohang-guard: 安装 IPv4 派发器失败，改用 DNS 兜底：${result.error}`)
      }
    } else {
      result.error = 'undici 不可用（Dispatcher/setGlobalDispatcher 缺失）'
    }
  }

  if (mode === 'dns' || mode === 'auto') {
    try {
      dns.setDefaultResultOrder('ipv4first')
      if (typeof net.setDefaultAutoSelectFamily === 'function') net.setDefaultAutoSelectFamily(false)
      result.mode = result.mode === 'off' ? 'dns' : `${result.mode}+dns`
      result.installed = true
      result.dispose = () => {
        try { dns.setDefaultResultOrder('verbatim') } catch { /* 忽略 */ }
        try { net.setDefaultAutoSelectFamily?.(true) } catch { /* 忽略 */ }
      }
    } catch (error) {
      result.error = `${result.error}${result.error ? '; ' : ''}${String(error?.message ?? error)}`
      log('warn', `zhixiaohang-guard: IPv4 兜底也失败：${result.error}`)
    }
  }
  return result
}

//#endregion
//#region ------------------------------------------------------------------ 宿主插件

function abortFinishChunk() {
  return {
    type: 'finish',
    reason: { kind: 'aborted', failure: { message: 'zhixiaohang-guard: request aborted while pacing', code: 'ABORTED' } },
  }
}

function isRecord(value) {
  return typeof value === 'object' && value !== null
}

/** 从各种可能的位置取 provider id / baseURL，用于节流匹配。 */
function readRoute(options) {
  const provider = []
  const urls = []
  const pushString = (value, bucket) => { if (typeof value === 'string' && value.length > 0) bucket.push(value) }
  if (isRecord(options)) {
    pushString(options.provider, provider)
    pushString(options.providerId, provider)
    pushString(options.baseURL, urls)
    pushString(options.baseUrl, urls)
    pushString(options.url, urls)
    if (isRecord(options.model)) pushString(options.model.provider, provider)
    if (isRecord(options.config)) {
      pushString(options.config.provider, provider)
      pushString(options.config.baseURL, urls)
      pushString(options.config.baseUrl, urls)
    }
  }
  return { provider, urls }
}

function hostOf(value) {
  try { return new URL(value).hostname.toLowerCase() } catch { return '' }
}

/**
 * 宿主入口。
 * @param ctx 插件上下文（on / logger / timeout / effect / inject）
 * @param rawConfig 来自 cordis.patch.yml 的 config
 */
export function apply(ctx, rawConfig = {}) {
  const config = normalizeConfig(rawConfig)
  const logger = ctx?.logger ?? console
  const warn = (message) => { try { logger.warn?.(message) } catch {} }
  const info = (message) => { try { logger.info?.(message) } catch {} }

  /** providerId → baseURL，best-effort（拿不到就只按 provider id 匹配） */
  const providerHosts = new Map()
  const stats = { paced: 0, waited: 0, sanitized: 0, claimed: 0, precheckHits: 0, emptyStreams: 0, emptyStreamRetries: 0, emptyStreamAsRateLimit: 0, retryAfterEnriched: 0, errors: 0 }
  const sanitizedFailures = new WeakSet()
  let offCampusUntil = 0
  let streamRegistered = false
  let errorRegistered = false
  /** 最近一次从 429 失败消息里解析到的服务端冷却提示（用于把"空流"识别为容量不足） */
  let lastCapacityHint = null
  /** 强制 IPv4 的现场信息（挂载后异步填充，写进状态页） */
  const ipv4Info = {
    enabled: null, mode: 'pending', installed: false, hosts: [],
    previousDispatcher: '', error: '', skippedByProxy: [],
    dnsRecords: null, connectProbe: null, probedAt: null,
  }
  let disposeIpv4 = null

  // ---- 可观测性：挂载自检 + 状态落盘 + 只读状态路由（宿主日志不落盘，只能自带信号）----
  const statusDir = typeof rawConfig?.dataDir === 'string' && rawConfig.dataDir.trim().length > 0
    ? rawConfig.dataDir
    : join(process.env.DSH_HOME || join(homedir(), '.dsh'), 'zhixiaohang-guard')
  const statusFile = join(statusDir, 'status.json')
  const mountedAt = new Date().toISOString()
  const recentEvents = []
  let routeMounted = false
  let routeDisposer = null

  // 自检：拿一段"校外乱码"样本走一遍真实净化路径（不依赖任何外部文件）
  const selfTest = (() => {
    try {
      const sample = '500: <!doctype html><html><body><img src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAAB'
        + 'AAAABHNCSVQICAgIfAhkiAAAAAlwSFlzAAALEgAACxIB0t1+/AAAADh0RVh0U29mdHdhcmUAbWF0cGxvdGxpYiB2ZXJzaW9u'.repeat(3)
        + '" /><p>当前系统仅限校内访问，校外请通过登录校综合服务门户或VPN系统进行访问。</p>'
        + '<p>服务热线：(025)84890123</p></body></html>'
      const detected = looksOffCampus(sample, config)
      const notice = composeNotice(sample, config)
      const cleaned = cleanGarbled(sample).text
      const leaked = /iVBORw0KGgo|data:image\/[\w.+-]+\/[\w.+-]+;base64,/.test(notice)
        || /(?<![A-Za-z0-9+/])[A-Za-z0-9+/]{24,}={0,2}(?![A-Za-z0-9+/=])/.test(notice)
        || /<[a-zA-Z][^>]*>/.test(notice)
      return {
        ok: detected === true && leaked === false && notice.length <= config.maxNoticeChars,
        detectedOffCampus: detected,
        sampleBytes: sample.length,
        noticeChars: notice.length,
        noticeLeakFree: leaked === false,
        reminderExtracted: cleaned.includes('仅限校内访问'),
        noticePreview: notice.split('\n')[0],
      }
    } catch (error) {
      return { ok: false, error: String(error?.message ?? error) }
    }
  })()

  const snapshot = () => ({
    plugin: name,
    version: VERSION,
    status: 'mounted',
    mountedAt,
    pid: process.pid,
    module: import.meta.url,
    profileDir: process.env.DSH_PROFILE_DIR ?? null,
    statusFile,
    config,
    listeners: { 'llm/stream': streamRegistered, 'agent/request-error': errorRegistered },
    webRoute: routeMounted ? '/zhixiaohang-guard' : null,
    selfTest,
    ipv4: ipv4Info,
    stats: { ...stats, admittedInWindow: pacer.admitted(), waiting: pacer.waiting(), offCampusUntil },
    lastEvents: recentEvents,
  })

  const writeStatus = () => {
    try {
      mkdirSync(statusDir, { recursive: true })
      writeFileSync(statusFile, JSON.stringify(snapshot(), null, 2))
    } catch { /* 状态文件写不了也绝不影响主流程 */ }
  }

  const record = (event) => {
    try {
      recentEvents.unshift({ at: new Date().toISOString(), ...event })
      if (recentEvents.length > 20) recentEvents.length = 20
      writeStatus()
    } catch { /* 同上 */ }
  }

  const sleep = (ms, signal) => new Promise((resolve) => {
    let settled = false
    let cancelTimer = null
    const finish = (value) => {
      if (settled) return
      settled = true
      if (cancelTimer !== null) { try { cancelTimer() } catch {} }
      try { signal?.removeEventListener('abort', onAbort) } catch {}
      resolve(value)
    }
    const onAbort = () => finish(false)
    try {
      if (ctx?.timeout) cancelTimer = ctx.timeout(() => finish(true), ms)
      else {
        const handle = setTimeout(() => finish(true), ms)
        cancelTimer = () => clearTimeout(handle)
      }
    } catch {
      const handle = setTimeout(() => finish(true), ms)
      cancelTimer = () => clearTimeout(handle)
    }
    try { signal?.addEventListener('abort', onAbort, { once: true }) } catch {}
    if (signal?.aborted) finish(false)
  })

  const pacer = createPacer({ rpm: config.rpm, windowMs: config.windowMs, maxWaiters: config.maxWaiters, sleep })

  const isTargetRoute = (options) => {
    const { provider, urls } = readRoute(options)
    for (const id of provider) {
      if (config.providers.some((wanted) => wanted.toLowerCase() === id.toLowerCase())) return true
      const host = providerHosts.get(id)
      if (host && config.hosts.some((wanted) => wanted.toLowerCase() === host.toLowerCase())) return true
    }
    for (const url of urls) {
      const host = hostOf(url)
      if (host && config.hosts.some((wanted) => wanted.toLowerCase() === host.toLowerCase())) return true
    }
    return false
  }

  // 可选：从 llm 服务读 provider → baseURL，供 hosts 兜底匹配（拿不到就走 provider id）
  if (config.hosts.length > 0) {
    try {
      ctx.inject?.(['llm'], (scope) => {
        try {
          const list = typeof scope?.llm?.listProviders === 'function' ? scope.llm.listProviders() : []
          for (const entry of Array.isArray(list) ? list : []) {
            const id = entry?.id ?? entry?.provider
            const url = entry?.baseURL ?? entry?.baseUrl
            if (typeof id === 'string' && typeof url === 'string') {
              const host = hostOf(url)
              if (host) providerHosts.set(id, host)
            }
          }
          if (providerHosts.size > 0) info(`zhixiaohang-guard: 已解析 ${providerHosts.size} 个 provider 主机名用于兜底匹配`)
        } catch (error) {
          warn(`zhixiaohang-guard: 读取 provider 目录失败，退回按 provider id 匹配：${String(error?.message ?? error)}`)
        }
      })
    } catch (error) {
      warn(`zhixiaohang-guard: 注入 llm 服务失败（忽略）：${String(error?.message ?? error)}`)
    }
  }

  /**
   * 就地净化单个分片；任何异常都返回原分片。
   * @param chunk 分片
   * @param isTarget 是否属于被守护的 provider（非目标 provider 一律原样透传，免得影响别人）
   */
  const sanitizeChunk = (chunk, isTarget) => {
    try {
      if (!isRecord(chunk) || isTarget !== true) return chunk
      if (chunk.type !== 'finish') {
        if (!config.sanitize) return chunk
        if (chunk.type === 'text-delta' || chunk.type === 'reasoning-delta') {
          const text = chunk.text
          if (typeof text !== 'string' || text.length === 0) return chunk
          if (!looksOffCampus(text, config)) return chunk
          const notice = composeNotice(text, config)
          stats.sanitized += 1
          warn(`zhixiaohang-guard: assistant 正文命中校外乱码签名，已替换为短提示（${text.length} 字符 → ${notice.length} 字符）`)
          return { ...chunk, text: notice }
        }
        return chunk
      }

      const reason = chunk.reason
      if (!isRecord(reason) || !isRecord(reason.failure)) return chunk
      const message = reason.failure.message
      if (typeof message !== 'string' || message.length === 0) return chunk

      // 1) 校外乱码 → 短提示。这条必须最先做：真机里原文最长 316,850 字节。
      if (config.sanitize && looksOffCampus(message, config)) {
        const notice = composeNotice(message, config)
        const failure = Object.freeze({
          message: notice,
          code: config.offCampusCode,
          ...(Number.isInteger(reason.failure.status) && reason.failure.status >= 100 && reason.failure.status <= 599
            ? { status: reason.failure.status }
            : {}),
        })
        sanitizedFailures.add(failure)
        sanitizedFailures.add(reason.failure)
        stats.sanitized += 1
        if (config.precheck) offCampusUntil = Date.now() + config.precheckTtlMs
        info(`zhixiaohang-guard: 校外乱码已在入路处屏蔽（failure.message ${message.length} 字符 → 提示 ${notice.length} 字符，code ${reason.failure.code ?? 'UNKNOWN'} → ${config.offCampusCode}）`)
        record({ type: 'sanitized-off-campus', fromCode: reason.failure.code ?? 'UNKNOWN', fromBytes: message.length, toChars: notice.length })
        return { ...chunk, reason: { ...reason, failure } }
      }

      // 2) 补上宿主漏掉的 providerRetryAfterMs（只补字段，不改文案）
      if (config.honorRetryAfterSeconds && reason.failure.providerRetryAfterMs === undefined) {
        const hintMs = parseRetryAfterMs(message)
        if (hintMs !== undefined) {
          stats.retryAfterEnriched += 1
          lastCapacityHint = { ms: hintMs, at: Date.now() }
          info(`zhixiaohang-guard: 从失败消息解析到 retry_after=${hintMs}ms，补进 providerRetryAfterMs（宿主原本未填）`)
          record({ type: 'retry-after-enriched', retryAfterMs: hintMs })
          return {
            ...chunk,
            reason: { ...reason, failure: Object.freeze({ ...reason.failure, providerRetryAfterMs: hintMs }) },
          }
        }
      }
      return chunk
    } catch (error) {
      stats.errors += 1
      warn(`zhixiaohang-guard: 分片净化失败（原样放行）：${String(error?.message ?? error)}`)
      return chunk
    }
  }

  const disposeStream = ctx.on('llm/stream', (options, next) => {
    const target = (() => {
      try { return isTargetRoute(options) } catch (error) {
        stats.errors += 1
        warn(`zhixiaohang-guard: 路由匹配失败（按非目标放行）：${String(error?.message ?? error)}`)
        return false
      }
    })()

    return (async function* guarded() {
      // 可选预检：刚判定过校外 → TTL 内直接快速失败，不再白跑一次请求
      if (config.precheck && config.sanitize && target && Date.now() < offCampusUntil) {
        stats.precheckHits += 1
        info('zhixiaohang-guard: 预检命中（TTL 内已知校外），直接返回短提示，未发起请求')
        yield {
          type: 'finish',
          reason: { kind: 'error', failure: { message: config.notice.slice(0, config.maxNoticeChars), code: config.offCampusCode } },
        }
        return
      }

      if (config.pace && target) {
        stats.paced += 1
        const startedAt = Date.now()
        let admitted = true
        try {
          admitted = await pacer.acquire(options?.signal)
        } catch (error) {
          stats.errors += 1
          warn(`zhixiaohang-guard: 节流等待异常（放行）：${String(error?.message ?? error)}`)
          admitted = true
        }
        const waitedMs = Date.now() - startedAt
        if (waitedMs > 0) {
          stats.waited += 1
          info(`zhixiaohang-guard: 智小航节流生效，排队等待 ${waitedMs}ms（窗口 ${config.windowMs}ms / ${config.rpm} 次）`)
          record({ type: 'paced', waitedMs, rpm: config.rpm, windowMs: config.windowMs })
        }
        if (!admitted) {
          yield abortFinishChunk()
          return
        }
      }

      // 每一轮：跑一次 next()；只有"这一轮没吐出任何内容却以空白流失败"才会被吞掉后重发。
      const maxAttempts = 1 + (target ? config.emptyStreamRetries : 0)
      for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
        let contentSeen = false
        let retryThisRound = false
        const inner = next()
        for await (const chunk of inner) {
          if (isRecord(chunk) && chunk.type !== 'finish' && chunk.type !== 'usage' && chunk.type !== 'start') {
            contentSeen = true
          }
          if (
            target
            && !contentSeen
            && isRecord(chunk)
            && chunk.type === 'finish'
            && isRecord(chunk.reason)
            && isRecord(chunk.reason.failure)
            && looksEmptyStream(chunk.reason.failure.message)
          ) {
            stats.emptyStreams += 1
            // 真机证据：网关上游容量不足（429 circuit.upstream_capacity_exhausted）时，
            // 流式接口有时以 200 + 空流呈现。若本插件刚解析到过 retry_after，就把这次空流
            // 改写成带提示的 RATE_LIMIT 失败，交给重试插件按冷却重试（而不是丢一句死胡同）。
            const hintFresh = lastCapacityHint !== null && Date.now() - lastCapacityHint.at <= config.capacityHintTtlMs
            if (config.emptyStreamAsRateLimit && hintFresh) {
              stats.emptyStreamAsRateLimit += 1
              warn(`zhixiaohang-guard: 空白流 + 近期上游容量不足证据 → 转成 RATE_LIMIT（retry_after≈${Math.round(lastCapacityHint.ms / 1000)}s），交由重试插件按冷却重试`)
              record({ type: 'empty-stream-as-rate-limit', retryAfterMs: lastCapacityHint.ms })
              yield {
                ...chunk,
                reason: {
                  ...chunk.reason,
                  failure: Object.freeze({
                    message: `429: 网关上游容量不足（由空白流识别，服务端提示约 ${Math.round(lastCapacityHint.ms / 1000)}s）：${chunk.reason.failure.message}`,
                    code: 'RATE_LIMIT',
                    providerRetryAfterMs: lastCapacityHint.ms,
                  }),
                },
              }
              return
            }
            if (attempt < maxAttempts) {
              retryThisRound = true
              stats.emptyStreamRetries += 1
              info(`zhixiaohang-guard: 空白流（${chunk.reason.failure.message}），本轮无内容，退避 ${config.emptyStreamBackoffMs}ms 后自动重发（第 ${attempt}/${maxAttempts} 次）`)
              break
            }
            const notice = config.emptyStreamNotice.slice(0, config.maxNoticeChars)
            warn(`zhixiaohang-guard: 空白流重发后仍失败，改为短提示（code ${chunk.reason.failure.code ?? 'TRANSPORT'}）`)
            record({ type: 'empty-stream-exhausted', attempts: maxAttempts })
            yield {
              ...chunk,
              reason: {
                ...chunk.reason,
                failure: Object.freeze({ message: notice, code: chunk.reason.failure.code ?? 'TRANSPORT' }),
              },
            }
            return
          }
          yield sanitizeChunk(chunk, target)
        }
        if (!retryThisRound) return
        const slept = await sleep(config.emptyStreamBackoffMs, options?.signal)
        if (!slept) {
          yield abortFinishChunk()
          return
        }
      }
    })()
  }, { global: true, prepend: true })
  streamRegistered = true

  const disposeError = ctx.on('agent/request-error', (payload, next) => {
    try {
      const failure = payload?.failure
      if (!isRecord(failure)) return next()
      const ours = sanitizedFailures.has(failure)
        || failure.code === config.offCampusCode
        || (config.sanitize && looksOffCampus(failure.message, config))
      if (!ours) return next()
      stats.claimed += 1
      if (config.precheck) offCampusUntil = Date.now() + config.precheckTtlMs
      warn('zhixiaohang-guard: 判定为校外/未连 VPN（需要用户操作，不可重试）——本插件抢先认领，不再交给重试插件')
      record({ type: 'claimed-off-campus', code: failure.code, messageChars: typeof failure.message === 'string' ? failure.message.length : 0 })
      // 故意不调用 next()：该失败已上报一次短提示，忽略不前的 429 休眠重试。
      return undefined
    } catch (error) {
      stats.errors += 1
      warn(`zhixiaohang-guard: 认领失败判定出错（交回后续监听器）：${String(error?.message ?? error)}`)
      return next()
    }
  }, { global: true, prepend: true })
  errorRegistered = true

  // 只读状态路由（宿主主进程日志不落盘，靠它确认插件真的在跑）
  const statusPath = '/zhixiaohang-guard'
  const sendStatusJson = (req, res) => {
    try {
      const body = JSON.stringify(snapshot(), null, 2)
      res.statusCode = 200
      res.setHeader('content-type', 'application/json; charset=utf-8')
      res.setHeader('cache-control', 'no-store')
      res.end(req?.method === 'HEAD' ? undefined : body)
    } catch {
      try { res.statusCode = 500; res.setHeader('content-type', 'application/json'); res.end('{"status":"error"}') } catch {}
    }
  }
  try {
    ctx.inject?.(['webServer'], (scope) => {
      try {
        routeDisposer = scope.webServer.register({
          kind: 'prefix',
          path: statusPath,
          handler: (req, res) => {
            const pathname = String(req?.url ?? '/').split('?')[0].replace(/\/+$/, '') || '/'
            if (pathname === `${statusPath}/api/status`) { sendStatusJson(req, res); return }
            if (pathname === statusPath) {
              try {
                res.statusCode = 200
                res.setHeader('content-type', 'text/html; charset=utf-8')
                res.setHeader('cache-control', 'no-store')
                res.end('<!doctype html><meta charset="utf-8"><title>智小航守卫</title>'
                  + '<body style="font:13px/1.6 ui-monospace,Consolas,monospace;padding:16px">'
                  + '<h3>dsh-zhixiaohang-guard 状态</h3><pre id="o">加载中…</pre>'
                  + `<script>fetch('${statusPath}/api/status',{cache:'no-store'}).then(r=>r.text()).then(t=>{document.getElementById('o').textContent=t}).catch(e=>{document.getElementById('o').textContent='读取失败: '+e})</script>`)
              } catch {}
              return
            }
            sendStatusJson(req, res)
          },
        })
        routeMounted = true
        info(`zhixiaohang-guard: 状态页挂载于 ${statusPath}`)
      } catch (error) {
        warn(`zhixiaohang-guard: 状态路由注册失败（不影响节流与屏蔽）：${String(error?.message ?? error)}`)
      }
    })
  } catch (error) {
    warn(`zhixiaohang-guard: 注入 webServer 失败（忽略）：${String(error?.message ?? error)}`)
  }

  try {
    ctx.effect?.(() => () => {
      try { disposeStream?.() } catch {}
      try { disposeError?.() } catch {}
      try { routeDisposer?.() } catch {}
      try { disposeIpv4?.() } catch {}
    }, 'zhixiaohang-guard: unmount listeners')
  } catch (error) {
    warn(`zhixiaohang-guard: 注册卸载清理失败（忽略）：${String(error?.message ?? error)}`)
  }

  // ---- 强制 IPv4：包一层 undici 全局派发器，只影响 forceIpv4Hosts ----
  ipv4Info.enabled = config.forceIpv4 && config.forceIpv4Mode !== 'off'
  ipv4Info.hosts = [...config.forceIpv4Hosts]
  if (ipv4Info.enabled) {
    installHostScopedIpv4({
      hosts: config.forceIpv4Hosts,
      mode: config.forceIpv4Mode,
      profileDir: process.env.DSH_PROFILE_DIR,
      logger: { info, warn },
    }).then(async (result) => {
      ipv4Info.mode = result.mode
      ipv4Info.installed = result.installed
      ipv4Info.previousDispatcher = result.previous
      ipv4Info.error = result.error
      ipv4Info.skippedByProxy = result.skippedByProxy ?? []
      disposeIpv4 = typeof result.dispose === 'function' ? result.dispose : null
      if (result.installed) {
        info(`zhixiaohang-guard: 已对 [${config.forceIpv4Hosts.join(', ')}] 强制 IPv4（模式 ${result.mode}，原派发器 ${result.previous || 'n/a'}）`)
      } else {
        warn(`zhixiaohang-guard: 强制 IPv4 未生效（${result.error || '未知原因'}）`)
      }
      record({ type: 'ipv4-installed', mode: result.mode, hosts: ipv4Info.hosts, previous: result.previous })
      if (config.forceIpv4Probe) {
        try {
          const host = config.forceIpv4Hosts[0]
          const [records, probe] = await Promise.all([resolveHostRecords(host), probeConnectFamilies(host)])
          ipv4Info.dnsRecords = records
          ipv4Info.connectProbe = probe
          ipv4Info.probedAt = new Date().toISOString()
          info(`zhixiaohang-guard: ${host} 连接探测 IPv4=${probe.v4.ok ? probe.v4.remoteAddress : `失败(${probe.v4.reason})`} / IPv6=${probe.v6.ok ? probe.v6.remoteAddress : `失败(${probe.v6.reason})`}`)
          record({ type: 'ipv4-probe', host, v4: probe.v4.ok ? probe.v4.remoteAddress : `fail:${probe.v4.reason}`, v6: probe.v6.ok ? probe.v6.remoteAddress : `fail:${probe.v6.reason}` })
        } catch (error) {
          warn(`zhixiaohang-guard: IPv4 探测失败（忽略）：${String(error?.message ?? error)}`)
        }
      }
      writeStatus()
    }).catch((error) => {
      ipv4Info.error = String(error?.message ?? error)
      warn(`zhixiaohang-guard: 强制 IPv4 安装异常（忽略）：${ipv4Info.error}`)
    })
  }

  info(`zhixiaohang-guard: mounted（v${VERSION}，pace=${config.pace ? `${config.rpm}/${config.windowMs}ms` : 'off'}，providers=[${config.providers.join(', ')}]，hosts=[${config.hosts.join(', ')}]，sanitize=${config.sanitize}，precheck=${config.precheck}，forceIpv4=${ipv4Info.enabled ? 'on' : 'off'}，自检=${selfTest.ok ? 'OK' : 'FAILED'}，状态页=${routeMounted ? statusPath : '未挂载'}）`)
  record({ type: 'mounted', selfTestOk: selfTest.ok })
  writeStatus()

  return {
    /** 供宿主/测试读取运行统计（只读快照）。 */
    stats: () => ({ ...stats, admittedInWindow: pacer.admitted(), waiting: pacer.waiting() }),
    /** 完整状态快照（含自检、监听器注册情况、最近事件）。 */
    status: () => snapshot(),
    /** 供测试重置节流窗口。 */
    resetPacer: () => pacer.reset(),
  }
}
