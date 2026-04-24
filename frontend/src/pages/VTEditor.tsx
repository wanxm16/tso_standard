import { useEffect, useMemo, useState } from 'react'
import {
  Alert,
  Button,
  Card,
  Cascader,
  Checkbox,
  Collapse,
  Descriptions,
  Divider,
  Drawer,
  Form,
  Input,
  message,
  Modal,
  Popconfirm,
  Radio,
  Select,
  Space,
  Spin,
  Table,
  Tabs,
  Tag,
  Tooltip,
  Typography,
} from 'antd'
import { CheckOutlined, DeleteOutlined, EditOutlined, PlusOutlined, ReloadOutlined, SaveOutlined, StopOutlined, ThunderboltOutlined, UndoOutlined } from '@ant-design/icons'
import { useNavigate, useParams } from 'react-router-dom'
import { api } from '../api'
import { emitScaffoldChanged } from '../lib/events'
import { ROLE_OPTIONS, TABLE_TYPE_COLOR } from '../types'
import type { BaseSlot, NormalizationItem, Slot, SourceTableWithFields, VTDetail } from '../types'
import { MetricTooltip } from '../components/MetricTooltip'

const { Text, Title } = Typography

type DirtySlot = Slot & { _key: number }
let _keyCounter = 0
const newKey = () => ++_keyCounter

function toDirty(slots: Slot[]): DirtySlot[] {
  return slots.map((s) => ({ ...s, _key: newKey() }))
}

function stripKey(s: DirtySlot): Slot {
  const { _key, ...rest } = s
  return rest
}

export default function VTEditor({ onSaved }: { onSaved: () => void }) {
  const { vt_id } = useParams<{ vt_id: string }>()
  const navigate = useNavigate()
  const [vt, setVt] = useState<VTDetail | null>(null)
  const [slots, setSlots] = useState<DirtySlot[]>([])
  const [originalSlots, setOriginalSlots] = useState<string>('')
  const [baseSlots, setBaseSlots] = useState<BaseSlot[]>([])
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [editing, setEditing] = useState<DirtySlot | null>(null)
  const [form] = Form.useForm()
  // I-14: 元信息编辑 + 源表增删
  const [metaEditOpen, setMetaEditOpen] = useState(false)
  const [metaForm] = Form.useForm()
  const [addSourceOpen, setAddSourceOpen] = useState(false)
  const [addSourceSearch, setAddSourceSearch] = useState('')
  const [allTables, setAllTables] = useState<Array<{ en: string; cn: string; field_count: number }>>([])
  const [nextActionBanner, setNextActionBanner] = useState<string | null>(null)
  // I-15: 分类树
  const [categories, setCategories] = useState<Array<{ name: string; children: Array<{ name: string }> }>>([])
  // I-16: LLM 生成槽位
  const [generating, setGenerating] = useState(false)
  const [extending, setExtending] = useState(false)
  const [extendResult, setExtendResult] = useState<any | null>(null)
  // 黑名单扫描
  const [scanningBlacklist, setScanningBlacklist] = useState(false)
  const [blacklistScanResult, setBlacklistScanResult] = useState<Awaited<
    ReturnType<typeof api.blacklistScanForVT>
  > | null>(null)
  const [scanModalOpen, setScanModalOpen] = useState(false)
  const [scanSelectedFields, setScanSelectedFields] = useState<string[]>([])
  const [applyingBlacklist, setApplyingBlacklist] = useState(false)
  const [extendSelectedKeys, setExtendSelectedKeys] = useState<string[]>([])
  // 槽位合并 & 相似度检查
  const [mergeSource, setMergeSource] = useState<DirtySlot | null>(null)
  const [mergeTargetName, setMergeTargetName] = useState<string | undefined>()
  const [similarityOpen, setSimilarityOpen] = useState(false)
  // 归一审核 Tab
  const [normItems, setNormItems] = useState<any[]>([])
  const [normLoading, setNormLoading] = useState(false)
  const [normStatusFilter, setNormStatusFilter] = useState<string>('conflict')
  const [normOnlyUnreviewed, setNormOnlyUnreviewed] = useState<boolean>(true)
  const [normKeyword, setNormKeyword] = useState<string>('')
  // 归一审核详情抽屉
  const [reviewDrawerItem, setReviewDrawerItem] = useState<NormalizationItem | null>(null)
  // 「新增槽位」对话框 AI 生成英文名 loading 状态
  const [suggestingName, setSuggestingName] = useState(false)
  // 槽位搜索（按 name / 中文名 / aliases 过滤）
  const [slotSearch, setSlotSearch] = useState('')
  // 「按源表看字段」跨表搜索 + Collapse 展开 keys
  const [bySourceSearch, setBySourceSearch] = useState('')
  const [bySourceActiveKey, setBySourceActiveKey] = useState<string[]>([])

  const reloadNormItems = async () => {
    if (!vt_id) return
    setNormLoading(true)
    try {
      const resp = await api.normList({
        vt_id,
        status: normStatusFilter || undefined,
        only_unreviewed: normOnlyUnreviewed,
        keyword: normKeyword.trim() || undefined,
        limit: 500,
      })
      setNormItems(resp.items)
    } finally {
      setNormLoading(false)
    }
  }

  // 刷新 vt 对象 + 合并后端新出现的 slot 到前端 slots state
  // 之前实现：不动 slots 保护用户编辑，但导致 mark_new_slot 后端建好的新 slot 前端看不见，
  //   更严重的是：用户再点「保存槽位」会 PUT 旧 slots 把新建 slot 覆盖删掉
  // 新实现：合并式 —— 后端新出现（按 name 对比）的 slot 追加到前端 slots 末尾；
  //   前端已有的 slot 保留（保护编辑）；前端本地新增未保存的 slot 保留
  const reloadVT = async () => {
    if (!vt_id) return
    try {
      const fresh = await api.getVT(vt_id)
      setVt(fresh)
      const currentNames = new Set(slots.map((s) => s.name))
      const freshExtras = (fresh.slots || []).filter((s: Slot) => !currentNames.has(s.name))
      if (freshExtras.length > 0) {
        const added = toDirty(freshExtras)
        setSlots((cur) => [...cur, ...added])
        // 同步 originalSlots，避免新增的 slot 被判为 dirty 导致"保存槽位"按钮错亮
        const merged = [...slots, ...added].map(stripKey)
        setOriginalSlots(JSON.stringify(merged))
      }
    } catch {
      // 静默失败，不打断归一审核流程
    }
  }

  // 同 VT 内 (field_name, slot_key 对应的 slot) 相同 + 未审 + 非当前行的批量候选
  const findBatchCandidates = (currentRow: any, slotKey: 'top1_slot' | 'top2_slot' | 'top3_slot'): any[] => {
    const slot = currentRow?.[slotKey]
    if (!slot) return []
    return normItems.filter(
      (it) =>
        it.field_name === currentRow.field_name &&
        !it.decision &&
        it[slotKey] === slot &&
        !(it.table_en === currentRow.table_en && it.vt_id === currentRow.vt_id),
    )
  }

  // 弹 Modal 询问是否对候选批量应用同决策。返回是否真的批量执行了
  const askBatchAndApply = async (
    candidates: any[],
    buildPayload: (it: any) => Parameters<typeof api.normDecision>[0],
    actionLabel: string,
  ): Promise<boolean> => {
    if (candidates.length === 0) return false
    return await new Promise<boolean>((resolve) => {
      Modal.confirm({
        title: `同 VT 还有 ${candidates.length} 条同名字段推荐相同槽位`,
        content: (
          <div>
            <div style={{ marginBottom: 8 }}>
              字段 <Text code>{candidates[0].field_name}</Text> 在以下源表也是相同推荐：
            </div>
            <ul style={{ paddingLeft: 20, margin: 0, fontSize: 12 }}>
              {candidates.slice(0, 6).map((it) => (
                <li key={it.table_en}><Text code style={{ fontSize: 11 }}>{it.table_en}</Text></li>
              ))}
              {candidates.length > 6 ? <li>… 共 {candidates.length} 条</li> : null}
            </ul>
            <div style={{ marginTop: 8, color: '#999', fontSize: 12 }}>
              是否一起 {actionLabel}？
            </div>
          </div>
        ),
        okText: `一起处理（${candidates.length} 条）`,
        cancelText: '仅本条',
        onOk: async () => {
          const results = await Promise.allSettled(candidates.map((it) => api.normDecision(buildPayload(it))))
          const okCount = results.filter((r) => r.status === 'fulfilled').length
          message.success(`已批量${actionLabel} ${okCount} 条`)
          resolve(true)
        },
        onCancel: () => resolve(false),
      })
    })
  }
  useEffect(() => {
    if (vt_id) reloadNormItems()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [vt_id, normStatusFilter, normOnlyUnreviewed])

  // keyword 输入防抖：300ms 后才请求
  useEffect(() => {
    if (!vt_id) return
    const t = setTimeout(() => reloadNormItems(), 300)
    return () => clearTimeout(t)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [normKeyword])

  useEffect(() => {
    if (!vt_id) return
    // P1-5 防护：切换 VT 时把上一个 VT 的所有 loading state 强制复位，
    // 避免"上一个 VT 的 setXxx(true) 还没 resolve，切到新 VT 时 spinner 永远不复位"
    setSaving(false)
    setGenerating(false)
    setExtending(false)
    setScanningBlacklist(false)
    setApplyingBlacklist(false)
    setLoading(true)
    Promise.all([api.getVT(vt_id), api.getBaseSlots()])
      .then(([vt, bs]) => {
        setVt(vt)
        const dirty = toDirty(vt.slots)
        setSlots(dirty)
        setOriginalSlots(JSON.stringify(vt.slots))
        setBaseSlots(bs)
      })
      .finally(() => setLoading(false))
  }, [vt_id])

  const isDirty = useMemo(() => {
    const current = JSON.stringify(slots.map(stripKey))
    return current !== originalSlots
  }, [slots, originalSlots])

  const baseSlotByName = useMemo(() => {
    const m: Record<string, BaseSlot> = {}
    baseSlots.forEach((b) => (m[b.name] = b))
    return m
  }, [baseSlots])

  const handleSave = async () => {
    if (!vt_id) return
    setSaving(true)
    try {
      // 扁平化 slot.mapped_fields → seed_mappings，让后端把它们写入 field_normalization_reviewed.parquet
      // 作为下游 field_normalization 重算的种子
      const seedMappings: Array<{ table_en: string; field_name: string; slot_name: string }> = []
      slots.forEach((s) => {
        (s.mapped_fields || []).forEach((mf) => {
          if (mf.table_en && mf.field_name) {
            seedMappings.push({
              table_en: mf.table_en,
              field_name: mf.field_name,
              slot_name: s.name,
            })
          }
        })
      })
      const resp = await api.updateSlots(
        vt_id,
        slots.map(stripKey),
        undefined,
        seedMappings.length > 0 ? seedMappings : undefined,
      )
      const seedsMsg = resp?.seeds_written ? `，种子 ${resp.seeds_written} 条已写入` : ''
      message.success(`保存成功${seedsMsg}`)
      const fresh = await api.getVT(vt_id)
      setVt(fresh)
      setSlots(toDirty(fresh.slots))
      setOriginalSlots(JSON.stringify(fresh.slots))
      onSaved()
    } catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: string } }; message?: string }
      message.error(err.response?.data?.detail || err.message || '保存失败')
    } finally {
      setSaving(false)
    }
  }

  const handleReset = () => {
    if (!vt) return
    setSlots(toDirty(JSON.parse(originalSlots) as Slot[]))
  }

  const handleDelete = (key: number) => {
    setSlots((cur) => cur.filter((s) => s._key !== key))
  }

  // 合并两个槽位：keepSlot 保留（aliases 合并），dropSlot 从清单移除
  const handleMerge = (keepName: string, dropName: string) => {
    if (keepName === dropName) return
    setSlots((cur) => {
      const keep = cur.find((s) => s.name === keepName)
      const drop = cur.find((s) => s.name === dropName)
      if (!keep || !drop) return cur
      const mergedAliases = Array.from(new Set([
        ...(keep.aliases || []),
        ...(drop.aliases || []),
        // 把 drop 的 name/cn_name 也加到 aliases 以便后续能命中
        drop.name,
        drop.cn_name || '',
      ].filter(Boolean)))
      // base 槽位的 aliases 不可写到 slot_definitions（base 引用只有 from+name+role）
      // 如果 keep 是 base 且想合并 extended 的 aliases，需降级为 extended
      const keepAsExtended = keep.from === 'base' && drop.from === 'extended'
      const merged: DirtySlot = keepAsExtended
        ? {
            ...keep,
            from: 'extended',
            aliases: mergedAliases,
            cn_name: keep.cn_name || baseSlotByName[keep.name]?.cn_name || drop.cn_name,
            logical_type: keep.logical_type || baseSlotByName[keep.name]?.logical_type || drop.logical_type,
            llm_reason: `合并 ${dropName} (原 ${keep.name} 是 base，合并为 extended 以携带新 aliases)`,
          }
        : {
            ...keep,
            aliases: mergedAliases,
            llm_reason: keep.llm_reason || `合并 ${dropName}`,
          }
      return cur.filter((s) => s.name !== dropName).map((s) => (s.name === keepName ? merged : s))
    })
    message.success(`已合并 ${dropName} → ${keepName}（请审核后点「保存槽位」）`, 5)
    setMergeSource(null)
    setMergeTargetName(undefined)
  }

  // 相似度计算：name + cn_name + aliases 的 token Jaccard
  const computeSimilarPairs = () => {
    const tokenize = (s: string) =>
      s.toLowerCase().split(/[\s_·/·、\-]+/).filter(Boolean)
    const makeSet = (slot: DirtySlot) => {
      const effective = slot.aliases && slot.aliases.length > 0
        ? slot.aliases
        : slot.from === 'base' ? baseSlotByName[slot.name]?.aliases || [] : []
      const parts = [slot.name, slot.cn_name || '', ...(effective as string[])].filter(Boolean)
      const set = new Set<string>()
      parts.forEach((p) => tokenize(p).forEach((t) => set.add(t)))
      return set
    }
    const pairs: Array<{ a: DirtySlot; b: DirtySlot; sim: number; common: string[] }> = []
    const sets = slots.map((s) => ({ slot: s, tokens: makeSet(s) }))
    for (let i = 0; i < sets.length; i++) {
      for (let j = i + 1; j < sets.length; j++) {
        const a = sets[i].tokens
        const b = sets[j].tokens
        if (a.size === 0 || b.size === 0) continue
        const inter = new Set([...a].filter((t) => b.has(t)))
        const union = new Set([...a, ...b])
        const sim = inter.size / union.size
        if (sim >= 0.2) {
          pairs.push({
            a: sets[i].slot,
            b: sets[j].slot,
            sim,
            common: Array.from(inter),
          })
        }
      }
    }
    pairs.sort((x, y) => y.sim - x.sim)
    return pairs
  }

  // ========== I-14: 元信息编辑 ==========
  const openMetaEdit = async () => {
    if (!vt) return
    metaForm.setFieldsValue({
      topic: vt.topic,
      grain_desc: vt.grain_desc,
      table_type: vt.table_type,
      l_path: [vt.l2_path[0] || '', vt.l2_path[1] || ''].filter(Boolean),
    })
    if (categories.length === 0) {
      try {
        const resp = await api.getCategories()
        setCategories(resp.categories)
      } catch {}
    }
    setMetaEditOpen(true)
  }

  const handleMetaSave = async () => {
    if (!vt_id) return
    const values = await metaForm.validateFields()
    const path: string[] = values.l_path || []
    try {
      const resp = await api.updateVTMeta(vt_id, {
        topic: values.topic,
        grain_desc: values.grain_desc,
        table_type: values.table_type,
        l2_path: path.filter(Boolean),
      })
      message.success('元信息已保存')
      if (resp.next_action) setNextActionBanner(resp.next_action)
      setMetaEditOpen(false)
      const fresh = await api.getVT(vt_id)
      setVt(fresh)
      emitScaffoldChanged({ source: 'vt-editor', action: 'update_meta', extra: { vt_id } })
      onSaved()
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    }
  }

  // ========== I-14: 删除 VT ==========
  const handleDeleteVT = async () => {
    if (!vt_id) return
    try {
      const resp = await api.deleteVT(vt_id)
      message.success(`VT 已删除 (${resp.new_vt_total} 剩余)`)
      if (resp.next_action) {
        // 用 alert 让用户看清楚
        message.warning(resp.next_action, 5)
      }
      emitScaffoldChanged({ source: 'vt-editor', action: 'delete_vt', extra: { vt_id } })
      onSaved()
      navigate('/')
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    }
  }

  // ========== I-14: 源表增删 ==========
  const openAddSource = async () => {
    setAddSourceOpen(true)
    if (allTables.length === 0) {
      try {
        const t = await api.listAllTables()
        setAllTables(t)
      } catch (e: any) {
        message.error(e?.response?.data?.detail || String(e))
      }
    }
  }

  const handleAddSource = async (en: string, cn: string) => {
    if (!vt || !vt_id) return
    const existing = vt.source_tables_with_fields.map((s) => s.en)
    if (existing.includes(en)) {
      message.info('该表已在源表列表中')
      return
    }
    const newTables = [
      ...existing.map((e) => {
        const match = vt.source_tables_with_fields.find((s) => s.en === e)
        return { en: e, cn: match?.cn || '' }
      }),
      { en, cn },
    ]
    try {
      const resp = await api.updateVTMeta(vt_id, { candidate_tables: newTables })
      message.success(`已添加源表: ${en}`)
      if (resp.next_action) setNextActionBanner(resp.next_action)
      const fresh = await api.getVT(vt_id)
      setVt(fresh)
      emitScaffoldChanged({ source: 'vt-editor', action: 'add_source', extra: { vt_id, en } })
      onSaved()
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    }
  }

  // ========== I-16 Gap3: LLM 生成槽位（单 VT，重新生成）==========
  const handleGenerateSlots = async () => {
    if (!vt_id) return
    setGenerating(true)
    try {
      const resp = await api.generateSlotsForVT(vt_id)
      if (resp.slots.length === 0) {
        message.warning('LLM 返回空槽位')
        return
      }
      const newDirty = toDirty(resp.slots as unknown as Slot[])
      setSlots(newDirty)
      message.success(
        `重新生成 ${resp.slots.length} 个槽位（${resp.elapsed_sec}s${resp.elapsed_sec < 0.5 ? '，命中缓存' : ''}）。请审核后点「保存槽位」`,
        5,
      )
      if (resp.warnings.length > 0) {
        message.warning(`警告：${resp.warnings.join('；')}`, 8)
      }
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    } finally {
      setGenerating(false)
    }
  }

  // ========== I-16 扩展槽位 ==========
  const handleExtendSlots = async () => {
    if (!vt_id) return
    setExtending(true)
    try {
      const resp = await api.extendSlotsForVT(vt_id)
      setExtendResult(resp)
      setExtendSelectedKeys(resp.new_slots.map((s) => s.name))
      if (resp.new_slots.length === 0) {
        message.info(resp.summary || '没有需要补充的槽位')
      }
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    } finally {
      setExtending(false)
    }
  }

  const handleApplyExtendSelected = () => {
    if (!extendResult) return
    const selectedSlots = extendResult.new_slots.filter((s: any) => extendSelectedKeys.includes(s.name))
    if (selectedSlots.length === 0) {
      message.info('未选中任何槽位')
      return
    }
    const existingNames = new Set(slots.map((s) => s.name))
    const toAdd = selectedSlots.filter((s: any) => !existingNames.has(s.name))
    // 按 source 分别构造：base 只留 name/from/role/mapped_fields；extended 保留全字段
    // 两者都把 LLM 的 covers_mapped_fields（完整 dict）写入 mapped_fields，避免保存后种子字段丢失
    const normalized = toAdd.map((s: any) => {
      const mf = Array.isArray(s.covers_mapped_fields) && s.covers_mapped_fields.length > 0
        ? s.covers_mapped_fields
        : []
      if (s.source === 'base' || s.from === 'base') {
        return {
          name: s.name,
          from: 'base',
          role: s.role,
          mapped_fields: mf,
        } as unknown as Slot
      }
      return { ...s, from: 'extended', mapped_fields: mf } as Slot
    })
    const added = toDirty(normalized)
    setSlots((cur) => [...cur, ...added])
    const baseCount = toAdd.filter((s: any) => s.source === 'base' || s.from === 'base').length
    const extCount = toAdd.length - baseCount
    message.success(`已合并 ${added.length} 个槽位（base 复用 ${baseCount} / 新 extended ${extCount}）到表格，请审核后点「保存槽位」`)
    setExtendResult(null)
    setExtendSelectedKeys([])
  }

  const handleBlacklistScan = async () => {
    if (!vt_id) return
    setScanningBlacklist(true)
    try {
      const resp = await api.blacklistScanForVT(vt_id)
      setBlacklistScanResult(resp)
      // 默认勾选：尚未在 exact_names 里的候选（那些才是有意义要加的）
      const defaultSel = resp.candidates
        .filter((c) => !c.already_in_exact)
        .map((c) => c.field_name)
      setScanSelectedFields(defaultSel)
      setScanModalOpen(true)
      if (resp.matched === 0) {
        message.info('没有扫描到符合黑名单规则的字段')
      }
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    } finally {
      setScanningBlacklist(false)
    }
  }

  const handleBatchBlacklistApply = async () => {
    if (!blacklistScanResult) return
    if (scanSelectedFields.length === 0) {
      message.info('未选中任何字段')
      return
    }
    // 去重（UI 已按 field_name 勾选，但可能跨表重复的同名）
    const uniq = Array.from(new Set(scanSelectedFields))
    setApplyingBlacklist(true)
    try {
      const resp = await api.batchAddFieldBlacklist(uniq, `VT ${vt_id} 批量扫描踢除`)
      const parts = [`新增 ${resp.added_count}`]
      if (resp.skipped_count > 0) parts.push(`跳过 ${resp.skipped_count}（已在黑名单）`)
      if (resp.added_count > 0) {
        try {
          const job = await api.startPipelineJob('field_features')
          message.success(
            `${parts.join('，')}。后台重跑 pipeline（${job.job_id}）让黑名单生效`,
            6,
          )
        } catch (jobErr: any) {
          if (jobErr?.response?.status === 409) {
            message.warning(
              `${parts.join('，')}。已有 pipeline 在跑，等它结束后再做任何黑名单动作会触发新一轮重跑`,
              8,
            )
          } else {
            message.error(
              `${parts.join('，')}。启动 pipeline 失败: ${jobErr?.response?.data?.detail || String(jobErr)}`,
              8,
            )
          }
        }
      } else {
        message.info(parts.join('，'))
      }
      setScanModalOpen(false)
      setBlacklistScanResult(null)
      setScanSelectedFields([])
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    } finally {
      setApplyingBlacklist(false)
    }
  }

  const handleRemoveSource = async (en: string) => {
    if (!vt || !vt_id) return
    const newTables = vt.source_tables_with_fields
      .filter((s) => s.en !== en)
      .map((s) => ({ en: s.en, cn: s.cn }))
    try {
      const resp = await api.updateVTMeta(vt_id, { candidate_tables: newTables })
      message.success(`已移除源表: ${en}`)
      if (resp.next_action) setNextActionBanner(resp.next_action)
      const fresh = await api.getVT(vt_id)
      setVt(fresh)
      emitScaffoldChanged({ source: 'vt-editor', action: 'remove_source', extra: { vt_id, en } })
      onSaved()
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    }
  }

  const handleAdd = () => {
    setEditing({
      _key: newKey(),
      name: '',
      from: 'extended',
      role: 'display',
      cn_name: '',
      logical_type: 'custom',
      aliases: [],
    })
    form.resetFields()
  }

  const handleEdit = (slot: DirtySlot) => {
    setEditing({ ...slot })
    form.setFieldsValue({
      name: slot.name,
      from: slot.from,
      role: slot.role,
      cn_name: slot.cn_name || '',
      logical_type: slot.logical_type || '',
      aliases: (slot.aliases || []).join(', '),
      llm_reason: slot.llm_reason || '',
    })
  }

  const handleEditorOk = async () => {
    const values = await form.validateFields()
    if (!editing) return

    const cleaned: DirtySlot = {
      _key: editing._key,
      name: values.name.trim(),
      from: values.from,
      role: values.role,
    }
    if (values.from === 'extended') {
      cleaned.cn_name = values.cn_name.trim()
      cleaned.logical_type = (values.logical_type || '').trim() || 'custom'
      const aliasList = (values.aliases || '')
        .split(/[,，]/)
        .map((s: string) => s.trim())
        .filter(Boolean)
      cleaned.aliases = aliasList
      cleaned.llm_reason = values.llm_reason?.trim() || ''
      if (aliasList.length < 3) {
        message.warning('extended 槽位建议至少 3 个 aliases')
      }
    } else {
      const ref = baseSlotByName[cleaned.name]
      if (!ref) {
        message.error(`base 槽位 ${cleaned.name} 不在 base_slots 库中`)
        return
      }
    }

    const exists = slots.some((s) => s._key !== editing._key && s.name === cleaned.name)
    if (exists) {
      message.error(`槽位名 ${cleaned.name} 已存在`)
      return
    }

    setSlots((cur) => {
      const idx = cur.findIndex((s) => s._key === editing._key)
      if (idx === -1) return [...cur, cleaned]
      const next = [...cur]
      next[idx] = cleaned
      return next
    })
    setEditing(null)
  }

  // 「按源表看字段」过滤后的源表（每张表的 fields_sample 按搜索串过滤；空表搜索时隐藏）
  const filteredSourceTables = useMemo(() => {
    if (!vt) return []
    const q = bySourceSearch.trim().toLowerCase()
    if (!q) return vt.source_tables_with_fields
    return vt.source_tables_with_fields
      .map((st) => {
        const matchedFields = st.fields_sample.filter((f: any) =>
          (f.field || '').toLowerCase().includes(q)
          || (f.comment || '').toLowerCase().includes(q)
          || (f.mapped_slot || '').toLowerCase().includes(q),
        )
        return { ...st, fields_sample: matchedFields, _matched: matchedFields.length > 0 }
      })
      .filter((st: any) => st._matched)
  }, [vt, bySourceSearch])

  // 搜索时自动展开所有匹配的 source table panel
  useEffect(() => {
    if (bySourceSearch.trim()) {
      setBySourceActiveKey(filteredSourceTables.map((_, idx) => String(idx)))
    }
  }, [bySourceSearch, filteredSourceTables.length])

  // 搜索过滤后的 slots：按 name / cn_name / aliases 子串匹配
  const filteredSlots = useMemo(() => {
    const q = slotSearch.trim().toLowerCase()
    if (!q) return slots
    return slots.filter((s) => {
      if (s.name.toLowerCase().includes(q)) return true
      if ((s.cn_name || '').toLowerCase().includes(q)) return true
      const aliases = s.aliases || (s.from === 'base' ? baseSlotByName[s.name]?.aliases || [] : [])
      return aliases.some((a) => String(a).toLowerCase().includes(q))
    })
  }, [slots, slotSearch, baseSlotByName])

  // 聚合：slot.name → {fields, hit, maxScore}（仅统计「人工已审且确认归属」的字段）
  // decision ∈ {accept_top1, use_top2, use_top3, use_slot, mark_new_slot} 才计入；
  // mark_noise / skip / 未审 不计
  const slotHitMap = useMemo(() => {
    const REVIEWED_DECISIONS = new Set(['accept_top1', 'use_top2', 'use_top3', 'use_slot', 'mark_new_slot'])
    const m: Record<string, { fields: any[]; hit: number; maxScore: number }> = {}
    if (!vt) return m
    vt.source_tables_with_fields.forEach((st) => {
      st.fields_sample.forEach((f: any) => {
        if (!f.mapped_slot) return
        if (!REVIEWED_DECISIONS.has(f.decision)) return
        if (!m[f.mapped_slot]) {
          m[f.mapped_slot] = { fields: [], hit: 0, maxScore: 0 }
        }
        const bucket = m[f.mapped_slot]
        bucket.fields.push({ ...f, table_en: st.en, table_cn: st.cn })
        bucket.hit++
        if (typeof f.selected_score === 'number' && f.selected_score > bucket.maxScore) {
          bucket.maxScore = f.selected_score
        }
      })
    })
    return m
  }, [vt])

  const columns = [
    {
      title: 'Name',
      dataIndex: 'name',
      key: 'name',
      width: 220,
      render: (name: string, row: DirtySlot) => (
        <Space direction="vertical" size={0}>
          <Text strong>{name}</Text>
          {row.from === 'base' && baseSlotByName[name] && (
            <Text type="secondary" style={{ fontSize: 12 }}>
              {baseSlotByName[name].cn_name}
            </Text>
          )}
          {row.from === 'extended' && row.cn_name && (
            <Text type="secondary" style={{ fontSize: 12 }}>
              {row.cn_name}
            </Text>
          )}
        </Space>
      ),
    },
    {
      title: 'From',
      dataIndex: 'from',
      key: 'from',
      width: 95,
      render: (v: string) => (
        <Tag color={v === 'base' ? 'blue' : 'gold'}>{v}</Tag>
      ),
    },
    {
      title: 'Role',
      dataIndex: 'role',
      key: 'role',
      width: 140,
      render: (role: string) => <Tag>{role}</Tag>,
    },
    {
      title: 'Logical Type',
      dataIndex: 'logical_type',
      key: 'logical_type',
      width: 180,
      render: (v: string, row: DirtySlot) => {
        const effective = v || (row.from === 'base' && baseSlotByName[row.name]?.logical_type) || ''
        return <Text type="secondary">{effective}</Text>
      },
    },
    {
      title: 'Aliases',
      dataIndex: 'aliases',
      key: 'aliases',
      width: 260,
      render: (aliases: string[] | undefined, row: DirtySlot) => {
        const effective = aliases && aliases.length > 0
          ? aliases
          : (row.from === 'base' ? baseSlotByName[row.name]?.aliases || [] : [])
        return (
          <Space size={[0, 4]} wrap>
            {effective.slice(0, 6).map((a) => (
              <Tag key={a} style={{ marginBottom: 4 }}>{a}</Tag>
            ))}
            {effective.length > 6 && <Text type="secondary">+{effective.length - 6}</Text>}
          </Space>
        )
      },
    },
    {
      title: (
        <Space size={4}>
          <span>理由</span>
          <Tooltip title="LLM 生成/扩展槽位时给出的建议理由（base 槽位复用自 base_slots 一般无理由）">
            <Text type="secondary" style={{ fontSize: 11 }}>ⓘ</Text>
          </Tooltip>
        </Space>
      ),
      dataIndex: 'llm_reason',
      key: 'llm_reason',
      width: 60,
      align: 'center' as const,
      render: (v: string, row: DirtySlot) => {
        if (!v) {
          return <Text type="secondary" style={{ fontSize: 11 }}>{row.from === 'base' ? '—' : '-'}</Text>
        }
        return (
          <Tooltip title={v} styles={{ root: { maxWidth: 480 } }}>
            <Text type="secondary" style={{ fontSize: 14, cursor: 'help' }}>ⓘ</Text>
          </Tooltip>
        )
      },
    },
    {
      title: (
        <Space size={4}>
          <span>种子字段</span>
          <Tooltip title="重新生成时 LLM 为该槽位指定的源字段（mapped_fields）。保存后会写入 field_normalization_reviewed.parquet 作为下游归一化种子">
            <Text type="secondary" style={{ fontSize: 11 }}>ⓘ</Text>
          </Tooltip>
        </Space>
      ),
      key: 'mapped_fields',
      width: 80,
      sorter: (a: DirtySlot, b: DirtySlot) => (a.mapped_fields?.length || 0) - (b.mapped_fields?.length || 0),
      render: (_: unknown, row: DirtySlot) => {
        const n = (row.mapped_fields || []).length
        if (n === 0) return <Text type="secondary" style={{ fontSize: 11 }}>—</Text>
        return (
          <Tag color={n >= 2 ? 'purple' : 'geekblue'} style={{ margin: 0 }}>
            种子 {n}
          </Tag>
        )
      },
    },
    {
      title: '归一命中',
      key: 'hit',
      width: 90,
      sorter: (a: DirtySlot, b: DirtySlot) => (slotHitMap[a.name]?.hit || 0) - (slotHitMap[b.name]?.hit || 0),
      render: (_: unknown, row: DirtySlot) => {
        const hit = slotHitMap[row.name]?.hit || 0
        return hit === 0
          ? <Tag color="default" style={{ fontSize: 10 }}>⚠ 0</Tag>
          : <Tag color={hit >= 5 ? 'green' : hit >= 2 ? 'gold' : 'default'}>{hit}</Tag>
      },
    },
    {
      title: '操作',
      key: 'actions',
      width: 200,
      render: (_: unknown, row: DirtySlot) => (
        <Space size={4}>
          <Button size="small" icon={<EditOutlined />} onClick={() => handleEdit(row)}>
            编辑
          </Button>
          <Tooltip title="把此槽位合并到另一个：另一个为主（保留），此槽位的 name/cn_name/aliases 全部并入，然后此槽位从清单移除">
            <Button size="small" onClick={() => { setMergeSource(row); setMergeTargetName(undefined) }}>
              合并
            </Button>
          </Tooltip>
          <Popconfirm title="删除此槽位？" onConfirm={() => handleDelete(row._key)}>
            <Button size="small" icon={<DeleteOutlined />} danger />
          </Popconfirm>
        </Space>
      ),
    },
  ]

  if (loading) {
    return (
      <div style={{ padding: 40, textAlign: 'center' }}>
        <Spin size="large" />
      </div>
    )
  }

  if (!vt) return <div style={{ padding: 24 }}>未找到</div>

  const baseCount = slots.filter((s) => s.from === 'base').length
  const extCount = slots.length - baseCount
  const reuseRatio = slots.length ? (baseCount / slots.length) * 100 : 0

  return (
    <div style={{ padding: 24 }}>
      <Space style={{ marginBottom: 16, justifyContent: 'space-between', width: '100%' }}>
        <div>
          <Title level={3} style={{ margin: 0 }}>
            <Tag color={TABLE_TYPE_COLOR[vt.table_type] || 'default'}>{vt.table_type}</Tag>
            {vt.topic}
            {vt.is_pending && <Tag color="red" style={{ marginLeft: 8 }}>待定</Tag>}
          </Title>
          <Text type="secondary">{vt.l2_path.join(' / ')} · {vt.vt_id}</Text>
        </div>
        <Space>
          <Tooltip title="从零重新生成：LLM 基于源表 DDL 产出一套完整槽位清单（覆盖当前 slots，需手动点「保存槽位」才写入）">
            <Popconfirm
              title="重新生成会覆盖当前所有槽位改动"
              description="当前表格里的槽位会被 LLM 输出替换，保存前仍可重置。确定？"
              onConfirm={handleGenerateSlots}
              disabled={!vt.source_tables_with_fields.length}
            >
              <Button
                icon={<ThunderboltOutlined />}
                loading={generating}
                disabled={!vt.source_tables_with_fields.length}
              >
                重新生成
              </Button>
            </Popconfirm>
          </Tooltip>
          <Tooltip title="扩展生成：在现有槽位基础上，LLM 分析未被覆盖的字段，建议新增 extended 槽位；你再勾选合并">
            <Button
              icon={<ThunderboltOutlined />}
              loading={extending}
              onClick={handleExtendSlots}
              disabled={!vt.source_tables_with_fields.length}
            >
              扩展槽位
            </Button>
          </Tooltip>
          <Tooltip title="只针对本 VT 重跑：slot_scores 过滤后重算 → field_normalization 决策重算 → merge 回 parquet（不动其他 VT）。异步，右下角看进度">
            <Popconfirm
              title="重跑本 VT 的归一？"
              description={
                <div>
                  <div>只重算 <Text code>{vt.vt_id}</Text> 的 slot_scores + field_normalization</div>
                  <div style={{ color: '#999', fontSize: 11, marginTop: 4 }}>
                    异步执行，~10-40 秒。其他 VT 不动。完成后自动刷新。
                  </div>
                </div>
              }
              onConfirm={async () => {
                if (!vt_id) return
                try {
                  const resp = await api.rerunVTNormalization(vt_id)
                  message.success(`已启动 (${resp.job_id})，右下角看进度`, 6)
                } catch (e: any) {
                  message.error(e?.response?.data?.detail || String(e))
                }
              }}
            >
              <Button icon={<ReloadOutlined />}>重跑归一（仅本 VT）</Button>
            </Popconfirm>
          </Tooltip>
          <Button icon={<EditOutlined />} onClick={openMetaEdit}>编辑元信息</Button>
          <Popconfirm
            title={`确认删除 VT ${vt.vt_id}？`}
            description={
              <div>
                <div>这会从 scaffold 中移除该 VT</div>
                <div style={{ color: '#d46b08', marginTop: 4 }}>
                  首次编辑会备份 scaffold；删除后需运行 run_pipeline.py --from slot_definitions 让下游同步
                </div>
              </div>
            }
            okText="删除"
            okButtonProps={{ danger: true }}
            onConfirm={handleDeleteVT}
          >
            <Button danger icon={<DeleteOutlined />}>删除 VT</Button>
          </Popconfirm>
          {isDirty && (
            <Tooltip title="撤销所有未保存改动">
              <Button icon={<UndoOutlined />} onClick={handleReset}>重置</Button>
            </Tooltip>
          )}
          <Button
            type="primary"
            icon={<SaveOutlined />}
            disabled={!isDirty}
            loading={saving}
            onClick={handleSave}
          >
            保存槽位
          </Button>
        </Space>
      </Space>

      {nextActionBanner && (
        <Alert
          type="warning"
          showIcon
          closable
          message="scaffold 已修改，需重跑下游才能生效"
          description={<Space direction="vertical">
            <Text>请在终端执行：</Text>
            <Text code copyable>{nextActionBanner}</Text>
          </Space>}
          style={{ marginBottom: 16 }}
          onClose={() => setNextActionBanner(null)}
        />
      )}

      {vt.is_pending && (
        <Alert
          type="warning"
          message="本 VT 为『待定』类型（脚手架阶段被识别为 misplaced 表暂留当前 L2）"
          description={
            <Space direction="vertical" size={8} style={{ width: '100%' }}>
              {vt.review_hint ? (
                <>
                  <div style={{ fontSize: 13 }}>
                    <Text>LLM 建议归属到 </Text>
                    <Tag color="blue">{vt.review_hint.llm_suggested_l2}</Tag>
                    <Text type="secondary"> （当前：{vt.review_hint.current_l2}）</Text>
                  </div>
                  {vt.review_hint.items && vt.review_hint.items.length > 0 && (
                    <div style={{ fontSize: 12, color: '#666' }}>
                      理由：{vt.review_hint.items[0].reason}
                    </div>
                  )}
                </>
              ) : (
                <div style={{ fontSize: 13, color: '#999' }}>
                  此 VT 没有 review_hint 详情（可能只是 table_type=待定）
                </div>
              )}
              <Space>
                <Popconfirm
                  title="确认保留当前 L2"
                  description="清除这条『待定』提示。脚手架 l2_path 不变。"
                  onConfirm={async () => {
                    if (!vt_id) return
                    try {
                      await api.updateVTMeta(vt_id, { clear_review_hint: true })
                      message.success('已确认当前 L2，提示已清除')
                      const fresh = await api.getVT(vt_id)
                      setVt(fresh)
                      onSaved()
                    } catch (e: any) {
                      message.error(e?.response?.data?.detail || String(e))
                    }
                  }}
                >
                  <Button size="small">✓ 确认保留当前 L2</Button>
                </Popconfirm>
                {vt.review_hint && vt.review_hint.llm_suggested_l2 && (
                  <Popconfirm
                    title={`迁移到 ${vt.review_hint.llm_suggested_l2}`}
                    description={
                      <div style={{ maxWidth: 360 }}>
                        将 l2_path 从 <Text code>{vt.review_hint.current_l2}</Text> 改为{' '}
                        <Text code>{vt.review_hint.llm_suggested_l2}</Text>，并清除『待定』提示。
                        <br />
                        <Text type="secondary" style={{ fontSize: 12 }}>
                          L1 保持不变；如果目标 L2 不存在会按现有习惯创建
                        </Text>
                      </div>
                    }
                    onConfirm={async () => {
                      if (!vt_id || !vt.review_hint) return
                      const l1 = vt.l2_path[0] || '主体主档'
                      const newPath = [l1, vt.review_hint.llm_suggested_l2]
                      try {
                        await api.updateVTMeta(vt_id, {
                          l2_path: newPath,
                          clear_review_hint: true,
                        })
                        message.success(`已迁移到 ${vt.review_hint.llm_suggested_l2}`)
                        const fresh = await api.getVT(vt_id)
                        setVt(fresh)
                        emitScaffoldChanged({
                          source: 'vt-editor',
                          action: 'migrate_l2',
                          extra: { vt_id, new_l2: vt.review_hint.llm_suggested_l2 },
                        })
                        onSaved()
                      } catch (e: any) {
                        message.error(e?.response?.data?.detail || String(e))
                      }
                    }}
                  >
                    <Button size="small" type="primary">
                      → 迁移到建议 L2
                    </Button>
                  </Popconfirm>
                )}
              </Space>
            </Space>
          }
          style={{ marginBottom: 16 }}
        />
      )}

      <Card size="small" style={{ marginBottom: 16 }}>
        <Descriptions column={4} size="small">
          <Descriptions.Item label="粒度">{vt.grain_desc}</Descriptions.Item>
          <Descriptions.Item label="源表数">{vt.source_table_count}</Descriptions.Item>
          <Descriptions.Item label="槽位数">
            {slots.length} (base={baseCount}, ext={extCount})
          </Descriptions.Item>
          <Descriptions.Item label="base 复用率">
            {reuseRatio.toFixed(1)}%
          </Descriptions.Item>
        </Descriptions>
        {vt.summary && (
          <Text type="secondary" style={{ fontSize: 12 }}>
            LLM summary: {vt.summary}
          </Text>
        )}
      </Card>

      <Card size="small" style={{ marginBottom: 16 }}>
      <Tabs
        size="small"
        defaultActiveKey="slots"
        items={[
          {
            key: 'slots',
            label: (() => {
              const total = slots.length
              const empty = slots.filter((s) => !(slotHitMap[s.name]?.hit)).length
              return (
                <Space>
                  <span>槽位（{total}）</span>
                  {empty > 0 && (
                    <Tooltip title={`${empty} 个槽位无字段命中（潜在冗余）`}>
                      <Tag color="default" style={{ fontSize: 10, margin: 0 }}>{empty} 空</Tag>
                    </Tooltip>
                  )}
                </Space>
              )
            })(),
            children: (
              <>
                <Space style={{ marginBottom: 8, width: '100%', justifyContent: 'space-between' }}>
                  <Space size={8}>
                    <Input.Search
                      placeholder="搜索 name / 中文名 / aliases"
                      allowClear
                      size="small"
                      style={{ width: 280 }}
                      value={slotSearch}
                      onChange={(e) => setSlotSearch(e.target.value)}
                    />
                    {slotSearch && (
                      <Text type="secondary" style={{ fontSize: 12 }}>
                        匹配 {filteredSlots.length} / {slots.length}
                      </Text>
                    )}
                  </Space>
                  <Space>
                    <Button size="small" onClick={() => setSimilarityOpen(true)}>
                      检查相似槽位
                    </Button>
                    <Button icon={<PlusOutlined />} onClick={handleAdd} size="small">
                      新增槽位
                    </Button>
                  </Space>
                </Space>
                <Table
                  rowKey="_key"
                  size="small"
                  dataSource={filteredSlots}
                  columns={columns}
                  pagination={false}
                  scroll={{ x: 'max-content' }}
                  expandable={{
                    rowExpandable: (row) =>
                      (slotHitMap[row.name]?.hit || 0) > 0 || (row.mapped_fields?.length || 0) > 0,
                    expandedRowRender: (row) => {
                      const hitFields = slotHitMap[row.name]?.fields || []
                      const mfs = row.mapped_fields || []
                      // 合并：按 (table_en, field_name) 去重；一个字段可能同时是种子 + 归一命中
                      type RowData = {
                        table_en: string
                        table_cn?: string
                        field_name: string
                        comment: string
                        is_seed: boolean
                        is_hit: boolean
                        review_status?: string
                        selected_score?: number | null
                      }
                      const byKey = new Map<string, RowData>()
                      for (const m of mfs) {
                        const key = `${m.table_en}|${m.field_name}`
                        byKey.set(key, {
                          table_en: m.table_en || '',
                          field_name: m.field_name,
                          comment: m.field_comment || '',
                          is_seed: true,
                          is_hit: false,
                        })
                      }
                      for (const h of hitFields as any[]) {
                        const key = `${h.table_en}|${h.field}`
                        const existing = byKey.get(key)
                        if (existing) {
                          existing.is_hit = true
                          existing.table_cn = h.table_cn
                          existing.review_status = h.review_status
                          existing.selected_score = h.selected_score
                          if (!existing.comment && h.comment) existing.comment = h.comment
                        } else {
                          byKey.set(key, {
                            table_en: h.table_en,
                            table_cn: h.table_cn,
                            field_name: h.field,
                            comment: h.comment || '',
                            is_seed: false,
                            is_hit: true,
                            review_status: h.review_status,
                            selected_score: h.selected_score,
                          })
                        }
                      }
                      const unified = Array.from(byKey.values())
                      const seedOnly = unified.filter((x) => x.is_seed && !x.is_hit).length
                      const hitOnly = unified.filter((x) => !x.is_seed && x.is_hit).length
                      const both = unified.filter((x) => x.is_seed && x.is_hit).length

                      return (
                        <div>
                          <Space size={12} style={{ marginBottom: 8, fontSize: 11 }}>
                            <Text type="secondary">关联字段 {unified.length} 条：</Text>
                            <Tag color="purple" style={{ fontSize: 10, margin: 0 }}>两者 {both}</Tag>
                            <Tag color="geekblue" style={{ fontSize: 10, margin: 0 }}>纯种子 {seedOnly}</Tag>
                            <Tag color="cyan" style={{ fontSize: 10, margin: 0 }}>纯归一 {hitOnly}</Tag>
                            <Tooltip title={'种子 = slot_definitions.mapped_fields；归一 = field_normalization.selected_slot 命中该槽位。理想状态两者一致；纯种子=anchor 但未影响打分；纯归一=自动归一但未固化为种子'}>
                              <Text type="secondary" style={{ fontSize: 11 }}>ⓘ</Text>
                            </Tooltip>
                          </Space>
                          <Table
                            size="small"
                            rowKey={(f: RowData) => `${f.table_en}|${f.field_name}`}
                            pagination={false}
                            dataSource={unified}
                            columns={[
                              {
                                title: 'field',
                                width: 180,
                                render: (_: unknown, f: RowData) => (
                                  <Text code style={{ fontSize: 11 }}>{f.field_name}</Text>
                                ),
                              },
                              {
                                title: '来源',
                                width: 110,
                                render: (_: unknown, f: RowData) => (
                                  <Space size={2}>
                                    {f.is_seed && f.is_hit && <Tag color="purple" style={{ fontSize: 10, margin: 0 }}>两者</Tag>}
                                    {f.is_seed && !f.is_hit && <Tag color="geekblue" style={{ fontSize: 10, margin: 0 }}>种子</Tag>}
                                    {!f.is_seed && f.is_hit && <Tag color="cyan" style={{ fontSize: 10, margin: 0 }}>归一</Tag>}
                                  </Space>
                                ),
                              },
                              { title: 'comment', dataIndex: 'comment', width: 200, ellipsis: true },
                              {
                                title: 'table',
                                ellipsis: true,
                                render: (_: unknown, f: RowData) => (
                                  <Space direction="vertical" size={0}>
                                    <Text code style={{ fontSize: 11 }}>{f.table_en}</Text>
                                    {f.table_cn && (
                                      <Text type="secondary" style={{ fontSize: 11 }}>{f.table_cn}</Text>
                                    )}
                                  </Space>
                                ),
                              },
                              {
                                title: 'status',
                                width: 100,
                                render: (_: unknown, f: RowData) => {
                                  const v = f.review_status
                                  if (!v) return <Text type="secondary" style={{ fontSize: 10 }}>-</Text>
                                  const color = v === 'auto_accepted' ? 'green' : v === 'needs_review' ? 'gold'
                                    : v === 'conflict' ? 'magenta' : v === 'low_confidence' ? 'red'
                                    : v === 'manual' || v === 'manual_new' ? 'blue' : 'default'
                                  return <Tag color={color} style={{ fontSize: 10 }}>{v}</Tag>
                                },
                              },
                              {
                                title: 'score',
                                width: 70,
                                render: (_: unknown, f: RowData) =>
                                  typeof f.selected_score === 'number' ? f.selected_score.toFixed(3) : '-',
                              },
                            ]}
                          />
                        </div>
                      )
                    },
                  }}
                />
              </>
            ),
          },
          {
            key: 'by-source',
            label: (
              <Space>
                <span>按源表看字段（{vt.source_tables_with_fields.length}）</span>
                <Tooltip title="每源表独立看；字段「使用」= SQL usage_count>0；「归一结果」= I-04 top1">
                  <Text type="secondary" style={{ fontSize: 11 }}>ⓘ</Text>
                </Tooltip>
              </Space>
            ),
            children: (() => {
              const totalSample = vt.source_tables_with_fields.reduce(
                (a, st) => a + st.sample_size,
                0,
              )
              const totalUsed = vt.source_tables_with_fields.reduce(
                (a, st) => a + st.used_count,
                0,
              )
              const totalMapped = vt.source_tables_with_fields.reduce(
                (a, st) => a + (st.mapped_count || 0),
                0,
              )
              const totalUsedMapped = vt.source_tables_with_fields.reduce(
                (a, st) => a + (st.used_mapped_count || 0),
                0,
              )
              const overall = totalSample ? (totalUsed / totalSample) * 100 : 0
              const mappedOverall = totalSample ? (totalMapped / totalSample) * 100 : 0
              const usedMappedOverall = totalUsed ? (totalUsedMapped / totalUsed) * 100 : 0
              return (
                <>
                  <Space style={{ marginBottom: 12, width: '100%', justifyContent: 'space-between' }}>
                    <Space>
                      <MetricTooltip kind="usage">
                        <Text>
                          <strong>使用率：</strong>
                          <Tag color={overall >= 60 ? 'green' : overall >= 40 ? 'gold' : 'red'}>
                            {totalUsed}/{totalSample} ({overall.toFixed(1)}%)
                          </Tag>
                        </Text>
                      </MetricTooltip>
                      <MetricTooltip kind="mapped">
                        <Text>
                          <strong>审核映射率：</strong>
                          <Tag color={mappedOverall >= 60 ? 'green' : mappedOverall >= 40 ? 'gold' : 'red'}>
                            {totalMapped}/{totalSample} ({mappedOverall.toFixed(1)}%)
                          </Tag>
                        </Text>
                      </MetricTooltip>
                      <MetricTooltip kind="usage_mapped">
                        <Text>
                          <strong>已用映射率：</strong>
                          <Tag color={usedMappedOverall >= 60 ? 'green' : usedMappedOverall >= 40 ? 'gold' : 'red'}>
                            {totalUsedMapped}/{totalUsed} ({usedMappedOverall.toFixed(1)}%)
                          </Tag>
                        </Text>
                      </MetricTooltip>
                    </Space>
                    <Space>
                      <Input.Search
                        placeholder="跨表搜索 field / comment / 槽位名"
                        allowClear
                        size="small"
                        style={{ width: 280 }}
                        value={bySourceSearch}
                        onChange={(e) => setBySourceSearch(e.target.value)}
                      />
                      <Button
                        size="small"
                        icon={<StopOutlined />}
                        loading={scanningBlacklist}
                        onClick={handleBlacklistScan}
                      >
                        扫描黑名单
                      </Button>
                      <Button size="small" icon={<PlusOutlined />} onClick={openAddSource}>
                        添加源表
                      </Button>
                    </Space>
                  </Space>
                  {bySourceSearch && (
                    <Text type="secondary" style={{ fontSize: 12, marginBottom: 8, display: 'block' }}>
                      匹配 {filteredSourceTables.length} / {vt.source_tables_with_fields.length} 张表，共 {filteredSourceTables.reduce((a, st) => a + st.fields_sample.length, 0)} 个字段
                    </Text>
                  )}
        <Collapse
          activeKey={bySourceActiveKey}
          onChange={(keys) => setBySourceActiveKey(Array.isArray(keys) ? keys : [keys])}
          items={[...filteredSourceTables]
            .sort((a, b) => (b.coverage_ratio || 0) - (a.coverage_ratio || 0))
            .map((st: SourceTableWithFields, idx) => {
            const ratio = st.coverage_ratio * 100
            const color = ratio >= 60 ? 'green' : ratio >= 40 ? 'gold' : 'red'
            const mappedRatio = (st.mapped_ratio || 0) * 100
            const mappedColor = mappedRatio >= 60 ? 'green' : mappedRatio >= 40 ? 'gold' : 'red'
            const usedMappedRatio = (st.used_mapped_ratio || 0) * 100
            const usedMappedColor = usedMappedRatio >= 60 ? 'green' : usedMappedRatio >= 40 ? 'gold' : 'red'
            return {
              key: String(idx),
              label: (
                <Space style={{ width: '100%', justifyContent: 'space-between' }}>
                  <Space>
                    <Text strong>{st.cn || st.en}</Text>
                    <Text type="secondary" style={{ fontSize: 12 }}>{st.en}</Text>
                    <Tag>{st.field_count} 字段</Tag>
                    <MetricTooltip kind="usage">
                      <Tag color={color}>
                        使用 {st.used_count}/{st.sample_size} ({ratio.toFixed(0)}%)
                      </Tag>
                    </MetricTooltip>
                    <MetricTooltip kind="mapped">
                      <Tag color={mappedColor}>
                        已审 {st.mapped_count || 0}/{st.sample_size} ({mappedRatio.toFixed(0)}%)
                      </Tag>
                    </MetricTooltip>
                    <MetricTooltip kind="usage_mapped">
                      <Tag color={usedMappedColor}>
                        已用映射 {st.used_mapped_count || 0}/{st.used_count} ({usedMappedRatio.toFixed(0)}%)
                      </Tag>
                    </MetricTooltip>
                  </Space>
                  <Popconfirm
                    title={`从本 VT 移除源表 ${st.en}？`}
                    description="该表本身不会删除，只从本 VT 的 candidate_tables 里移除"
                    onConfirm={(e) => { e?.stopPropagation(); handleRemoveSource(st.en) }}
                    onCancel={(e) => e?.stopPropagation()}
                  >
                    <Button size="small" danger onClick={(e) => e.stopPropagation()}>
                      移除
                    </Button>
                  </Popconfirm>
                </Space>
              ),
              children: (
                <Table
                  size="small"
                  pagination={false}
                  dataSource={st.fields_sample.map((f, i) => ({ ...f, _i: i }))}
                  rowKey="_i"
                  columns={[
                    {
                      title: '使用',
                      dataIndex: 'used',
                      width: 80,
                      render: (used: boolean, row: any) => {
                        const uc = row.usage_count ?? 0
                        const sc = row.sql_count ?? 0
                        if (used) {
                          return (
                            <Tooltip title={`在 ${sc} 个 SQL 中被使用 ${uc} 次`}>
                              <Tag color="green">已用</Tag>
                            </Tooltip>
                          )
                        }
                        return (
                          <Tooltip title="没有 SQL 使用记录">
                            <Tag>未用</Tag>
                          </Tooltip>
                        )
                      },
                    },
                    {
                      title: '归一结果（可改）',
                      dataIndex: 'mapped_slot',
                      width: 280,
                      render: (slot: string | null, row: any) => {
                        const rs = row.review_status as string | null | undefined
                        const color = rs === 'auto_accepted' ? 'green'
                          : rs === 'needs_review' ? 'gold'
                          : rs === 'conflict' ? 'magenta'
                          : rs === 'low_confidence' ? 'red'
                          : 'blue'
                        const scoreStr = typeof row.selected_score === 'number'
                          ? row.selected_score.toFixed(2)
                          : null

                        // 提交归一决策（Select 改值或「✓ 确认」按钮触发）
                        const submitDecision = async (newSlot: string) => {
                          if (!vt_id) return
                          // 判定 decision 类型：与 top1/2/3 匹配则用对应 accept_*/use_top*，否则 use_slot
                          let decision = 'use_slot'
                          if (newSlot === row.top1_slot) decision = 'accept_top1'
                          else if (newSlot === row.top2_slot) decision = 'use_top2'
                          else if (newSlot === row.top3_slot) decision = 'use_top3'
                          try {
                            await api.normDecision({
                              table_en: st.en,
                              field_name: row.field,
                              vt_id,
                              decision,
                              selected_slot: newSlot,
                            })
                            message.success(`已确认归一: ${row.field} → ${newSlot}`)
                            const fresh = await api.getVT(vt_id)
                            setVt(fresh)
                          } catch (e: any) {
                            message.error(e?.response?.data?.detail || String(e))
                          }
                        }

                        const handleChange = (newSlot: string) => {
                          if (!vt_id || newSlot === slot) return
                          Modal.confirm({
                            title: '确认修改归一结果？',
                            content: (
                              <div>
                                <div>字段：<Text code>{row.field}</Text></div>
                                <div>原归属：<Text code>{slot || '(无)'}</Text></div>
                                <div>新归属：<Text code>{newSlot}</Text></div>
                                {row.decision ? (
                                  <div style={{ color: '#fa8c16', marginTop: 8, fontSize: 12 }}>
                                    ⚠️ 该字段已审（{row.decision}），将覆盖之前的决策
                                  </div>
                                ) : null}
                              </div>
                            ),
                            okText: '确认',
                            cancelText: '取消',
                            onOk: () => submitDecision(newSlot),
                          })
                        }

                        const handleAcceptCurrent = () => {
                          if (!slot || !vt_id) return
                          Modal.confirm({
                            title: '确认当前归一结果？',
                            content: (
                              <div>
                                <div>字段：<Text code>{row.field}</Text></div>
                                <div>归属：<Text code>{slot}</Text></div>
                                <div style={{ color: '#999', marginTop: 8, fontSize: 12 }}>
                                  将写入审核记录，状态变为「已审 / manual」
                                </div>
                              </div>
                            ),
                            okText: '确认',
                            cancelText: '取消',
                            onOk: () => submitDecision(slot),
                          })
                        }

                        const isReviewed = !!row.decision

                        return (
                          <Space direction="vertical" size={2} style={{ width: '100%' }}>
                            <Space.Compact style={{ width: '100%', display: 'flex' }}>
                              <Select
                                size="small"
                                value={slot || undefined}
                                onChange={handleChange}
                                placeholder="选择归属槽位"
                                style={{ flex: 1, minWidth: 0 }}
                                showSearch
                                optionFilterProp="label"
                                optionLabelProp="value"
                                popupMatchSelectWidth={false}
                                options={[
                                  ...(row.top1_slot ? [{
                                    value: row.top1_slot,
                                    label: `${row.top1_slot}${typeof row.top1_score === 'number' ? ` · ${row.top1_score.toFixed(2)} (top1)` : ''}`,
                                  }] : []),
                                  ...(row.top2_slot && row.top2_slot !== row.top1_slot ? [{
                                    value: row.top2_slot,
                                    label: `${row.top2_slot}${typeof row.top2_score === 'number' ? ` · ${row.top2_score.toFixed(2)} (top2)` : ''}`,
                                  }] : []),
                                  ...slots
                                    .filter((s) => s.name !== row.top1_slot && s.name !== row.top2_slot)
                                    .map((s) => ({
                                      value: s.name,
                                      label: `${s.name}${s.cn_name ? ` · ${s.cn_name}` : ''} · ${s.role}`,
                                    })),
                                ]}
                              />
                              <Tooltip title={isReviewed ? '已确认（再次点击可覆盖）' : '确认当前归一结果'}>
                                <Button
                                  size="small"
                                  type={isReviewed ? 'default' : 'primary'}
                                  icon={<CheckOutlined />}
                                  disabled={!slot}
                                  onClick={handleAcceptCurrent}
                                />
                              </Tooltip>
                            </Space.Compact>
                            <Space size={4} style={{ fontSize: 10 }}>
                              <Tag color={color} style={{ fontSize: 9, margin: 0 }}>
                                {rs || row.match_reason || '-'}
                              </Tag>
                              {scoreStr && <Text type="secondary" style={{ fontSize: 10 }}>{scoreStr}</Text>}
                              {row.decision && (
                                <Tooltip title={`人工审核: ${row.decision} → ${row.decision_slot}`}>
                                  <Tag color="green" style={{ fontSize: 9, margin: 0 }}>已审</Tag>
                                </Tooltip>
                              )}
                            </Space>
                          </Space>
                        )
                      },
                    },
                    { title: 'field', dataIndex: 'field', width: 180 },
                    { title: 'type', dataIndex: 'type', width: 80 },
                    { title: 'comment', dataIndex: 'comment', width: 200 },
                    {
                      title: 'sample',
                      dataIndex: 'sample_data',
                      ellipsis: true,
                      render: (v: string) => (
                        <Text type="secondary" style={{ fontSize: 12 }}>
                          {v}
                        </Text>
                      ),
                    },
                    {
                      title: '操作',
                      width: 110,
                      render: (_: unknown, row: any) => (
                        <Popconfirm
                          title="剔除这个字段"
                          description={<>
                            <div>把 <Text code>{row.field}</Text> 加入全局黑名单（exact_name）</div>
                            <div style={{ color: '#999', fontSize: 12, marginTop: 4 }}>
                              提交后会自动在后台重跑 pipeline（约 11 分钟）让黑名单生效，可继续审核其他字段；右下角看进度
                            </div>
                          </>}
                          onConfirm={async () => {
                            try {
                              const resp = await api.addFieldBlacklist({
                                mode: 'exact_name',
                                value: row.field,
                                reason: `来自 VTEditor 源表 ${st.en}`,
                              })
                              if (!resp.added) {
                                message.info('已存在于黑名单')
                                return
                              }
                              // 自动触发后台 pipeline 重跑让黑名单生效
                              try {
                                const job = await api.startPipelineJob('field_features')
                                message.success(
                                  `已加入黑名单: ${row.field}，后台正在重跑 pipeline（${job.job_id}），可继续审核`,
                                  6,
                                )
                              } catch (jobErr: any) {
                                if (jobErr?.response?.status === 409) {
                                  message.warning(
                                    `已加入黑名单: ${row.field}。已有 pipeline 在跑，待该任务结束后再剔除任意字段会自动触发新一轮重跑（包含本次变更）`,
                                    8,
                                  )
                                } else {
                                  message.error(
                                    `已加入黑名单，但启动 pipeline 失败: ${jobErr?.response?.data?.detail || String(jobErr)}。请到 Pipeline Jobs 页手动重跑 --from field_features`,
                                    8,
                                  )
                                }
                              }
                            } catch (e: any) {
                              message.error(e?.response?.data?.detail || String(e))
                            }
                          }}
                        >
                          <Button size="small" danger icon={<StopOutlined />}>剔除</Button>
                        </Popconfirm>
                      ),
                    },
                  ]}
                />
              ),
            }
          })}
        />
                </>
              )
            })(),
          },
          {
            key: 'normalization-review',
            label: (
              <Space>
                <span>归一审核</span>
                <Tooltip title="本 VT 下所有字段的 I-04 归一结果 + 行内审核。提交 accept_top1 / use_top2 / use_top3 / skip 等决策，写入 field_normalization_reviewed.parquet">
                  <Text type="secondary" style={{ fontSize: 11 }}>ⓘ</Text>
                </Tooltip>
              </Space>
            ),
            children: (
              <Space direction="vertical" style={{ width: '100%' }} size={8}>
                <Space>
                  <Text>状态：</Text>
                  <Radio.Group value={normStatusFilter} onChange={(e) => setNormStatusFilter(e.target.value)} size="small">
                    <Radio.Button value="">全部</Radio.Button>
                    <Radio.Button value="conflict">⚠️ 冲突</Radio.Button>
                    <Radio.Button value="low_confidence">🔴 低置信</Radio.Button>
                    <Radio.Button value="needs_review">🟡 待审</Radio.Button>
                    <Radio.Button value="auto_accepted">🟢 自动通过</Radio.Button>
                  </Radio.Group>
                  <Checkbox
                    checked={normOnlyUnreviewed}
                    onChange={(e) => setNormOnlyUnreviewed(e.target.checked)}
                  >
                    只看未审
                  </Checkbox>
                  <Input.Search
                    placeholder="搜索 field / comment / table"
                    allowClear
                    size="small"
                    style={{ width: 240 }}
                    value={normKeyword}
                    onChange={(e) => setNormKeyword(e.target.value)}
                  />
                  <Button size="small" icon={<ReloadOutlined />} onClick={reloadNormItems} loading={normLoading}>刷新</Button>
                  <Text type="secondary">共 {normItems.length} 条</Text>
                </Space>
                <Table
                  rowKey={(r: any) => `${r.table_en}|${r.field_name}`}
                  size="small"
                  loading={normLoading}
                  dataSource={normItems}
                  pagination={{ pageSize: 50 }}
                  scroll={{ x: 'max-content' }}
                  columns={[
                    {
                      title: '字段',
                      width: 220,
                      render: (_: unknown, r: any) => {
                        const tableCn = vt.source_tables_with_fields.find((s) => s.en === r.table_en)?.cn || ''
                        const samples = Array.isArray(r.sample_values) && r.sample_values.length > 0
                          ? r.sample_values.slice(0, 5).join(' | ')
                          : ''
                        const tooltipContent = (
                          <div>
                            <div>table: {r.table_en}</div>
                            {tableCn ? <div style={{ marginTop: 2 }}>中文: {tableCn}</div> : null}
                            {samples ? <div style={{ marginTop: 4 }}>样例: {samples}</div> : null}
                          </div>
                        )
                        return (
                          <Tooltip title={tooltipContent}>
                            <Space direction="vertical" size={0}>
                              <Space size={6}>
                                <Text code style={{ fontSize: 12 }}>{r.field_name}</Text>
                                {r.decision ? (
                                  <Tag color="green" style={{ fontSize: 9, margin: 0 }}>已审 → {r.decision_slot}</Tag>
                                ) : null}
                              </Space>
                              <Text type="secondary" style={{ fontSize: 11 }}>{r.field_comment || '-'}</Text>
                            </Space>
                          </Tooltip>
                        )
                      },
                    },
                    {
                      title: '状态',
                      dataIndex: 'review_status',
                      width: 90,
                      render: (v: string) => {
                        const map: Record<string, { color: string; label: string }> = {
                          auto_accepted: { color: 'green', label: '🟢 自动' },
                          needs_review: { color: 'gold', label: '🟡 待审' },
                          conflict: { color: 'magenta', label: '⚠️ 冲突' },
                          low_confidence: { color: 'red', label: '🔴 低置信' },
                        }
                        const m = map[v] || { color: 'default', label: v }
                        return <Tag color={m.color} style={{ fontSize: 10, margin: 0 }}>{m.label}</Tag>
                      },
                    },
                    {
                      title: 'Top1',
                      width: 200,
                      render: (_: unknown, r: any) => (
                        <Space size={4}>
                          <Text code style={{ fontSize: 11, fontWeight: 600 }}>{r.top1_slot || '-'}</Text>
                          {typeof r.top1_score === 'number' && (
                            <Tag color="blue" style={{ fontSize: 10, margin: 0 }}>{r.top1_score.toFixed(3)}</Tag>
                          )}
                        </Space>
                      ),
                    },
                    {
                      title: 'Top2 / Top3',
                      width: 280,
                      render: (_: unknown, r: any) => (
                        <Space direction="vertical" size={2}>
                          {r.top2_slot && (
                            <Space size={4}>
                              <Text type="secondary" style={{ fontSize: 10 }}>2</Text>
                              <Text code style={{ fontSize: 11 }}>{r.top2_slot}</Text>
                              {typeof r.top2_score === 'number' && (
                                <Text type="secondary" style={{ fontSize: 10 }}>{r.top2_score.toFixed(3)}</Text>
                              )}
                            </Space>
                          )}
                          {r.top3_slot && (
                            <Space size={4}>
                              <Text type="secondary" style={{ fontSize: 10 }}>3</Text>
                              <Text code style={{ fontSize: 11 }}>{r.top3_slot}</Text>
                              {typeof r.top3_score === 'number' && (
                                <Text type="secondary" style={{ fontSize: 10 }}>{r.top3_score.toFixed(3)}</Text>
                              )}
                            </Space>
                          )}
                          {!r.top2_slot && !r.top3_slot && <Text type="secondary">-</Text>}
                        </Space>
                      ),
                    },
                    {
                      title: '操作',
                      width: 240,
                      render: (_: unknown, r: any) => (
                        <Space size={4}>
                          <Popconfirm
                            title={`接受 top1: ${r.top1_slot}？`}
                            onConfirm={async () => {
                              try {
                                await api.normDecision({
                                  table_en: r.table_en,
                                  field_name: r.field_name,
                                  vt_id: r.vt_id,
                                  decision: 'accept_top1',
                                  selected_slot: r.top1_slot,
                                })
                                message.success('已接受 top1')
                                const cands = findBatchCandidates(r, 'top1_slot')
                                await askBatchAndApply(
                                  cands,
                                  (it) => ({
                                    table_en: it.table_en,
                                    field_name: it.field_name,
                                    vt_id: it.vt_id,
                                    decision: 'accept_top1',
                                    selected_slot: it.top1_slot,
                                  }),
                                  '接受 top1',
                                )
                                reloadNormItems()
                                reloadVT()
                              } catch (e: any) {
                                message.error(e?.response?.data?.detail || String(e))
                              }
                            }}
                          >
                            <Button size="small" type="primary">accept top1</Button>
                          </Popconfirm>
                          {r.top2_slot && (
                            <Popconfirm
                              title={`改用 top2: ${r.top2_slot}？`}
                              onConfirm={async () => {
                                try {
                                  await api.normDecision({
                                    table_en: r.table_en,
                                    field_name: r.field_name,
                                    vt_id: r.vt_id,
                                    decision: 'use_top2',
                                    selected_slot: r.top2_slot,
                                  })
                                  message.success('已改用 top2')
                                  const cands = findBatchCandidates(r, 'top2_slot')
                                  await askBatchAndApply(
                                    cands,
                                    (it) => ({
                                      table_en: it.table_en,
                                      field_name: it.field_name,
                                      vt_id: it.vt_id,
                                      decision: 'use_top2',
                                      selected_slot: it.top2_slot!,
                                    }),
                                    '改用 top2',
                                  )
                                  reloadNormItems()
                                  reloadVT()
                                } catch (e: any) {
                                  message.error(e?.response?.data?.detail || String(e))
                                }
                              }}
                            >
                              <Button size="small">top2</Button>
                            </Popconfirm>
                          )}
                          <Popconfirm
                            title="跳过该字段（标为 skip）？"
                            onConfirm={async () => {
                              try {
                                await api.normDecision({
                                  table_en: r.table_en,
                                  field_name: r.field_name,
                                  vt_id: r.vt_id,
                                  decision: 'skip',
                                })
                                message.success('已跳过')
                                // skip 同步候选：同 field_name + 同 top1_slot + 未审
                                const cands = findBatchCandidates(r, 'top1_slot')
                                await askBatchAndApply(
                                  cands,
                                  (it) => ({
                                    table_en: it.table_en,
                                    field_name: it.field_name,
                                    vt_id: it.vt_id,
                                    decision: 'skip',
                                  }),
                                  '跳过',
                                )
                                reloadNormItems()
                                reloadVT()
                              } catch (e: any) {
                                message.error(e?.response?.data?.detail || String(e))
                              }
                            }}
                          >
                            <Button size="small">skip</Button>
                          </Popconfirm>
                          <Button size="small" type="link" onClick={() => setReviewDrawerItem(r as NormalizationItem)}>详情</Button>
                        </Space>
                      ),
                    },
                  ]}
                />
                <NormReviewDrawer
                  item={reviewDrawerItem}
                  vtSlots={vt.slots || []}
                  onClose={() => setReviewDrawerItem(null)}
                  onSubmitted={async (submittedItem, decision, decisionSlot) => {
                    setReviewDrawerItem(null)
                    // 批量同步候选：仅对 accept_top1/use_top2/use_top3/skip 这类有"top 推荐相同"含义的决策提示
                    let slotKey: 'top1_slot' | 'top2_slot' | 'top3_slot' | null = null
                    if (decision === 'accept_top1') slotKey = 'top1_slot'
                    else if (decision === 'use_top2') slotKey = 'top2_slot'
                    else if (decision === 'use_top3') slotKey = 'top3_slot'
                    else if (decision === 'skip') slotKey = 'top1_slot'
                    if (slotKey) {
                      const cands = findBatchCandidates(submittedItem, slotKey)
                      const sk = slotKey
                      await askBatchAndApply(
                        cands,
                        (it) => ({
                          table_en: it.table_en,
                          field_name: it.field_name,
                          vt_id: it.vt_id,
                          decision,
                          ...(decision !== 'skip' ? { selected_slot: it[sk] || decisionSlot || undefined } : {}),
                        }),
                        decision === 'skip' ? '跳过' : `应用决策 ${decision}`,
                      )
                    }
                    reloadNormItems()
                    reloadVT()
                  }}
                />
              </Space>
            ),
          },
        ]}
      />
      </Card>

      <Modal
        title={editing?.name ? `编辑槽位：${editing.name}` : '新增槽位'}
        open={!!editing}
        onOk={handleEditorOk}
        onCancel={() => setEditing(null)}
        width={720}
        destroyOnClose
      >
        <Form form={form} layout="vertical">
          <Form.Item
            label="From"
            name="from"
            rules={[{ required: true }]}
            extra="base = 复用基础槽位库；extended = 本 VT 独有新建"
          >
            <Select
              options={[
                { value: 'base', label: 'base（复用基础槽位）' },
                { value: 'extended', label: 'extended（本 VT 新建）' },
              ]}
              onChange={(v) => {
                if (v === 'base') {
                  form.setFieldValue('name', undefined)
                }
              }}
            />
          </Form.Item>

          <Form.Item
            noStyle
            shouldUpdate={(prev, next) => prev.from !== next.from}
          >
            {({ getFieldValue }) =>
              getFieldValue('from') === 'base' ? (
                <Form.Item label="Name (从 base_slots 选)" name="name" rules={[{ required: true }]}>
                  <Select
                    showSearch
                    options={baseSlots.map((b) => ({
                      value: b.name,
                      label: `${b.name} · ${b.cn_name} [${b.role}]`,
                    }))}
                    filterOption={(input, option) =>
                      (option?.label as string).toLowerCase().includes(input.toLowerCase())
                    }
                  />
                </Form.Item>
              ) : (
                // extended 模式：先填中文名，再 AI 生成英文名
                <>
                  <Form.Item label="中文名" name="cn_name" rules={[{ required: true }]} tooltip="先填中文，再点「AI 生成英文名」">
                    <Input placeholder="例：出生日期" />
                  </Form.Item>
                  <Form.Item
                    label="Name (英文 snake_case)"
                    required
                    tooltip="可手填；或先填中文后点 AI 自动生成"
                  >
                    <Space.Compact style={{ width: '100%', display: 'flex' }}>
                      <Form.Item
                        noStyle
                        name="name"
                        rules={[
                          { required: true, message: '请填写英文名' },
                          { pattern: /^[a-z][a-z0-9_]*$/, message: 'snake_case，字母开头' },
                        ]}
                      >
                        <Input placeholder="e.g. date_of_birth" style={{ flex: 1 }} />
                      </Form.Item>
                      <Tooltip title="基于已填中文名 + 当前 VT 现有槽位，由 LLM 生成 snake_case 英文名">
                        <Button
                          icon={<ThunderboltOutlined />}
                          loading={suggestingName}
                          onClick={async () => {
                            const cn = form.getFieldValue('cn_name')
                            if (!cn || !String(cn).trim()) {
                              message.warning('请先填中文名')
                              return
                            }
                            setSuggestingName(true)
                            try {
                              const r = await api.llmSuggestSlotName({
                                vt_id: vt_id || undefined,
                                user_cn_name: String(cn).trim(),
                              })
                              form.setFieldValue('name', r.name)
                              if (r.reason) form.setFieldValue('llm_reason', r.reason)
                              // 把 AI 生成的 aliases 也填上（如果用户已有内容则不覆盖，仅 append 去重）
                              if (Array.isArray(r.aliases) && r.aliases.length > 0) {
                                const existing = String(form.getFieldValue('aliases') || '')
                                  .split(/[,，]/)
                                  .map((s) => s.trim())
                                  .filter(Boolean)
                                const merged = Array.from(new Set([...existing, ...r.aliases]))
                                form.setFieldValue('aliases', merged.join(', '))
                              }
                              if (r.duplicate_of_existing) {
                                message.warning(`AI 建议名 ${r.name} 与已有槽位重名，请手动调整`)
                              } else {
                                message.success(`AI 生成: ${r.name}（含 ${r.aliases?.length || 0} 个别名）`)
                              }
                            } catch (e: any) {
                              message.error(e?.response?.data?.detail || String(e))
                            } finally {
                              setSuggestingName(false)
                            }
                          }}
                        >
                          AI 生成
                        </Button>
                      </Tooltip>
                    </Space.Compact>
                  </Form.Item>
                </>
              )
            }
          </Form.Item>

          <Form.Item label="Role" name="role" rules={[{ required: true }]}>
            <Select
              options={ROLE_OPTIONS.map((r) => ({ value: r, label: r }))}
            />
          </Form.Item>

          <Form.Item
            noStyle
            shouldUpdate={(prev, next) => prev.from !== next.from}
          >
            {({ getFieldValue }) =>
              getFieldValue('from') === 'extended' && (
                <>
                  <Form.Item label="Logical Type" name="logical_type">
                    <Input placeholder="e.g. status_code, amount, custom_xxx" />
                  </Form.Item>
                  <Form.Item
                    label="Aliases（逗号分隔，至少 3 个）"
                    name="aliases"
                    extra="例：出入境方向, 进出方向, fxbs"
                  >
                    <Input.TextArea rows={2} />
                  </Form.Item>
                  <Form.Item label="LLM reason (可选)" name="llm_reason">
                    <Input.TextArea rows={2} />
                  </Form.Item>
                </>
              )
            }
          </Form.Item>
        </Form>
      </Modal>

      {/* I-14: 元信息编辑 */}
      <Modal
        title="编辑 VT 元信息"
        open={metaEditOpen}
        onOk={handleMetaSave}
        onCancel={() => setMetaEditOpen(false)}
        width={640}
        destroyOnClose
      >
        <Form form={metaForm} layout="vertical">
          <Form.Item name="topic" label="topic（主题）" rules={[{ required: true }]}>
            <Input placeholder="例：人员主档" />
          </Form.Item>
          <Form.Item name="grain_desc" label="grain_desc（粒度描述）">
            <Input.TextArea autoSize={{ minRows: 2, maxRows: 4 }} placeholder="例：每人一行..." />
          </Form.Item>
          <Form.Item name="table_type" label="table_type（表类型）" rules={[{ required: true }]}>
            <Select
              options={[
                { value: '主档', label: '主档' },
                { value: '关系', label: '关系' },
                { value: '事件', label: '事件' },
                { value: '聚合', label: '聚合' },
                { value: '标签', label: '标签' },
                { value: '待定', label: '待定' },
              ]}
            />
          </Form.Item>
          <Form.Item name="l_path" label="L1 / L2 分类" rules={[{ required: true }]}>
            <Cascader
              options={categories.map((c) => ({
                value: c.name,
                label: c.name,
                children: c.children.map((ch) => ({ value: ch.name, label: ch.name })),
              }))}
              placeholder="选择分类"
              changeOnSelect
              showSearch={{
                filter: (input, path) => path.some((p) => String(p.label).toLowerCase().includes(input.toLowerCase())),
              }}
            />
          </Form.Item>
        </Form>
      </Modal>

      {/* 槽位合并 Modal */}
      <Modal
        title={mergeSource ? `合并槽位: ${mergeSource.name}` : ''}
        open={!!mergeSource}
        onOk={() => {
          if (mergeSource && mergeTargetName) {
            handleMerge(mergeTargetName, mergeSource.name)
          }
        }}
        okButtonProps={{ disabled: !mergeTargetName }}
        okText="合并"
        onCancel={() => { setMergeSource(null); setMergeTargetName(undefined) }}
        destroyOnClose
      >
        <Alert
          type="info"
          showIcon
          style={{ marginBottom: 12 }}
          message={<>
            <div>将 <Text code>{mergeSource?.name}</Text>（A）合并到下面选中的槽位（B）</div>
            <div style={{ marginTop: 4 }}>B 为主保留；A 的 name / cn_name / aliases 会并入 B 的 aliases；A 从清单移除</div>
          </>}
        />
        <Form.Item label="选择合并目标（B，保留主）" required>
          <Select
            showSearch
            optionFilterProp="label"
            value={mergeTargetName}
            onChange={setMergeTargetName}
            placeholder="选择本 VT 里的另一个槽位"
            options={slots
              .filter((s) => s.name !== mergeSource?.name)
              .map((s) => ({
                value: s.name,
                label: `${s.name}${s.cn_name ? ` · ${s.cn_name}` : ''} [${s.from} / ${s.role}]`,
              }))}
            style={{ width: '100%' }}
          />
        </Form.Item>
        {mergeSource && mergeTargetName && (() => {
          const target = slots.find((s) => s.name === mergeTargetName)
          if (!target) return null
          const aSet = new Set<string>([
            mergeSource.name,
            mergeSource.cn_name || '',
            ...(mergeSource.aliases || []),
          ].filter(Boolean))
          const bSet = new Set<string>(target.aliases || [])
          const preview = new Set<string>([...aSet, ...bSet])
          return (
            <Alert
              type="success"
              showIcon
              message="合并后 B 的 aliases 预览"
              description={
                <Space size={[0, 4]} wrap>
                  {Array.from(preview).map((a) => (
                    <Tag key={a} color={aSet.has(a) && !bSet.has(a) ? 'orange' : 'default'}>{a}</Tag>
                  ))}
                  <Text type="secondary" style={{ fontSize: 11 }}>（橙色=从 A 带入）</Text>
                </Space>
              }
            />
          )
        })()}
      </Modal>

      {/* 相似槽位检查 Modal */}
      <Modal
        title="相似槽位检查（本 VT 内）"
        open={similarityOpen}
        onCancel={() => setSimilarityOpen(false)}
        footer={<Button onClick={() => setSimilarityOpen(false)}>关闭</Button>}
        width={820}
        destroyOnClose
      >
        <Alert
          type="info"
          showIcon
          style={{ marginBottom: 12 }}
          message="比对方式：槽位的 name + cn_name + aliases 分词后计算 Jaccard 相似度。相似度 ≥ 0.2 的对会列出。"
        />
        {(() => {
          const pairs = computeSimilarPairs()
          if (pairs.length === 0) {
            return <Alert type="success" showIcon message="本 VT 下没有相似度 ≥ 0.2 的槽位对，看起来没有冗余" />
          }
          return (
            <Table
              size="small"
              rowKey={(p) => `${p.a.name}|${p.b.name}`}
              dataSource={pairs}
              pagination={{ pageSize: 20 }}
              columns={[
                {
                  title: '槽位 A',
                  width: 200,
                  render: (_: unknown, r: any) => (
                    <Space direction="vertical" size={0}>
                      <Text code style={{ fontSize: 12 }}>{r.a.name}</Text>
                      <Text type="secondary" style={{ fontSize: 11 }}>
                        {r.a.cn_name || '-'} · {r.a.from}
                      </Text>
                    </Space>
                  ),
                },
                {
                  title: '槽位 B',
                  width: 200,
                  render: (_: unknown, r: any) => (
                    <Space direction="vertical" size={0}>
                      <Text code style={{ fontSize: 12 }}>{r.b.name}</Text>
                      <Text type="secondary" style={{ fontSize: 11 }}>
                        {r.b.cn_name || '-'} · {r.b.from}
                      </Text>
                    </Space>
                  ),
                },
                {
                  title: '相似度',
                  dataIndex: 'sim',
                  width: 90,
                  render: (v: number) => (
                    <Tag color={v >= 0.5 ? 'red' : v >= 0.35 ? 'orange' : 'gold'}>{v.toFixed(2)}</Tag>
                  ),
                },
                {
                  title: '共同 token',
                  dataIndex: 'common',
                  render: (v: string[]) => (
                    <Space size={[0, 2]} wrap>
                      {v.slice(0, 8).map((t) => <Tag key={t} style={{ fontSize: 10 }}>{t}</Tag>)}
                      {v.length > 8 && <Text type="secondary" style={{ fontSize: 10 }}>+{v.length - 8}</Text>}
                    </Space>
                  ),
                },
                {
                  title: '操作',
                  width: 180,
                  render: (_: unknown, r: any) => (
                    <Space size={4}>
                      <Button
                        size="small"
                        onClick={() => {
                          setMergeSource(r.a)
                          setMergeTargetName(r.b.name)
                          setSimilarityOpen(false)
                        }}
                      >
                        A → B
                      </Button>
                      <Button
                        size="small"
                        onClick={() => {
                          setMergeSource(r.b)
                          setMergeTargetName(r.a.name)
                          setSimilarityOpen(false)
                        }}
                      >
                        B → A
                      </Button>
                    </Space>
                  ),
                },
              ]}
            />
          )
        })()}
      </Modal>

      {/* I-16: LLM 扩展槽位结果选择 */}
      <Modal
        title={`LLM 扩展槽位建议${extendResult ? ` · ${extendResult.new_slots?.length || 0} 个候选` : ''}`}
        open={!!extendResult}
        onCancel={() => { setExtendResult(null); setExtendSelectedKeys([]) }}
        width="min(1280px, 95vw)"
        footer={[
          <Button key="cancel" onClick={() => { setExtendResult(null); setExtendSelectedKeys([]) }}>取消</Button>,
          <Button
            key="apply"
            type="primary"
            disabled={extendSelectedKeys.length === 0}
            onClick={handleApplyExtendSelected}
          >
            合并 {extendSelectedKeys.length} 个到槽位表
          </Button>,
        ]}
      >
        {extendResult && (
          <Space direction="vertical" style={{ width: '100%' }} size={12}>
            <Alert
              type="info"
              showIcon
              message={
                <Space wrap>
                  <span>字段总数 <strong>{extendResult.total_fields}</strong></span>
                  <span>已覆盖 <strong style={{ color: '#52c41a' }}>{extendResult.covered_count}</strong></span>
                  <span>未覆盖 <strong style={{ color: '#faad14' }}>{extendResult.uncovered_count}</strong></span>
                  {typeof extendResult.skipped_empty === 'number' && extendResult.skipped_empty > 0 && (
                    <span>空样例过滤 <strong style={{ color: '#999' }}>{extendResult.skipped_empty}</strong></span>
                  )}
                  {typeof extendResult.skipped_noise === 'number' && extendResult.skipped_noise > 0 && (
                    <span>技术字段过滤 <strong style={{ color: '#999' }}>{extendResult.skipped_noise}</strong></span>
                  )}
                  {typeof extendResult.reviewed_seed_count === 'number' && extendResult.reviewed_seed_count > 0 && (
                    <Tooltip
                      title={
                        <div>
                          <div style={{ marginBottom: 4 }}>这些人工已审字段已作为 seed 告知 LLM（不会重复建槽位）：</div>
                          {(extendResult.reviewed_seed_sample || []).map((f: any) => (
                            <div key={f.field_name} style={{ fontSize: 11 }}>
                              • <Text code style={{ fontSize: 11 }}>{f.field_name}</Text>
                              {f.comment ? ` — ${f.comment}` : ''}
                            </div>
                          ))}
                          {extendResult.reviewed_seed_count > (extendResult.reviewed_seed_sample?.length || 0) && (
                            <div style={{ fontSize: 11, color: '#999', marginTop: 4 }}>
                              … 共 {extendResult.reviewed_seed_count} 条，仅展示 {extendResult.reviewed_seed_sample?.length || 0} 条
                            </div>
                          )}
                        </div>
                      }
                    >
                      <span style={{ cursor: 'help' }}>
                        已审 seed <strong style={{ color: '#1677ff' }}>{extendResult.reviewed_seed_count}</strong>
                      </span>
                    </Tooltip>
                  )}
                  <span>耗时 {extendResult.elapsed_sec}s{extendResult.elapsed_sec < 0.5 ? '（缓存）' : ''}</span>
                </Space>
              }
              description={extendResult.summary}
            />
            {extendResult.warnings && extendResult.warnings.length > 0 && (
              <Alert type="warning" showIcon message="警告" description={extendResult.warnings.join('；')} />
            )}
            {extendResult.new_slots.length === 0 ? (
              <Alert type="success" showIcon message="LLM 认为现有槽位已足够，或未覆盖字段都是噪声/自由文本" />
            ) : (
              <Table
                rowKey="name"
                size="small"
                rowSelection={{
                  selectedRowKeys: extendSelectedKeys,
                  onChange: (keys) => setExtendSelectedKeys(keys as string[]),
                }}
                dataSource={extendResult.new_slots}
                pagination={false}
                scroll={{ y: 480 }}
                columns={[
                  {
                    title: '槽位',
                    width: 220,
                    render: (_: unknown, r: any) => (
                      <Space direction="vertical" size={0}>
                        <Text code style={{ fontSize: 11 }}>{r.name}</Text>
                        <Text type="secondary" style={{ fontSize: 11 }}>{r.cn_name}</Text>
                      </Space>
                    ),
                  },
                  {
                    title: '来源',
                    width: 80,
                    render: (_: unknown, r: any) => (
                      r.source === 'base' ? (
                        <Tooltip title="复用已有 base 槽位（不新建）">
                          <Tag color="blue" style={{ fontSize: 10, margin: 0 }}>base</Tag>
                        </Tooltip>
                      ) : (
                        <Tooltip title="VT 独有新 extended 槽位">
                          <Tag color="gold" style={{ fontSize: 10, margin: 0 }}>extended</Tag>
                        </Tooltip>
                      )
                    ),
                  },
                  {
                    title: 'role / type',
                    width: 130,
                    render: (_: unknown, r: any) => (
                      <Space direction="vertical" size={0}>
                        <Tag style={{ fontSize: 10, margin: 0 }}>{r.role}</Tag>
                        <Text type="secondary" style={{ fontSize: 10 }}>{r.logical_type}</Text>
                      </Space>
                    ),
                  },
                  {
                    title: 'aliases',
                    dataIndex: 'aliases',
                    width: 220,
                    render: (v: string[]) => (
                      <Tooltip title={v.join(' / ')}>
                        <Space size={[0, 2]} wrap>
                          {v.slice(0, 3).map((a) => <Tag key={a} style={{ fontSize: 10 }}>{a}</Tag>)}
                          {v.length > 3 && <Text type="secondary" style={{ fontSize: 10 }}>+{v.length - 3}</Text>}
                        </Space>
                      </Tooltip>
                    ),
                  },
                  {
                    title: '覆盖字段',
                    dataIndex: 'covers_fields',
                    width: 180,
                    render: (v: string[]) => (
                      <Tooltip title={(v || []).join(' / ')}>
                        <Space size={[0, 2]} wrap>
                          {v?.slice(0, 3).map((f) => <Tag key={f} color="blue" style={{ fontSize: 10 }}>{f}</Tag>)}
                          {v && v.length > 3 && <Text type="secondary" style={{ fontSize: 10 }}>+{v.length - 3}</Text>}
                        </Space>
                      </Tooltip>
                    ),
                  },
                  {
                    title: '理由',
                    dataIndex: 'llm_reason',
                    ellipsis: { showTitle: false },
                    render: (v) => (
                      <Tooltip title={v} styles={{ root: { maxWidth: 420 } }}>
                        <Text type="secondary" style={{ fontSize: 11 }}>{v}</Text>
                      </Tooltip>
                    ),
                  },
                  {
                    title: '操作',
                    width: 110,
                    render: (_: unknown, r: any) => r.source === 'extended' ? (
                      <Popconfirm
                        title={`把 "${r.name}" 升级为 base 槽位？`}
                        description="该条目会写进 data/slot_library/base_slots.yaml，之后所有 VT 都能复用；同时会从本次候选里替换为 source=base"
                        onConfirm={async () => {
                          try {
                            await api.promoteExtendedToBase({
                              name: r.name,
                              cn_name: r.cn_name,
                              role: r.role,
                              logical_type: r.logical_type,
                              description: r.llm_reason,
                              aliases: r.aliases,
                              applicable_table_types: r.applicable_table_types,
                            })
                            message.success(`✅ 已升级 ${r.name} 为 base；刷新 base_slots.yaml`)
                            // 本地把该候选 source 切成 base（保持选中）
                            setExtendResult((prev: any) => ({
                              ...prev,
                              new_slots: prev.new_slots.map((x: any) =>
                                x.name === r.name ? { ...x, source: 'base', from: 'base' } : x,
                              ),
                            }))
                          } catch (e: any) {
                            message.error(e?.response?.data?.detail || String(e))
                          }
                        }}
                      >
                        <Button size="small" type="link">提升为 base</Button>
                      </Popconfirm>
                    ) : <Text type="secondary" style={{ fontSize: 10 }}>—</Text>,
                  },
                ]}
              />
            )}
            {extendResult.skipped_reason && Object.keys(extendResult.skipped_reason).length > 0 && (
              <details>
                <summary style={{ cursor: 'pointer', fontSize: 12, color: '#999' }}>
                  LLM 决定不建槽位的字段（{Object.keys(extendResult.skipped_reason).length}）
                </summary>
                <div style={{ marginTop: 8, maxHeight: 160, overflow: 'auto' }}>
                  {Object.entries(extendResult.skipped_reason).map(([f, r]) => (
                    <div key={f} style={{ fontSize: 11 }}>
                      <Text code style={{ fontSize: 10 }}>{f}</Text>: {r as string}
                    </div>
                  ))}
                </div>
              </details>
            )}
          </Space>
        )}
      </Modal>

      {/* I-14: 添加源表 */}
      <Modal
        title="添加源表"
        open={addSourceOpen}
        onCancel={() => { setAddSourceOpen(false); setAddSourceSearch('') }}
        footer={null}
        width={720}
      >
        <Input.Search
          placeholder="搜索表名或中文名"
          value={addSourceSearch}
          onChange={(e) => setAddSourceSearch(e.target.value)}
          style={{ marginBottom: 12 }}
          allowClear
        />
        <Table
          size="small"
          rowKey="en"
          pagination={{ pageSize: 10 }}
          dataSource={allTables.filter((t) => {
            if (!addSourceSearch) return true
            const kw = addSourceSearch.toLowerCase()
            return t.en.toLowerCase().includes(kw) || (t.cn || '').toLowerCase().includes(kw)
          })}
          columns={[
            {
              title: '表',
              dataIndex: 'en',
              render: (v: string, r: any) => (
                <Space direction="vertical" size={0}>
                  <Text code style={{ fontSize: 11 }}>{v}</Text>
                  {r.cn && <Text type="secondary" style={{ fontSize: 11 }}>{r.cn}</Text>}
                </Space>
              ),
            },
            { title: '字段', dataIndex: 'field_count', width: 70, render: (v) => <Tag>{v}</Tag> },
            {
              title: '',
              width: 80,
              render: (_: unknown, r: any) => {
                const alreadyIn = vt?.source_tables_with_fields.some((s) => s.en === r.en)
                return alreadyIn ? (
                  <Tag color="green">已在</Tag>
                ) : (
                  <Button size="small" type="primary" onClick={() => { handleAddSource(r.en, r.cn); setAddSourceOpen(false) }}>
                    添加
                  </Button>
                )
              },
            },
          ]}
        />
      </Modal>

      {/* 黑名单扫描结果 Modal */}
      <Modal
        title="黑名单扫描结果"
        open={scanModalOpen}
        onCancel={() => setScanModalOpen(false)}
        width={960}
        footer={[
          <Button key="cancel" onClick={() => setScanModalOpen(false)}>取消</Button>,
          <Button
            key="apply"
            type="primary"
            danger
            loading={applyingBlacklist}
            disabled={scanSelectedFields.length === 0}
            onClick={handleBatchBlacklistApply}
          >
            批量踢除 {scanSelectedFields.length}
          </Button>,
        ]}
      >
        {blacklistScanResult && (
          <>
            <div style={{ marginBottom: 12 }}>
              <Space>
                <Text type="secondary">
                  扫描 {blacklistScanResult.total_scanned} 字段，命中 {blacklistScanResult.matched}
                </Text>
                <Text type="secondary">
                  · 勾选字段将加入全局黑名单 <Text code>exact_names</Text>（已在的会自动跳过）
                </Text>
              </Space>
            </div>
            <Table
              size="small"
              rowKey={(r: any) => `${r.table_en}||${r.field_name}`}
              dataSource={blacklistScanResult.candidates}
              pagination={{ pageSize: 50, size: 'small' }}
              rowSelection={{
                selectedRowKeys: blacklistScanResult.candidates
                  .filter((c) => scanSelectedFields.includes(c.field_name))
                  .map((c) => `${c.table_en}||${c.field_name}`),
                onChange: (keys) => {
                  // key 格式：table_en||field_name，取出 field_name 去重（同名跨表合并）
                  const names = new Set<string>()
                  keys.forEach((k) => {
                    const fn = String(k).split('||')[1]
                    if (fn) names.add(fn)
                  })
                  setScanSelectedFields(Array.from(names))
                },
                getCheckboxProps: (r: any) => ({
                  // 已在 exact_names 的默认不选，但允许用户手动勾（接口会自动跳过重复）
                  disabled: false,
                }),
              }}
              columns={[
                {
                  title: '字段',
                  dataIndex: 'field_name',
                  width: 180,
                  render: (v: string, r: any) => (
                    <Space direction="vertical" size={0}>
                      <Text strong code>{v}</Text>
                      {r.already_in_exact && (
                        <Tag color="default" style={{ fontSize: 10 }}>已在 exact_names</Tag>
                      )}
                    </Space>
                  ),
                },
                {
                  title: '表',
                  dataIndex: 'table_en',
                  width: 240,
                  ellipsis: true,
                  render: (v: string) => <Text type="secondary" style={{ fontSize: 12 }}>{v}</Text>,
                },
                {
                  title: '注释',
                  dataIndex: 'field_comment',
                  width: 140,
                  ellipsis: true,
                },
                {
                  title: '匹配规则',
                  dataIndex: 'match_reason',
                  width: 180,
                  render: (v: string, r: any) => {
                    const color =
                      r.match_type === 'exact_already' ? 'default' :
                      r.match_type === 'hardcoded_name' ? 'red' :
                      r.match_type === 'pattern' ? 'orange' :
                      r.match_type === 'pair' ? 'purple' :
                      r.match_type === 'hardcoded_suffix' ? 'gold' : 'blue'
                    return <Tag color={color}>{v}</Tag>
                  },
                },
                {
                  title: '样例',
                  dataIndex: 'sample',
                  ellipsis: true,
                  render: (v: string) => (
                    <Text type="secondary" style={{ fontSize: 12 }}>{v || '—'}</Text>
                  ),
                },
              ]}
            />
          </>
        )}
      </Modal>
    </div>
  )
}

// ============================================================
// 归一审核详情抽屉：精细审核单个字段，支持 5 种决策 + 噪声剔除
// ============================================================

type ReviewDecision = 'accept_top1' | 'use_top2' | 'use_top3' | 'use_slot' | 'mark_new_slot'

function NormReviewDrawer({
  item,
  vtSlots,
  onClose,
  onSubmitted,
}: {
  item: NormalizationItem | null
  vtSlots: Slot[]
  onClose: () => void
  onSubmitted: (item: NormalizationItem, decision: string, decisionSlot: string | null) => void | Promise<void>
}) {
  const [decision, setDecision] = useState<ReviewDecision>('accept_top1')
  const [selectedSlot, setSelectedSlot] = useState<string | undefined>(undefined)
  const [newSlotName, setNewSlotName] = useState<string>('')
  const [newSlotCnName, setNewSlotCnName] = useState<string>('')
  const [reviewerNote, setReviewerNote] = useState<string>('')
  const [submitting, setSubmitting] = useState(false)
  const [suggestingName, setSuggestingName] = useState(false)

  // 切换审核字段时重置表单（默认选 accept_top1）
  useEffect(() => {
    if (item) {
      setDecision('accept_top1')
      setSelectedSlot(undefined)
      setNewSlotName('')
      setNewSlotCnName('')
      setReviewerNote('')
    }
  }, [item?.table_en, item?.field_name, item?.vt_id])

  if (!item) {
    return <Drawer open={false} onClose={onClose} />
  }

  // 「归入其他已有槽位」候选：当前 VT 槽位清单，剔除 top1/2/3
  const topSet = new Set([item.top1_slot, item.top2_slot, item.top3_slot].filter(Boolean) as string[])
  const otherSlotOptions = vtSlots
    .filter((s) => !topSet.has(s.name))
    .map((s) => ({
      label: s.cn_name ? `${s.name} (${s.cn_name})` : s.name,
      value: s.name,
    }))

  const submit = async (overrideDecision?: 'mark_noise') => {
    if (!item) return
    const finalDecision = overrideDecision || decision
    // 校验
    if (finalDecision === 'use_slot' && !selectedSlot) {
      message.error('请选择要归入的槽位')
      return
    }
    if (finalDecision === 'mark_new_slot' && !newSlotName.trim()) {
      message.error('请填写新槽位英文名')
      return
    }

    const payload: Parameters<typeof api.normDecision>[0] = {
      table_en: item.table_en,
      field_name: item.field_name,
      vt_id: item.vt_id,
      decision: finalDecision,
      reviewer_note: reviewerNote || undefined,
    }
    if (finalDecision === 'accept_top1') payload.selected_slot = item.top1_slot || undefined
    if (finalDecision === 'use_top2') payload.selected_slot = item.top2_slot || undefined
    if (finalDecision === 'use_top3') payload.selected_slot = item.top3_slot || undefined
    if (finalDecision === 'use_slot') payload.selected_slot = selectedSlot
    if (finalDecision === 'mark_new_slot') {
      payload.new_slot_name = newSlotName.trim()
      payload.new_slot_cn_name = newSlotCnName.trim() || undefined
    }

    setSubmitting(true)
    try {
      const resp = await api.normDecision(payload)
      message.success(overrideDecision === 'mark_noise' ? '已标记为噪声' : '审核已提交')
      onSubmitted(item, finalDecision, resp?.decision_slot ?? null)
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    } finally {
      setSubmitting(false)
    }
  }

  const conflictTags = (item.conflict_types || []).map((ct) => (
    <Tag key={ct} color="orange">{ct}</Tag>
  ))

  return (
    <Drawer
      title={<span>审核：<Text code>{item.field_name}</Text></span>}
      open={!!item}
      onClose={onClose}
      width={720}
      destroyOnClose
      extra={
        <Space>
          <Button onClick={onClose}>取消</Button>
          <Popconfirm
            title="仅在本 VT 内将该字段标记为噪声（不归到任何槽位）。如需全局拉黑该字段名，请使用「按源表看字段」的「剔除」按钮。"
            onConfirm={() => submit('mark_noise')}
          >
            <Button danger loading={submitting}>标记为噪声（仅本 VT）</Button>
          </Popconfirm>
          <Button type="primary" loading={submitting} onClick={() => submit()}>提交</Button>
        </Space>
      }
    >
      {/* 字段元信息 */}
      <Descriptions column={2} bordered size="small">
        <Descriptions.Item label="字段名">{item.field_name}</Descriptions.Item>
        <Descriptions.Item label="类型">{item.data_type || '-'}</Descriptions.Item>
        <Descriptions.Item label="注释" span={2}>{item.field_comment || '-'}</Descriptions.Item>
        <Descriptions.Item label="物理表">{item.table_en}</Descriptions.Item>
        <Descriptions.Item label="L1 / L2">{[item.table_l1, item.table_l2].filter(Boolean).join(' / ') || '-'}</Descriptions.Item>
        <Descriptions.Item label="VT">{item.vt_id}</Descriptions.Item>
        <Descriptions.Item label="状态">
          <Tag color="magenta">{item.review_status}</Tag>
        </Descriptions.Item>
        <Descriptions.Item label="注释命中关键词" span={2}>
          {(item.comment_keywords || []).length
            ? (item.comment_keywords || []).map((k) => <Tag key={k} color="cyan">{k}</Tag>)
            : <Text type="secondary">-</Text>}
        </Descriptions.Item>
        <Descriptions.Item label="样例 pattern" span={2}>
          {(item.sample_patterns || []).length
            ? (item.sample_patterns || []).map((p) => <Tag key={p}>{p}</Tag>)
            : <Text type="secondary">-</Text>}
        </Descriptions.Item>
        <Descriptions.Item label="样例值" span={2}>
          {(item.sample_values || []).length ? (item.sample_values || []).slice(0, 8).join(' | ') : '-'}
        </Descriptions.Item>
        <Descriptions.Item label="冲突类型" span={2}>
          {conflictTags.length ? <Space wrap>{conflictTags}</Space> : <Text type="secondary">-</Text>}
        </Descriptions.Item>
      </Descriptions>

      <Divider orientation="left" plain style={{ marginTop: 24 }}>Top 3 候选槽位（含已映射种子字段）</Divider>
      <Space direction="vertical" style={{ width: '100%' }} size={8}>
        {([
          { rank: 'top1', slot: item.top1_slot, score: item.top1_score, color: '#52c41a', bg: '#f6ffed' },
          { rank: 'top2', slot: item.top2_slot, score: item.top2_score, color: '#fa8c16', bg: '#fff7e6' },
          { rank: 'top3', slot: item.top3_slot, score: item.top3_score, color: '#fa8c16', bg: '#fff7e6' },
        ] as const).map((t) => {
          if (!t.slot) return null
          const slotDef = vtSlots.find((s) => s.name === t.slot)
          const mfs = slotDef?.mapped_fields || []
          const cnName = slotDef?.cn_name || ''
          const role = slotDef?.role || ''
          const fromType = slotDef?.from || ''
          return (
            <div key={t.rank} style={{ background: t.bg, padding: '8px 12px', borderRadius: 4 }}>
              <Space size={12} style={{ marginBottom: mfs.length ? 6 : 0 }}>
                <Text type="secondary" style={{ fontSize: 12 }}>{t.rank}</Text>
                <Text code strong>{t.slot}</Text>
                {cnName && <Text type="secondary" style={{ fontSize: 12 }}>({cnName})</Text>}
                {fromType && <Tag style={{ fontSize: 10, margin: 0 }} color={fromType === 'base' ? 'blue' : 'gold'}>{fromType}</Tag>}
                {role && <Tag style={{ fontSize: 10, margin: 0 }}>{role}</Tag>}
                <Text style={{ color: t.color }}>{t.score?.toFixed(3)}</Text>
                <Text type="secondary" style={{ fontSize: 11 }}>
                  已映射 {mfs.length} 个种子字段
                </Text>
              </Space>
              {mfs.length > 0 && (
                <div style={{ marginTop: 4, paddingLeft: 20, fontSize: 11 }}>
                  {mfs.slice(0, 8).map((mf, i) => (
                    <div key={i} style={{ color: '#666' }}>
                      • <Text code style={{ fontSize: 10 }}>{mf.field_name}</Text>
                      {mf.field_comment && <Text type="secondary" style={{ fontSize: 11, marginLeft: 6 }}>{mf.field_comment}</Text>}
                      <Text type="secondary" style={{ fontSize: 10, marginLeft: 6 }}>· {String(mf.table_en || '').slice(0, 50)}</Text>
                    </div>
                  ))}
                  {mfs.length > 8 && (
                    <Text type="secondary" style={{ fontSize: 10 }}>… 还有 {mfs.length - 8} 条</Text>
                  )}
                </div>
              )}
              {mfs.length === 0 && (
                <Text type="secondary" style={{ fontSize: 11, marginLeft: 20 }}>（该槽位尚无种子，通过本次审核将第一次建立映射）</Text>
              )}
            </div>
          )
        })}
      </Space>

      <Divider orientation="left" plain style={{ marginTop: 24 }}>审核决策</Divider>
      <Form layout="vertical" size="small">
        <Form.Item label="决定" required>
          <Radio.Group value={decision} onChange={(e) => setDecision(e.target.value)}>
            <Space direction="vertical">
              <Radio value="accept_top1">接受 top1 ({item.top1_slot})</Radio>
              {item.top2_slot && <Radio value="use_top2">改用 top2 ({item.top2_slot})</Radio>}
              {item.top3_slot && <Radio value="use_top3">改用 top3 ({item.top3_slot})</Radio>}
              <Radio value="use_slot">归入其他已有槽位…</Radio>
              <Radio value="mark_new_slot">提议新槽位…</Radio>
            </Space>
          </Radio.Group>
        </Form.Item>

        {decision === 'use_slot' && (
          <Form.Item label="选择槽位" required>
            <Select
              showSearch
              placeholder="从当前 VT 槽位清单中选择（已排除 top1/2/3）"
              value={selectedSlot}
              onChange={setSelectedSlot}
              options={otherSlotOptions}
              style={{ width: '100%' }}
              optionFilterProp="label"
              notFoundContent={<Text type="secondary">无可选槽位（top1/2/3 已覆盖全部）</Text>}
            />
          </Form.Item>
        )}

        {decision === 'mark_new_slot' && (
          <>
            <Form.Item label="新槽位中文名" required tooltip="先填中文，再点下方「AI 生成英文名」自动产出 snake_case">
              <Input
                value={newSlotCnName}
                onChange={(e) => setNewSlotCnName(e.target.value)}
                placeholder="如 配偶证件号"
              />
            </Form.Item>
            <Form.Item
              label="新槽位英文名"
              required
              tooltip="可手填 snake_case；或填好中文后点「AI 生成英文名」自动产出"
            >
              <Space.Compact style={{ width: '100%', display: 'flex' }}>
                <Input
                  value={newSlotName}
                  onChange={(e) => setNewSlotName(e.target.value)}
                  placeholder="如 spouse_certificate_no"
                  style={{ flex: 1 }}
                />
                <Tooltip title={newSlotCnName ? '基于中文名 + DDL 自动生成 snake_case 英文名' : '基于字段 DDL 自动生成 snake_case 英文名 + 中文名'}>
                  <Button
                    icon={<ThunderboltOutlined />}
                    loading={suggestingName}
                    onClick={async () => {
                      if (!item) return
                      setSuggestingName(true)
                      try {
                        const r = await api.llmSuggestSlotName({
                          table_en: item.table_en,
                          field_name: item.field_name,
                          vt_id: item.vt_id,
                          user_cn_name: newSlotCnName || undefined,
                        })
                        setNewSlotName(r.name)
                        // 用户没填中文时由 AI 填；填了的话不覆盖
                        if (!newSlotCnName && r.cn_name) setNewSlotCnName(r.cn_name)
                        if (r.reason) {
                          setReviewerNote((prev) => {
                            const tag = `[AI 命名理由] ${r.reason}`
                            return prev ? `${prev}\n${tag}` : tag
                          })
                        }
                        if (r.duplicate_of_existing) {
                          message.warning(`AI 建议名 ${r.name} 与已有槽位重名，请手动调整`)
                        } else {
                          message.success(`AI 生成英文名: ${r.name}`)
                        }
                      } catch (e: any) {
                        message.error(e?.response?.data?.detail || String(e))
                      } finally {
                        setSuggestingName(false)
                      }
                    }}
                  >
                    AI 生成英文名
                  </Button>
                </Tooltip>
              </Space.Compact>
            </Form.Item>
          </>
        )}

        <Form.Item label="备注（可选）">
          <Input.TextArea
            value={reviewerNote}
            onChange={(e) => setReviewerNote(e.target.value)}
            rows={2}
            placeholder="审核理由 / 上下文说明"
          />
        </Form.Item>
      </Form>
    </Drawer>
  )
}
