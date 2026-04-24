import { useEffect, useMemo, useState } from 'react'
import {
  Alert,
  Badge,
  Button,
  Card,
  Collapse,
  Form,
  Input,
  InputNumber,
  message,
  Modal,
  Popconfirm,
  Select,
  Space,
  Switch,
  Table,
  Tabs,
  Tag,
  Tooltip,
  Typography,
} from 'antd'
import { QuestionCircleOutlined } from '@ant-design/icons'
import { api } from '../api'

const { Title, Text } = Typography
const { Panel } = Collapse

type Candidate = {
  candidate_id: string
  suggested_action: 'exact_name' | 'name_pattern' | 'table_field_pair'
  suggested_value: string
  field_name: string
  field_comment: string
  data_type: string
  usage_count: number
  example_tables: string[]
  affected_field_count: number
  score: number
  reasons: string[]
  llm_judgement?: string
  llm_confidence?: number
  llm_category?: string
  llm_reason?: string
  _row?: {
    selected: boolean
    action: 'exact_name' | 'name_pattern' | 'table_field_pair'
    value: string
  }
}

type CurrentField = {
  field_name: string
  field_comment: string
  data_type: string
  usage_count: number
  sample_values: string[]
  noise_reason: string
  whitelisted: boolean
}

type TableGroup = { table_en: string; table_cn: string; field_count: number; fields: CurrentField[] }
type L2Group = { l2: string; table_count: number; field_count: number; tables: TableGroup[] }
type L1Group = { l1: string; l2_count: number; field_count: number; l2_groups: L2Group[] }

const ACTION_OPTIONS = [
  { value: 'exact_name', label: 'exact_name（全库同名字段）' },
  { value: 'name_pattern', label: 'name_pattern（fnmatch 模式）' },
  { value: 'table_field_pair', label: 'table_field_pair（仅这一对）' },
]

const REASON_EXPLAIN: Record<string, string> = {
  sys_timestamp_pattern: '系统时间戳命名：create_time / update_by / modify_dt 这类',
  boolean_prefix: '布尔前缀：is_ / has_ / flag_ 开头',
  flag_suffix: '标志后缀：_flag / _flg / _bit 结尾',
  hash_suffix: '哈希/签名：_md5 / _hash / _digest / _sign 结尾',
  pipeline_prefix: '管道字段：load_ / ingest_ / etl_ / dq_ 开头',
  reserved_field: '保留字段：reserved_ / spare_ / bei_yong_ 开头',
  short_id: '短 ID：pid / rid / sid 等 3-4 字母',
  pk_fk_suffix: '主外键后缀：_pk / _fk / _pk_id / _fk_id',
  row_rec_prefix: '行记录前缀：row_ / rec_ / record_',
  hidden_ts_name: '隐式时间戳：纯 ts / dt / ut / ct / mt',
  tmp_debug_field: '调试/临时字段：tmp_ / temp_ / test_ / debug_',
  dup_suffix: '冗余副本：xx_2 / xx_copy / xx_dup',
  secret_password: '敏感字段：password / token / secret / api_key',
  empty_and_unused: '完全不用：usage_count=0 且所有样例为空',
  all_samples_null: '样例全空：有样例槽位但都是 null/空串',
  no_usage: 'SQL 中从未出现',
  type_time_with_sys_keyword: '时间类型 + 名字含 etl/sys/load/ingest',
  samples_all_masked: '样例全是 # 开头或 *** 占位（加密/脱敏）',
}

const renderReasonTag = (r: string) => {
  const explain = Object.entries(REASON_EXPLAIN).find(([k]) => r.startsWith(k))?.[1]
  const tag = <Tag style={{ fontSize: 10, marginBottom: 2 }}>{r}</Tag>
  return explain ? <Tooltip title={explain} key={r}>{tag}</Tooltip> : tag
}

const ColHeader = ({ title, help }: { title: string; help: string }) => (
  <Tooltip title={help}>
    <Space size={4}>
      <span>{title}</span>
      <QuestionCircleOutlined style={{ color: '#bfbfbf', fontSize: 11 }} />
    </Space>
  </Tooltip>
)

export default function BlacklistAutoDetect() {
  return (
    <div style={{ padding: 24 }}>
      <Title level={4} style={{ margin: '0 0 16px 0' }}>字段黑名单治理（W5-E）</Title>
      <Tabs
        defaultActiveKey="candidates"
        items={[
          { key: 'candidates', label: '新候选', children: <CandidatesTab /> },
          { key: 'current', label: '当前黑名单', children: <CurrentTab /> },
        ]}
      />
    </div>
  )
}

function CandidatesTab() {
  const [data, setData] = useState<any>(null)
  const [rows, setRows] = useState<Candidate[]>([])
  const [regenOpen, setRegenOpen] = useState(false)
  const [regenBusy, setRegenBusy] = useState(false)
  const [applying, setApplying] = useState(false)
  const [appliedIds, setAppliedIds] = useState<Set<string>>(new Set())
  const [form] = Form.useForm()

  const load = async () => {
    const d = await api.getTechFieldCandidates()
    setData(d)
    setRows(
      (d.proposals || []).map((p: any) => ({
        ...p,
        _row: {
          selected: p.score >= 0.7 || p.llm_judgement === 'technical',
          action: p.suggested_action,
          value: p.suggested_value,
        },
      })),
    )
  }
  useEffect(() => { load() }, [])

  const handleRegen = async () => {
    const v = await form.validateFields()
    setRegenBusy(true)
    try {
      await api.regenerateTechFieldCandidates({
        no_llm: !!v.no_llm,
        min_score: v.min_score,
        llm_low: v.llm_low,
        llm_high: v.llm_high,
      })
      message.success('技术字段候选已重跑')
      setRegenOpen(false)
      await load()
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    } finally {
      setRegenBusy(false)
    }
  }

  const updateRow = (id: string, patch: Partial<Candidate['_row']>) => {
    setRows((prev) => prev.map((r) => (r.candidate_id === id ? { ...r, _row: { ...(r._row as any), ...patch } } : r)))
  }

  const handleApply = async () => {
    const items = rows
      .filter((r) => r._row?.selected && !appliedIds.has(r.candidate_id))
      .map((r) => ({
        candidate_id: r.candidate_id,
        action: r._row!.action,
        value: r._row!.value,
        reason: `W5-E detect score=${r.score} reasons=${r.reasons.join('|')}`,
      }))
    if (!items.length) { message.warning('未勾选任何候选'); return }
    setApplying(true)
    try {
      const resp = await api.applyTechFieldBlacklist({ items })
      message.success(`✅ 已追加：exact=${resp.exact_name} / pattern=${resp.name_pattern} / pair=${resp.table_field_pair}（重复跳过 ${resp.skipped_duplicate}）`, 5)
      setAppliedIds((prev) => {
        const next = new Set(prev)
        for (const it of items) next.add(it.candidate_id)
        return next
      })
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    } finally {
      setApplying(false)
    }
  }

  if (!data || !data.exists) {
    return (
      <div>
        <Alert type="warning" message="尚未生成候选" showIcon />
        <Button type="primary" onClick={() => setRegenOpen(true)} style={{ marginTop: 16 }}>扫描技术字段</Button>
        <RegenModal open={regenOpen} onCancel={() => setRegenOpen(false)} onOk={handleRegen} busy={regenBusy} form={form} />
      </div>
    )
  }

  const s = data.summary || {}
  const selectedCount = rows.filter((r) => r._row?.selected && !appliedIds.has(r.candidate_id)).length

  const columns = [
    {
      title: <ColHeader title="选" help="勾选后点'批量应用'会把该条规则写进 field_blacklist.yaml。高置信默认勾选。" />,
      width: 50,
      render: (_: any, r: Candidate) => (
        <Switch
          size="small"
          disabled={appliedIds.has(r.candidate_id)}
          checked={!!r._row?.selected && !appliedIds.has(r.candidate_id)}
          onChange={(v) => updateRow(r.candidate_id, { selected: v })}
        />
      ),
    },
    {
      title: <ColHeader title="field" help="字段名 · 注释 · 数据类型；usage=该字段在 SQL 中出现次数；影响 N=如果该 action 应用，全库会影响多少字段" />,
      width: 220,
      render: (_: any, r: Candidate) => (
        <div>
          <Text code style={{ fontSize: 12 }}>{r.field_name}</Text>
          <br />
          <Text type="secondary" style={{ fontSize: 11 }}>{r.field_comment || '(无注释)'} · {r.data_type}</Text>
          <br />
          <Text type="secondary" style={{ fontSize: 11 }}>usage={r.usage_count} · 影响 {r.affected_field_count} 字段</Text>
        </div>
      ),
    },
    {
      title: <ColHeader title="example tables" help="命中该规则的表。若多表命中，说明这是全库性技术字段，建议用 exact_name 或 name_pattern；若只 1 表，用 table_field_pair 更精准。" />,
      width: 280,
      render: (_: any, r: Candidate) => (
        <div style={{ fontSize: 11, maxHeight: 60, overflow: 'auto' }}>
          {r.example_tables.slice(0, 3).map((t, i) => <div key={i}>{t}</div>)}
          {r.example_tables.length > 3 && <Text type="secondary">... +{r.example_tables.length - 3}</Text>}
        </div>
      ),
    },
    {
      title: <ColHeader title="score / reasons" help="score 0-1：多规则加权融合的技术字段置信度（≥0.7 红=高；0.5-0.7 橙=中；<0.5 灰=低）。reasons：触发了哪些规则（鼠标悬停看解释）。LLM：边界段兜底判断（technical/business/uncertain）。" />,
      width: 280,
      render: (_: any, r: Candidate) => (
        <Space direction="vertical" size={2}>
          <Space>
            <Tooltip title={`综合规则分数 ${r.score.toFixed(2)}（≥0.7 高置信）`}>
              <Tag color={r.score >= 0.7 ? 'red' : r.score >= 0.5 ? 'orange' : 'default'}>{r.score.toFixed(2)}</Tag>
            </Tooltip>
            {r.llm_judgement && (
              <Tooltip title={r.llm_reason || ''}>
                <Tag color={r.llm_judgement === 'technical' ? 'volcano' : r.llm_judgement === 'business' ? 'green' : 'default'}>
                  LLM: {r.llm_judgement}
                </Tag>
              </Tooltip>
            )}
          </Space>
          <div style={{ fontSize: 11 }}>
            {r.reasons.slice(0, 4).map((x) => renderReasonTag(x))}
          </div>
        </Space>
      ),
    },
    {
      title: <ColHeader title="action" help="加黑粒度：exact_name=全库同名字段；name_pattern=fnmatch 通配模式（如 etl_*）；table_field_pair=只加黑(表,字段)这一对，粒度最细。" />,
      width: 200,
      render: (_: any, r: Candidate) => (
        <Select
          size="small"
          disabled={appliedIds.has(r.candidate_id)}
          value={r._row?.action}
          onChange={(v) => updateRow(r.candidate_id, { action: v })}
          options={ACTION_OPTIONS}
          style={{ width: 200 }}
        />
      ),
    },
    {
      title: <ColHeader title="value" help="写入 field_blacklist.yaml 的字面值。exact_name 写 field_name；name_pattern 写 fnmatch 模式；table_field_pair 写 'table/field'。可人工修改。" />,
      render: (_: any, r: Candidate) => (
        <Input
          size="small"
          disabled={appliedIds.has(r.candidate_id)}
          value={r._row?.value}
          onChange={(e) => updateRow(r.candidate_id, { value: e.target.value })}
        />
      ),
    },
    {
      title: '',
      width: 80,
      render: (_: any, r: Candidate) => appliedIds.has(r.candidate_id) ? <Tag color="green">已加黑</Tag> : null,
    },
  ]

  return (
    <div>
      <Space style={{ width: '100%', justifyContent: 'space-between', marginBottom: 12 }}>
        <Space>
          <Tag>总扫: {s.total_scanned}</Tag>
          <Tag>已黑: {s.already_blacklisted}</Tag>
          <Tag color="red">新候选: {s.new_candidates}</Tag>
          <Tag color="orange">高置信: {s.by_confidence?.high ?? 0}</Tag>
          <Tag color="gold">中: {s.by_confidence?.medium ?? 0}</Tag>
          <Tag>低: {s.by_confidence?.low ?? 0}</Tag>
          <Tag color="blue">LLM 判: {s.llm_judged ?? 0}</Tag>
        </Space>
        <Space>
          <Button onClick={() => setRegenOpen(true)}>重跑扫描</Button>
          <Popconfirm
            title={`应用 ${selectedCount} 条到 field_blacklist.yaml？`}
            disabled={selectedCount === 0}
            onConfirm={handleApply}
          >
            <Button type="primary" loading={applying} disabled={selectedCount === 0}>
              批量应用（{selectedCount}）
            </Button>
          </Popconfirm>
        </Space>
      </Space>
      <Alert
        type="info"
        showIcon
        message={'黑名单 7 大类：技术字段 / 空值字段 / 废弃 / 调试临时 / 冗余副本 / 敏感字段 / 加密脱敏'}
        description={
          <div style={{ fontSize: 12, lineHeight: 1.8 }}>
            <div><Text strong>列说明</Text>（鼠标悬停列头/reason 标签看详细解释）：</div>
            <div>
              <Tag>选</Tag> 勾选后批量应用 ·
              <Tag>field</Tag> 字段名/注释/类型/usage/影响字段数 ·
              <Tag>example tables</Tag> 命中的表（多表→用 pattern/exact，单表→pair） ·
              <Tag>score</Tag> 技术字段置信度 0-1 ·
              <Tag>reasons</Tag> 触发的规则 ·
              <Tag>action</Tag> 加黑粒度 ·
              <Tag>value</Tag> 写入 yaml 的字面值
            </div>
            <div style={{ marginTop: 4 }}>
              <Text strong>置信度分级</Text>：
              <Tag color="red">高 ≥ 0.7</Tag>（默认勾选，多信号+命中强规则）
              <Tag color="orange">中 0.5-0.7</Tag>（建议人工复核，可能进入 LLM 兜底段）
              <Tag>低 &lt; 0.5</Tag>（大多是"无用量"这种弱信号，默认不勾选）
            </div>
            <div>
              <Text strong>action 粒度选择</Text>：同名字段跨多表共用（如 etl_time / create_by）→ <code>exact_name</code>；同族前/后缀批量（如 reserved_* / _flag）→ <code>name_pattern</code>；单表特例 → <code>table_field_pair</code>
            </div>
          </div>
        }
        style={{ marginBottom: 12 }}
      />
      <Table
        size="small"
        rowKey="candidate_id"
        dataSource={rows}
        columns={columns as any}
        pagination={{ pageSize: 50, showSizeChanger: true }}
      />
      <RegenModal open={regenOpen} onCancel={() => setRegenOpen(false)} onOk={handleRegen} busy={regenBusy} form={form} />
    </div>
  )
}

function RegenModal({ open, onCancel, onOk, busy, form }: any) {
  return (
    <Modal title="重跑技术字段扫描" open={open} onCancel={onCancel} onOk={onOk} okButtonProps={{ loading: busy }} destroyOnHidden>
      <Form form={form} layout="vertical" initialValues={{ no_llm: false, min_score: 0.3, llm_low: 0.4, llm_high: 0.7 }}>
        <Form.Item name="no_llm" label="跳过 LLM 兜底（只规则）" valuePropName="checked">
          <Switch />
        </Form.Item>
        <Form.Item name="min_score" label="最低入候选分">
          <InputNumber min={0.1} max={1} step={0.05} style={{ width: 120 }} />
        </Form.Item>
        <Form.Item name="llm_low" label="LLM 兜底区间下界">
          <InputNumber min={0.1} max={0.9} step={0.05} style={{ width: 120 }} />
        </Form.Item>
        <Form.Item name="llm_high" label="LLM 兜底区间上界">
          <InputNumber min={0.1} max={0.9} step={0.05} style={{ width: 120 }} />
        </Form.Item>
      </Form>
    </Modal>
  )
}

// ---- 当前黑名单 Tab ----

function CurrentTab() {
  const [data, setData] = useState<{ tree: L1Group[]; total_noise: number; whitelist_count: number } | null>(null)
  const [busy, setBusy] = useState<string | null>(null)
  const [keyword, setKeyword] = useState('')

  const load = async () => {
    const d = await api.getCurrentBlacklist(5000)
    setData(d)
  }
  useEffect(() => { load() }, [])

  const filtered = useMemo<L1Group[]>(() => {
    if (!data) return []
    if (!keyword.trim()) return data.tree
    const kw = keyword.toLowerCase()
    const matchField = (f: CurrentField) =>
      f.field_name.toLowerCase().includes(kw) ||
      (f.field_comment || '').includes(keyword) ||
      (f.sample_values || []).some((s) => String(s).toLowerCase().includes(kw))

    const out: L1Group[] = []
    for (const l1g of data.tree) {
      const l1Match = l1g.l1.includes(keyword)
      const l2New: L2Group[] = []
      for (const l2g of l1g.l2_groups) {
        const l2Match = l2g.l2.includes(keyword)
        const tNew: TableGroup[] = []
        for (const t of l2g.tables) {
          const tMatch = t.table_en.toLowerCase().includes(kw) || (t.table_cn || '').includes(keyword)
          const ffields = l1Match || l2Match || tMatch ? t.fields : t.fields.filter(matchField)
          if (ffields.length) tNew.push({ ...t, fields: ffields, field_count: ffields.length })
        }
        if (tNew.length) l2New.push({ ...l2g, tables: tNew, field_count: tNew.reduce((a, b) => a + b.field_count, 0) })
      }
      if (l2New.length) out.push({ ...l1g, l2_groups: l2New, field_count: l2New.reduce((a, b) => a + b.field_count, 0) })
    }
    return out
  }, [data, keyword])

  const toggle = async (t: string, f: string, remove: boolean) => {
    const key = `${t}::${f}::${remove}`
    setBusy(key)
    try {
      await api.toggleBlacklistWhitelist({ table_en: t, field_name: f, remove, reason: remove ? '重新拉黑' : '人工判业务字段' })
      message.success(remove ? '已取消白名单（pipeline 重跑后生效）' : '已加入白名单（pipeline 重跑后生效）', 4)
      await load()
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    } finally {
      setBusy(null)
    }
  }

  if (!data) return <Alert type="info" message="加载中..." />

  const renderFieldTable = (tableEn: string, fields: CurrentField[]) => (
    <Table
      size="small"
      pagination={false}
      rowKey="field_name"
      dataSource={fields}
      columns={[
        {
          title: 'field',
          width: 200,
          render: (_: any, r: CurrentField) => (
            <Space direction="vertical" size={0}>
              <Text code>{r.field_name}</Text>
              <Text type="secondary" style={{ fontSize: 11 }}>{r.field_comment || '(无注释)'} · {r.data_type}</Text>
            </Space>
          ),
        },
        {
          title: 'sample',
          width: 240,
          render: (_: any, r: CurrentField) => (
            <div style={{ fontSize: 11, maxHeight: 66, overflow: 'auto' }}>
              {(r.sample_values || []).length === 0 ? <Text type="secondary">(空)</Text> : (r.sample_values || []).slice(0, 5).map((v, i) => (
                <div key={i} style={{ whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                  {String(v).slice(0, 50)}
                </div>
              ))}
            </div>
          ),
        },
        { title: 'usage', dataIndex: 'usage_count', width: 70 },
        { title: 'noise_reason', width: 220, render: (_: any, r: CurrentField) => <Tag>{r.noise_reason}</Tag> },
        {
          title: '状态',
          width: 100,
          render: (_: any, r: CurrentField) => r.whitelisted ? <Tag color="green">已白名单</Tag> : <Tag color="red">标黑中</Tag>,
        },
        {
          title: '操作',
          width: 140,
          render: (_: any, r: CurrentField) => r.whitelisted ? (
            <Button size="small" danger
              loading={busy === `${tableEn}::${r.field_name}::true`}
              onClick={() => toggle(tableEn, r.field_name, true)}>
              取消白名单
            </Button>
          ) : (
            <Button size="small"
              loading={busy === `${tableEn}::${r.field_name}::false`}
              onClick={() => toggle(tableEn, r.field_name, false)}>
              加白名单
            </Button>
          ),
        },
      ] as any}
    />
  )

  return (
    <div>
      <Space style={{ width: '100%', justifyContent: 'space-between', marginBottom: 12 }}>
        <Space>
          <Tag color="red">当前标黑字段: {data.total_noise}</Tag>
          <Tag color="green">已白名单: {data.whitelist_count}</Tag>
          <Text type="secondary" style={{ fontSize: 12 }}>按 L1/L2 树形展示；点"加白名单"取消误杀</Text>
        </Space>
        <Input.Search
          placeholder="字段名 / 注释 / 样例 / 表名 / L1 / L2"
          value={keyword}
          onChange={(e) => setKeyword(e.target.value)}
          style={{ width: 360 }}
          allowClear
        />
      </Space>
      <Alert
        type="warning"
        showIcon
        message="白名单变更不会立刻更新页面统计"
        description={'白名单写入 `field_blacklist_whitelist.yaml` 后，必须跑 `run_pipeline.py --from field_features` 才会在 field_features.parquet 里把 is_technical_noise 翻过来。当前视图只显示"当前 parquet 里还被标黑的字段"。'}
        style={{ marginBottom: 12 }}
      />
      <Collapse>
        {filtered.map((l1g) => (
          <Panel
            key={`L1:${l1g.l1}`}
            header={
              <Space>
                <Text strong style={{ fontSize: 14 }}>{l1g.l1}</Text>
                <Badge count={l1g.l2_count} style={{ backgroundColor: '#1677ff' }} title="L2 数" />
                <Badge count={l1g.field_count} color="red" title="标黑字段数" />
              </Space>
            }
          >
            <Collapse>
              {l1g.l2_groups.map((l2g) => (
                <Panel
                  key={`L2:${l1g.l1}/${l2g.l2}`}
                  header={
                    <Space>
                      <Text strong>{l2g.l2}</Text>
                      <Badge count={l2g.table_count} style={{ backgroundColor: '#999' }} title="表数" />
                      <Badge count={l2g.field_count} color="red" title="标黑字段数" />
                    </Space>
                  }
                >
                  <Collapse>
                    {l2g.tables.map((t) => (
                      <Panel
                        key={`T:${t.table_en}`}
                        header={
                          <Space>
                            <Text code style={{ fontSize: 12 }}>{t.table_en}</Text>
                            {t.table_cn && <Text type="secondary" style={{ fontSize: 11 }}>（{t.table_cn}）</Text>}
                            <Badge count={t.field_count} color="red" />
                          </Space>
                        }
                      >
                        {renderFieldTable(t.table_en, t.fields)}
                      </Panel>
                    ))}
                  </Collapse>
                </Panel>
              ))}
            </Collapse>
          </Panel>
        ))}
      </Collapse>
    </div>
  )
}
