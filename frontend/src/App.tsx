import { useEffect, useMemo, useRef, useState } from 'react'
import {
  Alert,
  Cascader,
  Form,
  Input,
  Layout,
  Menu,
  message,
  Modal,
  Select,
  Spin,
  Switch,
  Tag,
  Tooltip,
  Typography,
  Badge,
  Space,
  ConfigProvider,
  theme,
  Button,
} from 'antd'
import { PlusOutlined } from '@ant-design/icons'
import zh_CN from 'antd/locale/zh_CN'
import { BrowserRouter, Link, Route, Routes, useLocation, useNavigate, useParams } from 'react-router-dom'
import { api } from './api'
import { TABLE_TYPE_COLOR } from './types'
import type { Stats, VTSummary } from './types'
import VTEditor from './pages/VTEditor'
import Welcome from './pages/Welcome'
import NormalizationDashboard from './pages/NormalizationDashboard'
import NormalizationReview from './pages/NormalizationReview'
import NamingDiagnosis from './pages/NamingDiagnosis'
import NamingHomonyms from './pages/NamingHomonyms'
import BenchmarkAttribution from './pages/BenchmarkAttribution'
import L2Alignment from './pages/L2Alignment'
import L1Alignment from './pages/L1Alignment'
import BasePromotion from './pages/BasePromotion'
import BlacklistAutoDetect from './pages/BlacklistAutoDetect'
import NewSlotCandidates from './pages/NewSlotCandidates'
import SlotLibrary from './pages/SlotLibrary'
import Categories from './pages/Categories'
import { emitScaffoldChanged, onScaffoldChanged } from './lib/events'

const { Header, Sider, Content } = Layout
const { Title, Text } = Typography

function Sidebar({
  vts,
  categoryTree,
  selectedId,
  onSelect,
  onCreate,
}: {
  vts: VTSummary[]
  categoryTree: Array<{ name: string; children: Array<{ name: string }> }>
  selectedId?: string
  onSelect: (id: string) => void
  onCreate: () => void
}) {
  const [keyword, setKeyword] = useState('')

  // 按 L1 → L2 → VT 分组（I-16 Gap1）
  // 合并 categoryTree：即使某 L2 下没有 VT，也要展示（否则分类树定义的空 L2 会被隐藏）
  const { items, l1Keys, pendingVTs } = useMemo(() => {
    const filtered = keyword
      ? vts.filter(
          (v) =>
            v.topic.includes(keyword) ||
            v.vt_id.includes(keyword) ||
            v.l2_path.join(' ').includes(keyword),
        )
      : vts
    // L1 → L2 → [VT]
    const tree: Record<string, Record<string, VTSummary[]>> = {}
    filtered.forEach((v) => {
      const l1 = v.l2_path[0] || '（无分类）'
      const l2 = v.l2_path[1] || '（无二级）'
      tree[l1] = tree[l1] || {}
      tree[l1][l2] = tree[l1][l2] || []
      tree[l1][l2].push(v)
    })
    // 合并分类树：保留所有 L2（哪怕无 VT）。keyword 搜索时不填充空 L2（避免搜索结果被噪声撑开）
    if (!keyword) {
      for (const l1 of categoryTree) {
        tree[l1.name] = tree[l1.name] || {}
        for (const l2 of l1.children || []) {
          if (!(l2.name in tree[l1.name])) {
            tree[l1.name][l2.name] = []
          }
        }
      }
    }
    const l1Keys = Object.keys(tree).sort()
    const items = l1Keys.map((l1) => {
      const l2Groups = tree[l1]
      const l1Count = Object.values(l2Groups).reduce((a, b) => a + b.length, 0)
      return {
        key: `L1:${l1}`,
        label: (
          <Space>
            <Text strong style={{ fontSize: 13 }}>{l1}</Text>
            <Badge count={l1Count} style={{ backgroundColor: '#1677ff' }} />
          </Space>
        ),
        children: Object.keys(l2Groups).sort().map((l2) => {
          const l2Vts = l2Groups[l2]
          const isEmpty = l2Vts.length === 0
          return {
          key: `L2:${l1}/${l2}`,
          label: (
            <Space>
              <Text style={{ fontSize: 12, color: isEmpty ? '#bfbfbf' : undefined }}>{l2}</Text>
              <Badge
                count={isEmpty ? '空' : l2Vts.length}
                style={{ backgroundColor: isEmpty ? '#d9d9d9' : '#999', color: isEmpty ? '#595959' : undefined }}
              />
            </Space>
          ),
          children: isEmpty
            ? [{
                key: `L2:${l1}/${l2}:__empty__`,
                label: (
                  <Text type="secondary" style={{ fontSize: 11, fontStyle: 'italic' }}>
                    分类树已定义，暂无 VT（去"新建 VT"填充）
                  </Text>
                ),
                disabled: true,
              }]
            : l2Vts.map((v) => {
            const isPending = v.is_pending || v.table_type === '待定'
            const hasNoSlots = v.slot_status === 'no_slots'
            return {
              key: v.vt_id,
              label: (
                <div style={{ display: 'flex', justifyContent: 'space-between', gap: 4, alignItems: 'center' }}>
                  <Space size={4} style={{ minWidth: 0 }}>
                    <Tag color={TABLE_TYPE_COLOR[v.table_type] || 'default'} style={{ fontSize: 10, margin: 0 }}>
                      {v.table_type}
                    </Tag>
                    <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                      {v.topic}
                    </span>
                  </Space>
                  <Space size={2} style={{ flexShrink: 0 }}>
                    {isPending && (
                      <Tooltip title={v.table_type === '待定' ? '待定 VT：table_type 未定，需要审核分类' : '待审：有未处理的 review 项'}>
                        <Tag color="orange" style={{ fontSize: 9, margin: 0, padding: '0 4px', lineHeight: '16px' }}>待审</Tag>
                      </Tooltip>
                    )}
                    {hasNoSlots ? (
                      <Tooltip title="无槽位：该 VT 还未生成 slot_definitions，需去 VTEditor 点「重新生成」">
                        <Text type="secondary" style={{ fontSize: 11, color: '#faad14' }}>⚠</Text>
                      </Tooltip>
                    ) : (
                      <Text type="secondary" style={{ fontSize: 11 }}>{v.slot_count}</Text>
                    )}
                  </Space>
                </div>
              ),
            }
          }),
          }
        }),
      }
    })
    // 待审计数统计（供跳转按钮 + 顶部提示）
    const pendingVTs = filtered.filter((v) => v.is_pending || v.table_type === '待定' || v.slot_status === 'no_slots')
    return { items, l1Keys, pendingVTs }
  }, [vts, keyword, categoryTree])

  return (
    <Sider width={320} theme="light" style={{ borderRight: '1px solid #f0f0f0', overflow: 'auto' }}>
      <div style={{ padding: 12 }}>
        <Space direction="vertical" style={{ width: '100%' }} size={8}>
          <Button type="primary" block icon={<PlusOutlined />} onClick={onCreate}>
            新建 VT
          </Button>
          <Input.Search
            placeholder="搜索 topic / vt_id / 分类"
            allowClear
            onChange={(e) => setKeyword(e.target.value)}
          />
          {pendingVTs.length > 0 && (
            <Button
              block
              size="small"
              onClick={() => {
                const idx = pendingVTs.findIndex((v) => v.vt_id === selectedId)
                const next = pendingVTs[(idx + 1) % pendingVTs.length]
                if (next) onSelect(next.vt_id)
              }}
              style={{ background: '#fff7e6', borderColor: '#ffd591', color: '#d46b08' }}
            >
              ⏭ 跳到下一个待审（{pendingVTs.length}）
            </Button>
          )}
        </Space>
      </div>
      <Menu
        mode="inline"
        selectedKeys={selectedId ? [selectedId] : []}
        defaultOpenKeys={l1Keys.map((l1) => `L1:${l1}`)}
        items={items}
        onClick={({ key }) => {
          if (vts.some((v) => v.vt_id === key)) onSelect(key)
        }}
        style={{ borderRight: 0 }}
      />
    </Sider>
  )
}

function TopNav({ stats }: { stats: Stats | null }) {
  const location = useLocation()
  const isNormalization = location.pathname.startsWith('/normalization')
  return (
    <Header
      style={{
        background: '#fff',
        padding: '0 24px',
        borderBottom: '1px solid #f0f0f0',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
      }}
    >
      <Space size="large">
        <Title level={4} style={{ margin: 0 }}>
          tso_standard · Review
        </Title>
        <Space size="small">
          <Link to="/slot-library">
            <Button type={location.pathname === '/slot-library' ? 'primary' : 'text'}>槽位库</Button>
          </Link>
          <Link to="/">
            <Button type={location.pathname === '/' || location.pathname.startsWith('/vt/') ? 'primary' : 'text'}>
              虚拟表
            </Button>
          </Link>
          <Link to="/normalization">
            <Button type={isNormalization && location.pathname === '/normalization' ? 'primary' : 'text'}>
              字段归一 Dashboard
            </Button>
          </Link>
          <Link to="/normalization/review">
            <Button type={location.pathname.startsWith('/normalization/review') ? 'primary' : 'text'}>
              归一审核
            </Button>
          </Link>
          <Link to="/normalization/new-slots">
            <Button type={location.pathname === '/normalization/new-slots' ? 'primary' : 'text'}>
              新槽位候选
            </Button>
          </Link>
          <Link to="/naming/diagnosis">
            <Button type={location.pathname === '/naming/diagnosis' ? 'primary' : 'text'}>
              命名诊断
            </Button>
          </Link>
          <Link to="/naming/homonyms">
            <Button type={location.pathname === '/naming/homonyms' ? 'primary' : 'text'}>
              同名异义
            </Button>
          </Link>
          <Link to="/benchmark/attribution">
            <Button type={location.pathname === '/benchmark/attribution' ? 'primary' : 'text'}>
              归因
            </Button>
          </Link>
          <Link to="/alignment/l2">
            <Button type={location.pathname === '/alignment/l2' ? 'primary' : 'text'}>
              L2 对齐
            </Button>
          </Link>
          <Link to="/alignment/l1">
            <Button type={location.pathname === '/alignment/l1' ? 'primary' : 'text'}>
              L1 对齐
            </Button>
          </Link>
          <Link to="/alignment/base">
            <Button type={location.pathname === '/alignment/base' ? 'primary' : 'text'}>
              base 提升
            </Button>
          </Link>
          <Link to="/blacklist/auto-detect">
            <Button type={location.pathname === '/blacklist/auto-detect' ? 'primary' : 'text'}>
              黑名单治理
            </Button>
          </Link>
          <Link to="/categories">
            <Button type={location.pathname === '/categories' ? 'primary' : 'text'}>
              分类树
            </Button>
          </Link>
        </Space>
      </Space>
      {stats && !isNormalization && (
        <Space size="middle">
          <Text type="secondary">
            {stats.vt_count} VT · {stats.total_slots} 槽位 · base {(stats.overall_base_reuse_ratio * 100).toFixed(1)}%
          </Text>
        </Space>
      )}
    </Header>
  )
}


type CreateL2Node = {
  name: string
  tables: Array<{ en: string; cn: string }>
  vts: Array<{ vt_id: string; topic: string; source_tables: Array<{ en: string; cn: string }> }>
}
type CreateL1Node = { name: string; children: CreateL2Node[] }

function CreateVTModal({ open, onClose, onCreated }: { open: boolean; onClose: () => void; onCreated: (vt_id: string) => void }) {
  const [form] = Form.useForm()
  const [submitting, setSubmitting] = useState(false)
  const [categories, setCategories] = useState<CreateL1Node[]>([])
  // 观察 L1/L2 选择
  const l_path = Form.useWatch('l_path', form) as string[] | undefined

  useEffect(() => {
    if (open && categories.length === 0) {
      api.getCategories().then((r) => setCategories(r.categories)).catch(() => {})
    }
  }, [open])

  // L1/L2 变更时清空已选物理表（避免旧 L2 的选中项残留）
  useEffect(() => {
    form.setFieldValue('candidate_tables', [])
  }, [l_path?.[0], l_path?.[1]])

  // 当前 L2 节点（根据 l_path）+ 可选物理表 + 挂载状态
  const currentL2Node = useMemo(() => {
    if (!l_path || l_path.length < 2) return null
    const l1 = categories.find((c) => c.name === l_path[0])
    return l1?.children.find((c) => c.name === l_path[1]) || null
  }, [l_path, categories])

  const tableOptions = useMemo(() => {
    if (!currentL2Node) return []
    // 计算挂载：table_en → [vt_topic, ...]
    const mounted: Record<string, string[]> = {}
    for (const vt of currentL2Node.vts) {
      for (const t of vt.source_tables || []) {
        (mounted[t.en] ||= []).push(vt.topic)
      }
    }
    return currentL2Node.tables.map((t) => {
      const vts = mounted[t.en] || []
      const isUnmounted = vts.length === 0
      return {
        value: t.en,
        label: `${isUnmounted ? '🆕 ' : ''}${t.en}${t.cn ? ' · ' + t.cn : ''}${vts.length > 0 ? ` [已挂 ${vts.length} 个 VT: ${vts.slice(0, 2).join(',')}${vts.length > 2 ? '…' : ''}]` : '（未挂载）'}`,
        isUnmounted,
      }
    }).sort((a, b) => (a.isUnmounted === b.isUnmounted ? 0 : a.isUnmounted ? -1 : 1))  // 未挂载优先
  }, [currentL2Node])

  const handleOk = async () => {
    try {
      const values = await form.validateFields()
      setSubmitting(true)
      const path: string[] = values.l_path || []
      const tableEnSet: string[] = values.candidate_tables || []
      const candidate_tables = tableEnSet.map((en) => {
        const t = currentL2Node?.tables.find((x) => x.en === en)
        return t ? { en: t.en, cn: t.cn } : { en, cn: '' }
      })
      const resp = await api.createVT({
        topic: values.topic,
        grain_desc: values.grain_desc || '',
        table_type: values.table_type || '待定',
        l2_path: path.filter(Boolean),
        candidate_tables,
      })
      message.success(`新建成功: ${resp.vt_id}（关联 ${candidate_tables.length} 张物理表）`)
      if (resp.next_action) message.warning(resp.next_action, 5)
      onCreated(resp.vt_id)
      form.resetFields()
      onClose()
    } catch (e: any) {
      if (e?.response) {
        message.error(e.response?.data?.detail || '创建失败')
      }
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <Modal title="新建 VT" open={open} onOk={handleOk} onCancel={onClose} confirmLoading={submitting} width={640} destroyOnClose>
      <Alert
        type="info"
        showIcon
        style={{ marginBottom: 16 }}
        message="新建后需运行 run_pipeline.py --from slot_definitions 让下游同步"
      />
      <Form form={form} layout="vertical">
        <Form.Item name="topic" label="topic（主题）" rules={[{ required: true }]}>
          <Input placeholder="例：二手车交易事件" />
        </Form.Item>
        <Form.Item name="grain_desc" label="grain_desc（粒度描述）">
          <Input.TextArea autoSize={{ minRows: 2 }} placeholder="例：每笔交易一行" />
        </Form.Item>
        <Form.Item name="table_type" label="table_type" initialValue="待定" rules={[{ required: true }]}>
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
            placeholder="选择分类（如需新增请去「分类树」页）"
            changeOnSelect
            showSearch={{
              filter: (input, path) => path.some((p) => String(p.label).toLowerCase().includes(input.toLowerCase())),
            }}
          />
        </Form.Item>
        <Form.Item
          name="candidate_tables"
          label={
            <Space size={4}>
              <span>关联物理表</span>
              <Tooltip title={'只展示所选 L1/L2 分类下的物理表。🆕 前缀=还没被任何 VT 挂载（空闲表，优先推荐）；已挂的表会显示"已挂 N 个 VT"'}>
                <Text type="secondary" style={{ fontSize: 11 }}>ⓘ</Text>
              </Tooltip>
            </Space>
          }
          extra={
            !l_path || l_path.length < 2
              ? <Text type="secondary" style={{ fontSize: 11 }}>请先选上方的 L1 / L2 分类，物理表会按该分类过滤展示</Text>
              : currentL2Node
                ? <Text type="secondary" style={{ fontSize: 11 }}>
                    {currentL2Node.tables.length} 张物理表（
                    未挂载 {tableOptions.filter((o: any) => o.isUnmounted).length} 张，
                    已挂载 {tableOptions.filter((o: any) => !o.isUnmounted).length} 张）
                  </Text>
                : <Text type="secondary" style={{ fontSize: 11 }}>该 L2 下无物理表定义</Text>
          }
        >
          <Select
            mode="multiple"
            placeholder={l_path && l_path.length >= 2 ? '搜索表名 / 中文名（🆕 标记=未挂载）' : '先选 L1/L2 分类'}
            allowClear
            showSearch
            disabled={!l_path || l_path.length < 2}
            optionFilterProp="label"
            maxTagCount="responsive"
            options={tableOptions}
            style={{ width: '100%' }}
          />
        </Form.Item>
      </Form>
    </Modal>
  )
}


function SlotReviewShell() {
  const [vts, setVts] = useState<VTSummary[]>([])
  const [stats, setStats] = useState<Stats | null>(null)
  const [categoryTree, setCategoryTree] = useState<Array<{ name: string; children: Array<{ name: string }> }>>([])
  const [loading, setLoading] = useState(true)
  const [createOpen, setCreateOpen] = useState(false)
  const navigate = useNavigate()
  const params = useParams()
  const selectedId = params.vt_id

  const refresh = () => {
    Promise.all([api.listVTs(), api.stats(), api.getCategories()])
      .then(([vts, stats, cats]) => {
        setVts(vts)
        setStats(stats)
        setCategoryTree(cats.categories || [])
      })
      .finally(() => setLoading(false))
  }

  useEffect(() => { refresh() }, [])

  // 监听跨页面 scaffold 变化 → 自动刷新侧栏
  useEffect(() => {
    const off = onScaffoldChanged(() => refresh())
    return off
  }, [])

  if (loading) {
    return <div style={{ padding: 40, textAlign: 'center' }}><Spin size="large" /></div>
  }

  return (
    <Layout style={{ flex: 1, minHeight: 0 }}>
      <Sidebar
        vts={vts}
        categoryTree={categoryTree}
        selectedId={selectedId}
        onSelect={(id) => navigate(`/vt/${id}`)}
        onCreate={() => setCreateOpen(true)}
      />
      <CreateVTModal
        open={createOpen}
        onClose={() => setCreateOpen(false)}
        onCreated={(vt_id) => {
          emitScaffoldChanged({ source: 'create-vt', action: 'create', extra: { vt_id } })
          navigate(`/vt/${vt_id}`)
        }}
      />
      <Content style={{ background: '#fafafa', overflow: 'auto' }}>
        <Routes>
          <Route path="/" element={<Welcome stats={stats} />} />
          <Route path="/vt/:vt_id" element={<VTEditor onSaved={refresh} />} />
        </Routes>
      </Content>
    </Layout>
  )
}


const AUTO_PIPELINE_LS_KEY = 'auto_pipeline_enabled'

function readAutoEnabled(): boolean {
  try {
    return localStorage.getItem(AUTO_PIPELINE_LS_KEY) === 'true'
  } catch {
    return false
  }
}

function writeAutoEnabled(v: boolean) {
  try {
    localStorage.setItem(AUTO_PIPELINE_LS_KEY, String(v))
  } catch {}
}

function formatEta(sec: number): string {
  if (sec < 60) return `${Math.round(sec)}s`
  const m = Math.floor(sec / 60)
  const s = Math.round(sec % 60)
  if (m < 60) return s > 0 ? `${m}m${s}s` : `${m}m`
  const h = Math.floor(m / 60)
  const mm = m % 60
  return `${h}h${mm}m`
}

function dirtySignature(sources: string[] | undefined | null): string {
  if (!sources || sources.length === 0) return ''
  return [...sources].sort().join(',')
}

function PipelineJobWatcher() {
  const [job, setJob] = useState<any | null>(null)
  const [dismissed, setDismissed] = useState(false)
  const [autoEnabled, setAutoEnabledState] = useState<boolean>(readAutoEnabled())
  const [dirty, setDirty] = useState<{ dirty: boolean; dirty_sources: string[]; from_step: string } | null>(null)
  const [ignoredDirtySig, setIgnoredDirtySig] = useState<string>('')
  // 自动重跑防抖：30s 内不重复触发同一种 dirty 自动重跑
  const lastAutoTriggerRef = useRef<number>(0)
  // 已通知完成的 job_id 集合（防止 effect 重跑导致 toast 重复）
  const notifiedJobIdsRef = useRef<Set<string>>(new Set())
  // autoEnabled 最新值供 tick 内闭包读取（避免 effect 依赖触发 tick 重启）
  const autoEnabledRef = useRef<boolean>(autoEnabled)
  autoEnabledRef.current = autoEnabled

  const setAutoEnabled = (v: boolean) => {
    writeAutoEnabled(v)
    setAutoEnabledState(v)
    if (v) {
      // OFF → ON：立刻让下一轮 tick 能触发
      lastAutoTriggerRef.current = 0
    }
  }

  useEffect(() => {
    let timer: any
    const tick = async () => {
      // 支持多 tab 同步偏好
      const storedAuto = readAutoEnabled()
      if (storedAuto !== autoEnabledRef.current) {
        autoEnabledRef.current = storedAuto
        setAutoEnabledState(storedAuto)
      }

      try {
        const jobs = await api.listPipelineJobs()
        const running = jobs.find((j) => j.state === 'running')
        if (running) {
          setJob(running)
          setDismissed(false)
        } else {
          // 后端清空 jobs（手动 DELETE 或重启）→ 清掉本地 stale state，避免显示假 running
          if (jobs.length === 0) {
            setJob(null)
          }
          // 找最近完成的
          const latest = jobs.sort((a, b) => (b.started_at || '').localeCompare(a.started_at || ''))[0]
          if (latest && !dismissed) {
            const isTerminal = latest.state === 'done' || latest.state === 'failed'
            if (isTerminal && !notifiedJobIdsRef.current.has(latest.job_id)) {
              notifiedJobIdsRef.current.add(latest.job_id)
              const wasRunning = job?.job_id === latest.job_id && job.state === 'running'
              if (wasRunning) {
                if (latest.state === 'done') {
                  message.success(`pipeline 跑完（--from ${latest.from_step}）`, 5)
                  emitScaffoldChanged({ source: 'pipeline', action: 'done', extra: { from_step: latest.from_step } })
                } else {
                  message.error(`pipeline 失败：${latest.error || `return_code=${latest.return_code}`}`, 10)
                }
              }
            }
            setJob(latest)
          }

          // 无 running job：更新 dirty state（始终）；是否自动触发看 autoEnabled
          try {
            const dirtyResp = await api.pipelineDirtyCheck()
            setDirty(dirtyResp)
            if (autoEnabledRef.current && dirtyResp.dirty && Date.now() - lastAutoTriggerRef.current > 30_000) {
              lastAutoTriggerRef.current = Date.now()
              try {
                const newJob = await api.startPipelineJob(dirtyResp.from_step)
                message.info(
                  `检测到 ${dirtyResp.dirty_sources.join(', ')} 已变更，已自动启动 pipeline 重跑（${newJob.job_id}）`,
                  6,
                )
              } catch (e: any) {
                if (e?.response?.status !== 409) {
                  console.warn('auto-trigger pipeline failed', e)
                }
              }
            }
          } catch {}
        }
      } catch {}
      timer = setTimeout(tick, 2000)
    }
    tick()
    return () => clearTimeout(timer)
  }, [job?.job_id, job?.state, dismissed])

  // 判断 job 浮窗是否要显示
  const jobVisible = (() => {
    if (!job || dismissed) return false
    if (job.state === 'running') return true
    const endTime = new Date(job.ended_at || job.started_at).getTime()
    return Date.now() - endTime <= 15000
  })()

  // 判断 dirty banner 是否要显示（job 浮窗不在时才显示，避免两个同时弹）
  const currentDirtySig = dirtySignature(dirty?.dirty_sources)
  const dirtyBannerVisible = !jobVisible && !!dirty?.dirty && currentDirtySig !== '' && currentDirtySig !== ignoredDirtySig

  const autoSwitch = (
    <Space size={4}>
      <Text style={{ fontSize: 11, color: '#8c8c8c' }}>自动重跑</Text>
      <Switch size="small" checked={autoEnabled} onChange={setAutoEnabled} />
    </Space>
  )

  if (jobVisible) {
    const elapsed = (() => {
      const start = new Date(job.started_at).getTime()
      const end = job.ended_at ? new Date(job.ended_at).getTime() : Date.now()
      const sec = Math.round((end - start) / 1000)
      if (sec < 60) return `${sec}s`
      return `${Math.floor(sec / 60)}m${sec % 60}s`
    })()

    return (
      <div
        style={{
          position: 'fixed',
          bottom: 16,
          right: 16,
          zIndex: 1100,
          background: '#fff',
          border: '1px solid #d9d9d9',
          borderRadius: 8,
          boxShadow: '0 4px 12px rgba(0,0,0,0.12)',
          padding: '10px 14px',
          minWidth: 280,
          maxWidth: 420,
        }}
      >
        <Space direction="vertical" size={4} style={{ width: '100%' }}>
          <Space style={{ width: '100%', justifyContent: 'space-between' }}>
            <Space size={6}>
              {job.state === 'running' && <Spin size="small" />}
              <Text strong style={{ fontSize: 12 }}>
                {job.state === 'running' ? 'Pipeline 运行中' : job.state === 'done' ? '✅ Pipeline 完成' : '❌ Pipeline 失败'}
              </Text>
              <Tag style={{ fontSize: 10 }}>--from {job.from_step}</Tag>
            </Space>
            <Space size={8}>
              {autoSwitch}
              <Button size="small" type="text" onClick={() => setDismissed(true)}>×</Button>
            </Space>
          </Space>
          {job.state === 'running' && (
            <Text type="secondary" style={{ fontSize: 11 }}>
              {job.current_step || '启动中...'} · {elapsed}
              {typeof job.eta_sec === 'number' && job.eta_sec > 0 && (
                <>
                  {' · '}
                  <span style={{ color: '#1677ff' }}>剩余 ~{formatEta(job.eta_sec)}</span>
                </>
              )}
            </Text>
          )}
          {job.state !== 'running' && (
            <Text type="secondary" style={{ fontSize: 11 }}>
              {elapsed} · 下游数据已更新，页面会自动刷新
            </Text>
          )}
        </Space>
      </div>
    )
  }

  if (dirtyBannerVisible && dirty) {
    const handleRunNow = async () => {
      try {
        const newJob = await api.startPipelineJob(dirty.from_step)
        message.info(`已启动 pipeline 重跑（${newJob.job_id}）`, 4)
        setIgnoredDirtySig('')
        lastAutoTriggerRef.current = Date.now()
      } catch (e: any) {
        if (e?.response?.status === 409) {
          message.warning('已有 pipeline 在跑', 3)
        } else {
          message.error(`启动失败：${e?.message || e}`, 5)
        }
      }
    }
    return (
      <div
        style={{
          position: 'fixed',
          bottom: 16,
          right: 16,
          zIndex: 1100,
          background: '#fffbe6',
          border: '1px solid #ffe58f',
          borderRadius: 8,
          boxShadow: '0 4px 12px rgba(0,0,0,0.12)',
          padding: '10px 14px',
          minWidth: 320,
          maxWidth: 460,
        }}
      >
        <Space direction="vertical" size={6} style={{ width: '100%' }}>
          <Space style={{ width: '100%', justifyContent: 'space-between' }}>
            <Text strong style={{ fontSize: 12 }}>
              ⚠️ 检测到数据变更
            </Text>
            {autoSwitch}
          </Space>
          <Text type="secondary" style={{ fontSize: 11 }}>
            {dirty.dirty_sources.join(', ')} 比下游产物新 · 建议从 <Tag style={{ fontSize: 10 }}>{dirty.from_step}</Tag> 开始重跑
          </Text>
          <Space size={6}>
            <Button size="small" type="primary" onClick={handleRunNow}>立即重跑</Button>
            <Button size="small" onClick={() => setIgnoredDirtySig(currentDirtySig)}>忽略本次</Button>
          </Space>
        </Space>
      </div>
    )
  }

  return null
}


function AppShell() {
  const [stats, setStats] = useState<Stats | null>(null)

  useEffect(() => {
    api.stats().then(setStats).catch(() => {})
  }, [])

  return (
    <Layout style={{ height: '100vh' }}>
      <TopNav stats={stats} />
      <PipelineJobWatcher />
      <Layout>
        <Content style={{ display: 'flex', flexDirection: 'column', minHeight: 0 }}>
          <Routes>
            <Route path="/slot-library" element={<SlotLibrary />} />
            <Route path="/categories" element={<Categories />} />
            <Route path="/normalization" element={<NormalizationDashboard />} />
            <Route path="/normalization/review" element={<NormalizationReview />} />
            <Route path="/normalization/new-slots" element={<NewSlotCandidates />} />
            <Route path="/naming/diagnosis" element={<NamingDiagnosis />} />
            <Route path="/naming/homonyms" element={<NamingHomonyms />} />
            <Route path="/benchmark/attribution" element={<BenchmarkAttribution />} />
            <Route path="/alignment/l2" element={<L2Alignment />} />
            <Route path="/alignment/l1" element={<L1Alignment />} />
            <Route path="/alignment/base" element={<BasePromotion />} />
            <Route path="/blacklist/auto-detect" element={<BlacklistAutoDetect />} />
            <Route path="/*" element={<SlotReviewShell />} />
          </Routes>
        </Content>
      </Layout>
    </Layout>
  )
}

export default function App() {
  return (
    <ConfigProvider locale={zh_CN} theme={{ algorithm: theme.defaultAlgorithm }}>
      <BrowserRouter>
        <Routes>
          <Route path="*" element={<AppShell />} />
        </Routes>
      </BrowserRouter>
    </ConfigProvider>
  )
}
