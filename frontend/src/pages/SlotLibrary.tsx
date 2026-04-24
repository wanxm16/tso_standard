import { useEffect, useMemo, useState } from 'react'
import {
  Alert,
  Button,
  Card,
  Collapse,
  Descriptions,
  Drawer,
  Input,
  Popconfirm,
  Space,
  Spin,
  Statistic,
  Tabs,
  Table,
  Tag,
  Typography,
  message,
} from 'antd'
import { DeleteOutlined, PlusOutlined, ThunderboltOutlined } from '@ant-design/icons'
import { Form, Modal, Select, Tooltip } from 'antd'
import { api } from '../api'
import type {
  ExtendedSlotGroup,
  SlotFieldHit,
  SlotLibraryEntry,
  SlotLibraryResponse,
} from '../types'

const { Title, Text, Paragraph } = Typography

const ROLE_ORDER = [
  'subject',
  'subject_id',
  'relation_subject',
  'time',
  'location',
  'display',
  'filter',
  'source',
  'measure',
  'description',
]

const ROLE_LABEL: Record<string, string> = {
  subject: '主语',
  subject_id: '主体标识',
  relation_subject: '关系另一方',
  time: '时间',
  location: '地点',
  display: '展示',
  filter: '过滤',
  source: '来源',
  measure: '统计',
  description: '描述',
}

const ROLE_COLOR: Record<string, string> = {
  subject: 'magenta',
  subject_id: 'red',
  relation_subject: 'volcano',
  time: 'orange',
  location: 'gold',
  display: 'lime',
  filter: 'green',
  source: 'cyan',
  measure: 'blue',
  description: 'purple',
}

function hitColor(cnt: number): string {
  if (cnt >= 300) return 'red'
  if (cnt >= 100) return 'orange'
  if (cnt >= 30) return 'gold'
  if (cnt >= 5) return 'lime'
  if (cnt > 0) return 'default'
  return 'default'
}

export default function SlotLibrary() {
  const [data, setData] = useState<SlotLibraryResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [activeTab, setActiveTab] = useState('base')
  const [keyword, setKeyword] = useState('')
  const [detailSlot, setDetailSlot] = useState<SlotLibraryEntry | null>(null)
  const [detailIsBase, setDetailIsBase] = useState(true)
  const [detailFields, setDetailFields] = useState<SlotFieldHit[]>([])
  const [detailLoading, setDetailLoading] = useState(false)
  const [addAliasInput, setAddAliasInput] = useState('')
  const [nextActionBanner, setNextActionBanner] = useState<string | null>(null)
  // 新增 base 槽位 Modal
  const [createOpen, setCreateOpen] = useState(false)
  const [createForm] = Form.useForm()
  const [suggestLoading, setSuggestLoading] = useState(false)
  const [suggestWarnings, setSuggestWarnings] = useState<string[]>([])
  const [suggested, setSuggested] = useState(false)

  const reload = () => {
    setLoading(true)
    api.slotLibrary().then(setData).finally(() => setLoading(false))
  }
  useEffect(reload, [])

  const openDetail = async (slot: SlotLibraryEntry, isBase: boolean) => {
    setDetailSlot(slot)
    setDetailIsBase(isBase)
    setAddAliasInput('')
    setDetailLoading(true)
    setDetailFields([])
    try {
      const fields = await api.slotLibraryFields(slot.name, 30)
      setDetailFields(fields)
    } finally {
      setDetailLoading(false)
    }
  }

  const handleAddAlias = async () => {
    if (!detailSlot || !addAliasInput.trim()) return
    const alias = addAliasInput.trim()
    try {
      const resp = await api.editBaseSlot(detailSlot.name, { aliases_add: [alias] })
      if (resp.added.length) {
        message.success(`已添加 alias: ${resp.added.join(', ')}`)
        if (resp.next_action) setNextActionBanner(resp.next_action)
        setAddAliasInput('')
        // 更新本地 state
        setDetailSlot({ ...detailSlot, aliases: [...detailSlot.aliases, ...resp.added] })
        reload()
      } else {
        message.info('alias 已存在')
      }
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    }
  }

  const handleRemoveAlias = async (alias: string) => {
    if (!detailSlot) return
    try {
      const resp = await api.editBaseSlot(detailSlot.name, { aliases_remove: [alias] })
      if (resp.removed.length) {
        message.success(`已删除 alias: ${resp.removed.join(', ')}`)
        if (resp.next_action) setNextActionBanner(resp.next_action)
        setDetailSlot({
          ...detailSlot,
          aliases: detailSlot.aliases.filter((a) => !resp.removed.includes(a)),
        })
        reload()
      }
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    }
  }

  const baseByRole = useMemo(() => {
    if (!data) return {}
    const groups: Record<string, SlotLibraryEntry[]> = {}
    for (const s of data.base_slots) {
      if (keyword && !(
        s.name.includes(keyword) ||
        s.cn_name.includes(keyword) ||
        s.aliases.some((a) => a.includes(keyword))
      )) continue
      groups[s.role] = groups[s.role] || []
      groups[s.role].push(s)
    }
    // sort each group by field_hit_count desc
    for (const r in groups) {
      groups[r].sort((a, b) => b.field_hit_count - a.field_hit_count)
    }
    return groups
  }, [data, keyword])

  const baseColumns = [
    {
      title: '槽位',
      dataIndex: 'name',
      width: 220,
      render: (v: string, r: SlotLibraryEntry) => (
        <Space direction="vertical" size={0}>
          <Text code style={{ fontSize: 13 }}>{v}</Text>
          <Text type="secondary" style={{ fontSize: 11 }}>{r.cn_name}</Text>
        </Space>
      ),
    },
    {
      title: 'logical_type',
      dataIndex: 'logical_type',
      width: 220,
      render: (v: string) => (
        <Tag style={{ whiteSpace: 'normal', wordBreak: 'break-all', lineHeight: '16px' }}>{v}</Tag>
      ),
    },
    {
      title: 'role',
      dataIndex: 'role',
      width: 110,
      render: (v: string) => (
        <Tag color={ROLE_COLOR[v] || 'default'}>{ROLE_LABEL[v] || v}</Tag>
      ),
    },
    {
      title: '被引用 VT',
      dataIndex: 'used_by_vt_count',
      width: 100,
      sorter: (a: SlotLibraryEntry, b: SlotLibraryEntry) => a.used_by_vt_count - b.used_by_vt_count,
      render: (v: number) => <Tag color={v >= 50 ? 'red' : v >= 10 ? 'orange' : 'default'}>{v} VT</Tag>,
    },
    {
      title: '命中字段',
      dataIndex: 'field_hit_count',
      width: 300,
      sorter: (a: SlotLibraryEntry, b: SlotLibraryEntry) => a.field_hit_count - b.field_hit_count,
      defaultSortOrder: 'descend' as const,
      render: (v: number, r: SlotLibraryEntry) => (
        <Space size={4} wrap>
          <Tag color={hitColor(v)}>{v}</Tag>
          {r.auto_accepted_count > 0 && <Tag color="green">auto {r.auto_accepted_count}</Tag>}
          {r.needs_review_count > 0 && <Tag color="gold">review {r.needs_review_count}</Tag>}
          {r.conflict_count > 0 && <Tag color="magenta">conf {r.conflict_count}</Tag>}
        </Space>
      ),
    },
    {
      title: 'aliases',
      dataIndex: 'aliases',
      width: 380,
      ellipsis: { showTitle: false },
      render: (v: string[]) => (
        <Tooltip
          title={v.join(', ')}
          styles={{ root: { maxWidth: 520 } }}
        >
          <Text type="secondary" style={{ fontSize: 12 }}>
            {v.slice(0, 5).join(', ')}{v.length > 5 ? ` (+${v.length - 5})` : ''}
          </Text>
        </Tooltip>
      ),
    },
    {
      title: '操作',
      width: 160,
      render: (_: unknown, r: SlotLibraryEntry) => (
        <Space size={4}>
          <Button size="small" onClick={() => openDetail(r, true)}>详情</Button>
          <Popconfirm
            title={`删除 base 槽位 ${r.name}？`}
            description={
              <div>
                <div>引用它的 VT 有 <strong>{r.used_by_vt_count}</strong> 个</div>
                <div style={{ color: '#999', fontSize: 12, marginTop: 4 }}>
                  删除后需重跑 pipeline --from field_features 让下游生效
                </div>
              </div>
            }
            onConfirm={async () => {
              try {
                const resp = await api.deleteBaseSlot(r.name)
                message.success(`已删除 base 槽位: ${r.name}`)
                if (resp.next_action) setNextActionBanner(resp.next_action)
                reload()
              } catch (e: any) {
                message.error(e?.response?.data?.detail || String(e))
              }
            }}
          >
            <Button size="small" danger icon={<DeleteOutlined />} />
          </Popconfirm>
        </Space>
      ),
    },
  ]

  const extendedColumns = [
    {
      title: '槽位',
      dataIndex: 'name',
      width: 200,
      render: (v: string, r: SlotLibraryEntry) => (
        <Space direction="vertical" size={0}>
          <Text code style={{ fontSize: 12 }}>{v}</Text>
          <Text type="secondary" style={{ fontSize: 11 }}>{r.cn_name}</Text>
        </Space>
      ),
    },
    { title: 'logical_type', dataIndex: 'logical_type', width: 140 },
    {
      title: 'role',
      dataIndex: 'role',
      width: 90,
      render: (v: string) => (
        <Tag color={ROLE_COLOR[v] || 'default'}>{ROLE_LABEL[v] || v}</Tag>
      ),
    },
    {
      title: '命中字段',
      dataIndex: 'field_hit_count',
      width: 100,
      render: (v: number) => <Tag color={hitColor(v)}>{v}</Tag>,
    },
    {
      title: 'aliases',
      dataIndex: 'aliases',
      ellipsis: true,
      render: (v: string[]) => (
        <Text type="secondary" style={{ fontSize: 11 }}>{v.slice(0, 3).join(', ')}</Text>
      ),
    },
    {
      title: '',
      width: 60,
      render: (_: unknown, r: SlotLibraryEntry) => (
        <Button size="small" type="link" onClick={() => openDetail(r, false)}>详情</Button>
      ),
    },
  ]

  if (loading || !data) {
    return <div style={{ padding: 24 }}><Spin /></div>
  }

  return (
    <div style={{ padding: 24 }}>
      <Title level={3} style={{ marginTop: 0 }}>槽位库（Slot Library）</Title>
      <Paragraph type="secondary">
        所有语义槽位的全局视图。<Text code>base_slots</Text> 是跨 VT 复用的基础槽位；
        <Text code>extended</Text> 是每张 VT 独有的扩展槽位。编辑 base 的 aliases 后
        运行 <Text code>run_pipeline.py --from field_features</Text> 生效。
      </Paragraph>

      {nextActionBanner && (
        <Alert
          type="warning"
          showIcon
          closable
          message="槽位库已修改，需重跑归一才能生效"
          description={
            <Space direction="vertical">
              <Text>请在终端执行：</Text>
              <Text code copyable>{nextActionBanner}</Text>
            </Space>
          }
          style={{ marginBottom: 16 }}
          onClose={() => setNextActionBanner(null)}
        />
      )}

      <Space size={24} wrap style={{ marginBottom: 16 }}>
        <Statistic title="base 槽位" value={data.stats.base_count} />
        <Statistic title="domain 槽位" value={data.stats.domain_count} />
        <Statistic title="extended 槽位（总）" value={data.stats.extended_total} />
        <Statistic title="VT 数" value={data.stats.vt_count} />
      </Space>

      <Input.Search
        placeholder="搜索槽位名 / 中文名 / alias"
        value={keyword}
        onChange={(e) => setKeyword(e.target.value)}
        allowClear
        style={{ maxWidth: 420, marginBottom: 16 }}
      />

      <Tabs
        activeKey={activeTab}
        onChange={setActiveTab}
        items={[
          {
            key: 'base',
            label: `base（${data.base_slots.length}）`,
            children: (
              <Space direction="vertical" style={{ width: '100%' }} size={12}>
                <Space style={{ width: '100%', justifyContent: 'space-between' }}>
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    按 role 分组展示；命中字段 = 归一结果中被选为 top1 的字段数
                  </Text>
                  <Button
                    type="primary"
                    icon={<PlusOutlined />}
                    onClick={() => {
                      createForm.resetFields()
                      setSuggestWarnings([])
                      setSuggested(false)
                      setCreateOpen(true)
                    }}
                  >
                    新增 base 槽位
                  </Button>
                </Space>
                {ROLE_ORDER.filter((r) => baseByRole[r]?.length).map((role) => (
                  <Card
                    key={role}
                    size="small"
                    title={
                      <Space>
                        <Tag color={ROLE_COLOR[role]}>{ROLE_LABEL[role] || role}</Tag>
                        <Text type="secondary">{baseByRole[role].length} 个</Text>
                      </Space>
                    }
                  >
                    <Table
                      rowKey="name"
                      size="small"
                      pagination={false}
                      dataSource={baseByRole[role]}
                      columns={baseColumns as any}
                      scroll={{ x: 'max-content' }}
                    />
                  </Card>
                ))}
              </Space>
            ),
          },
          {
            key: 'domain',
            label: `domain（${data.domain_slots.length}）`,
            children: data.domain_slots.length === 0 ? (
              <Card><Text type="secondary">暂无 domain 槽位（通过 I-05b 接受 domain scope proposal 后会出现）</Text></Card>
            ) : (
              <Table
                rowKey="name"
                size="small"
                dataSource={data.domain_slots.filter((s) =>
                  !keyword || s.name.includes(keyword) || s.cn_name.includes(keyword)
                )}
                columns={baseColumns as any}
                pagination={{ pageSize: 30 }}
              />
            ),
          },
          {
            key: 'extended',
            label: `extended by VT（${data.stats.extended_total}）`,
            children: (() => {
              // 按 L1 → L2 → VT 分组
              type VTNode = ExtendedSlotGroup
              type L2Node = { name: string; vts: VTNode[]; total_extended: number }
              type L1Node = { name: string; l2s: Map<string, L2Node>; total_extended: number; total_vts: number }

              const l1Map = new Map<string, L1Node>()
              for (const g of data.extended_by_vt) {
                const l1 = g.l2_path[0] || '未分类'
                const l2 = g.l2_path[1] || '未分类'
                if (!l1Map.has(l1)) l1Map.set(l1, { name: l1, l2s: new Map(), total_extended: 0, total_vts: 0 })
                const l1n = l1Map.get(l1)!
                if (!l1n.l2s.has(l2)) l1n.l2s.set(l2, { name: l2, vts: [], total_extended: 0 })
                const l2n = l1n.l2s.get(l2)!
                l2n.vts.push(g)
                l2n.total_extended += g.extended_slots.length
                l1n.total_extended += g.extended_slots.length
                l1n.total_vts += 1
              }

              // 稳定排序：按 extended 数从多到少
              const sortedL1s = Array.from(l1Map.values()).sort(
                (a, b) => b.total_extended - a.total_extended,
              )

              return (
                <Collapse
                  items={sortedL1s.map((l1n) => ({
                    key: l1n.name,
                    label: (
                      <Space>
                        <Text strong style={{ fontSize: 14 }}>{l1n.name}</Text>
                        <Tag color="purple">{l1n.total_vts} VT</Tag>
                        <Tag color="blue">{l1n.total_extended} 个 extended</Tag>
                      </Space>
                    ),
                    children: (
                      <Collapse
                        size="small"
                        items={Array.from(l1n.l2s.values())
                          .sort((a, b) => b.total_extended - a.total_extended)
                          .map((l2n) => ({
                            key: l2n.name,
                            label: (
                              <Space>
                                <Text strong>{l2n.name}</Text>
                                <Tag color="default">{l2n.vts.length} VT</Tag>
                                <Tag color="blue">{l2n.total_extended} 个 extended</Tag>
                              </Space>
                            ),
                            children: (
                              <Collapse
                                size="small"
                                items={[...l2n.vts]
                                  .sort((a, b) => b.extended_slots.length - a.extended_slots.length)
                                  .map((g) => ({
                                    key: g.vt_id,
                                    label: (
                                      <Space>
                                        <Text strong>{g.topic}</Text>
                                        <Tag>{g.table_type}</Tag>
                                        <Text type="secondary" style={{ fontSize: 11 }}>{g.vt_id}</Text>
                                        <Tag color="blue">{g.extended_slots.length} 个 extended</Tag>
                                      </Space>
                                    ),
                                    children: (
                                      <Table
                                        rowKey="name"
                                        size="small"
                                        pagination={false}
                                        dataSource={g.extended_slots as any}
                                        columns={extendedColumns as any}
                                      />
                                    ),
                                  }))}
                              />
                            ),
                          }))}
                      />
                    ),
                  }))}
                />
              )
            })(),
          },
        ]}
      />

      <Drawer
        width={820}
        open={!!detailSlot}
        onClose={() => setDetailSlot(null)}
        title={
          detailSlot ? (
            <Space>
              <Text code>{detailSlot.name}</Text>
              <Text>{detailSlot.cn_name}</Text>
              <Tag color={ROLE_COLOR[detailSlot.role]}>{ROLE_LABEL[detailSlot.role] || detailSlot.role}</Tag>
              {detailIsBase ? <Tag color="magenta">base</Tag> : <Tag color="blue">extended</Tag>}
            </Space>
          ) : ''
        }
      >
        {detailSlot && (
          <Space direction="vertical" style={{ width: '100%' }} size={16}>
            <Descriptions column={2} size="small" bordered>
              <Descriptions.Item label="logical_type">{detailSlot.logical_type}</Descriptions.Item>
              <Descriptions.Item label="role">{detailSlot.role}</Descriptions.Item>
              <Descriptions.Item label="被引用 VT">{detailSlot.used_by_vt_count}</Descriptions.Item>
              <Descriptions.Item label="命中字段">{detailSlot.field_hit_count}</Descriptions.Item>
              <Descriptions.Item label="description" span={2}>
                {detailSlot.description || <Text type="secondary">(无)</Text>}
              </Descriptions.Item>
              <Descriptions.Item label="applicable_table_types" span={2}>
                <Space size={[0, 4]} wrap>
                  {detailSlot.applicable_table_types.map((t) => <Tag key={t}>{t}</Tag>)}
                </Space>
              </Descriptions.Item>
            </Descriptions>

            <div>
              <Title level={5}>aliases（{detailSlot.aliases.length}）</Title>
              <Space size={[4, 4]} wrap style={{ marginBottom: 8 }}>
                {detailSlot.aliases.map((a) => (
                  <Tag key={a} closable={detailIsBase} onClose={(e) => {
                    e.preventDefault()
                    if (detailIsBase) handleRemoveAlias(a)
                  }}>
                    {a}
                  </Tag>
                ))}
                {detailSlot.aliases.length === 0 && <Text type="secondary">(无)</Text>}
              </Space>
              {detailIsBase && (
                <Space.Compact style={{ width: '100%', maxWidth: 460 }}>
                  <Input
                    placeholder="新增 alias（例: xzjddm, sqjcwhdm）"
                    value={addAliasInput}
                    onChange={(e) => setAddAliasInput(e.target.value)}
                    onPressEnter={handleAddAlias}
                  />
                  <Button type="primary" icon={<PlusOutlined />} onClick={handleAddAlias}>追加</Button>
                </Space.Compact>
              )}
              {!detailIsBase && (
                <Text type="secondary" style={{ fontSize: 12 }}>
                  extended 槽位不支持在此编辑 aliases（走 I-01 重跑或人工改 slot_definitions.yaml）
                </Text>
              )}
            </div>

            <div>
              <Title level={5}>命中字段 Top 30（按 selected_score 降序）</Title>
              <Table
                rowKey={(r) => `${r.table_en}|${r.field_name}|${r.vt_id}`}
                size="small"
                pagination={false}
                loading={detailLoading}
                dataSource={detailFields}
                columns={[
                  { title: 'field', dataIndex: 'field_name', width: 180, render: (v) => <Text code style={{ fontSize: 11 }}>{v}</Text> },
                  { title: '注释', dataIndex: 'field_comment', width: 200, ellipsis: true },
                  { title: 'table', dataIndex: 'table_en', ellipsis: true, render: (v) => <Text type="secondary" style={{ fontSize: 11 }}>{v}</Text> },
                  { title: 'VT', dataIndex: 'vt_id', width: 140, render: (v) => <Tag color="blue" style={{ fontSize: 11 }}>{v}</Tag> },
                  {
                    title: 'status',
                    dataIndex: 'review_status',
                    width: 100,
                    render: (v: string) => {
                      const color = v === 'auto_accepted' ? 'green' : v === 'needs_review' ? 'gold' : v === 'conflict' ? 'magenta' : 'red'
                      return <Tag color={color} style={{ fontSize: 10 }}>{v}</Tag>
                    },
                  },
                  { title: 'score', dataIndex: 'selected_score', width: 70, render: (v: number | null) => v?.toFixed(3) ?? '-' },
                ]}
              />
            </div>
          </Space>
        )}
      </Drawer>

      {/* 新增 base 槽位 Modal */}
      <Modal
        title="新增 base 槽位"
        open={createOpen}
        onCancel={() => {
          setCreateOpen(false)
          setSuggestWarnings([])
          setSuggested(false)
        }}
        onOk={async () => {
          try {
            const values = await createForm.validateFields()
            const cnAliases: string[] = (values.cn_aliases || []).map((s: string) => s.trim()).filter(Boolean)
            const enAliases: string[] = String(values.en_aliases || '')
              .split(/[,，]/)
              .map((s: string) => s.trim())
              .filter(Boolean)
            const mergedAliases = Array.from(new Set([...cnAliases, ...enAliases]))
            const resp = await api.createBaseSlot({
              name: values.name.trim(),
              cn_name: values.cn_name.trim(),
              role: values.role,
              logical_type: values.logical_type?.trim() || 'text',
              description: values.description?.trim() || '',
              aliases: mergedAliases,
            })
            message.success(`已添加 base 槽位: ${resp.name}`)
            if (resp.next_action) setNextActionBanner(resp.next_action)
            setCreateOpen(false)
            setSuggestWarnings([])
            setSuggested(false)
            reload()
          } catch (e: any) {
            if (e?.errorFields) return // antd 校验错误
            message.error(e?.response?.data?.detail || String(e))
          }
        }}
        width={680}
        destroyOnClose
      >
        <Form form={createForm} layout="vertical" initialValues={{ role: 'filter' }}>
          <Form.Item label="中文名" name="cn_name" rules={[{ required: true, message: '必填' }]}>
            <Input placeholder="例：车辆品牌" />
          </Form.Item>
          <Form.Item
            label="中文别名（至少 1 个）"
            name="cn_aliases"
            rules={[
              { required: true, message: '至少填 1 个中文别名' },
              {
                validator: (_, v) =>
                  Array.isArray(v) && v.filter((s: string) => s && s.trim()).length > 0
                    ? Promise.resolve()
                    : Promise.reject(new Error('至少填 1 个中文别名')),
              },
            ]}
            extra={'回车或按逗号分隔，输入多个中文同义说法，例：车品牌 / 品牌名称 / 汽车品牌'}
          >
            <Select
              mode="tags"
              tokenSeparators={[',', '，', ' ']}
              placeholder={'例：车品牌、品牌名称、汽车品牌'}
              style={{ width: '100%' }}
            />
          </Form.Item>
          <Form.Item label="role" name="role" rules={[{ required: true }]}>
            <Select
              options={ROLE_ORDER.map((r) => ({ value: r, label: `${r} · ${ROLE_LABEL[r] || r}` }))}
            />
          </Form.Item>

          <div style={{ margin: '4px 0 16px', display: 'flex', alignItems: 'center', gap: 8 }}>
            <Button
              type="primary"
              ghost
              icon={<ThunderboltOutlined />}
              loading={suggestLoading}
              onClick={async () => {
                try {
                  const values = await createForm.validateFields(['cn_name', 'cn_aliases', 'role'])
                  const cnAliases: string[] = (values.cn_aliases || [])
                    .map((s: string) => s.trim())
                    .filter(Boolean)
                  setSuggestLoading(true)
                  setSuggestWarnings([])
                  const resp = await api.suggestBaseSlot({
                    cn_name: values.cn_name.trim(),
                    cn_aliases: cnAliases,
                    role: values.role,
                  })
                  createForm.setFieldsValue({
                    name: resp.name,
                    description: resp.description,
                    logical_type: resp.logical_type,
                    en_aliases: (resp.aliases || []).join(', '),
                  })
                  setSuggested(true)
                  setSuggestWarnings(resp.warnings || [])
                  if (!resp.warnings || resp.warnings.length === 0) {
                    message.success('AI 已生成，请审核下方字段')
                  } else {
                    message.warning('AI 已生成，但有警告，请检查')
                  }
                } catch (e: any) {
                  if (e?.errorFields) return
                  message.error(e?.response?.data?.detail || String(e))
                } finally {
                  setSuggestLoading(false)
                }
              }}
            >
              AI 生成英文名和描述
            </Button>
            <Text type="secondary" style={{ fontSize: 12 }}>
              喂入中文名 + 中文别名 + role，由 LLM 参考已有 base_slots 生成建议（禁止拼音直译）
            </Text>
          </div>

          {suggestWarnings.length > 0 && (
            <Alert
              type="warning"
              showIcon
              style={{ marginBottom: 16 }}
              message="命名警告"
              description={
                <ul style={{ margin: 0, paddingLeft: 18 }}>
                  {suggestWarnings.map((w, i) => (
                    <li key={i}>{w}</li>
                  ))}
                </ul>
              }
            />
          )}

          <Form.Item
            label={suggested ? 'AI 建议英文名（可编辑）' : '英文名 (snake_case)'}
            name="name"
            rules={[
              { required: true, message: '必填；可点 AI 生成' },
              { pattern: /^[a-z][a-z0-9_]*$/, message: 'snake_case，字母开头' },
            ]}
          >
            <Input placeholder="例：vehicle_brand" />
          </Form.Item>
          <Form.Item label="logical_type" name="logical_type">
            <Input placeholder={'text / code / datetime / amount / id …'} />
          </Form.Item>
          <Form.Item label={suggested ? 'AI 建议描述（可编辑）' : '描述'} name="description">
            <Input.TextArea rows={2} placeholder={'1 句话描述该槽位的业务含义'} />
          </Form.Item>
          <Form.Item
            label={suggested ? 'AI 建议英文 aliases（可编辑）' : '英文 aliases'}
            name="en_aliases"
            extra={'英文 / 拼音缩写同义词；提交时会和中文别名自动合并去重'}
          >
            <Input.TextArea rows={2} placeholder={'例：clpp, vehicle_brand, brand_name'} />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  )
}
