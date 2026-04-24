import { useEffect, useState } from 'react'
import {
  Alert,
  Button,
  Card,
  Cascader,
  Collapse,
  Drawer,
  Form,
  Input,
  Modal,
  Popconfirm,
  Progress,
  Select,
  Space,
  Spin,
  Statistic,
  Table,
  Tabs,
  Tag,
  Typography,
  message,
} from 'antd'
import { DeleteOutlined, EditOutlined, EyeOutlined, PlusOutlined } from '@ant-design/icons'
import { Link } from 'react-router-dom'
import { api } from '../api'
import { emitScaffoldChanged } from '../lib/events'

const { Title, Text, Paragraph } = Typography

interface TableRef { en: string; cn: string }
interface VTRef {
  vt_id: string
  topic: string
  table_type: string
  source_table_count: number
  source_tables: TableRef[]
  slot_status?: 'has_slots' | 'no_slots'
}

interface L2Node {
  name: string
  vt_count: number
  table_count: number
  in_tree: boolean
  tables: TableRef[]
  vts: VTRef[]
}

interface L1Node {
  name: string
  vt_count: number
  table_count: number
  in_tree: boolean
  children: L2Node[]
}

export default function Categories() {
  const [data, setData] = useState<L1Node[]>([])
  const [loading, setLoading] = useState(true)
  const [addL1Open, setAddL1Open] = useState(false)
  const [addL2Open, setAddL2Open] = useState<string | null>(null) // l1_name or null
  const [renameOpen, setRenameOpen] = useState<{ mode: 'l1' | 'l2'; old: string; l1?: string } | null>(null)
  const [form] = Form.useForm()
  const [nextActionBanner, setNextActionBanner] = useState<string | null>(null)
  const [detailNode, setDetailNode] = useState<{ l1: string; l2: L2Node } | null>(null)
  // 每张物理表的字段缓存：table_en → {loading, fields}
  const [tableFieldsCache, setTableFieldsCache] = useState<Record<string, { loading: boolean; fields: Array<{ field: string; type: string; comment: string; sample_data: string }>; total?: number }>>({})
  // VT 行编辑
  const [vtEditOpen, setVtEditOpen] = useState<VTRef | null>(null)
  const [vtEditForm] = Form.useForm()
  // I-16 Gap2: VT 详情缓存（slots + coverage）
  const [vtDetailCache, setVtDetailCache] = useState<Record<string, { loading: boolean; detail: any | null }>>({})
  // I-16 Imp3: 批量生成状态
  const [batchGenLoading, setBatchGenLoading] = useState(false)
  const [batchGenResult, setBatchGenResult] = useState<any[] | null>(null)

  const loadVTDetail = async (vt_id: string) => {
    if (vtDetailCache[vt_id]?.detail) return
    setVtDetailCache((s) => ({ ...s, [vt_id]: { loading: true, detail: null } }))
    try {
      const resp = await api.getVT(vt_id)
      setVtDetailCache((s) => ({ ...s, [vt_id]: { loading: false, detail: resp } }))
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
      setVtDetailCache((s) => ({ ...s, [vt_id]: { loading: false, detail: null } }))
    }
  }

  // 物理表归属变更：把 table_en 加入 / 移出指定 VT 的 candidate_tables
  const [tableReassignBusy, setTableReassignBusy] = useState<string | null>(null)
  const reassignTable = async (table: TableRef, oldVTIds: string[], newVTIds: string[]) => {
    setTableReassignBusy(table.en)
    const toAdd = newVTIds.filter((v) => !oldVTIds.includes(v))
    const toRemove = oldVTIds.filter((v) => !newVTIds.includes(v))
    try {
      // 串行处理，避免并发 PUT 竞争
      for (const vt_id of toAdd) {
        const detail = await api.getVT(vt_id)
        const tables = (detail.source_tables_with_fields || []).map((s: any) => ({ en: s.en, cn: s.cn }))
        if (!tables.some((t: TableRef) => t.en === table.en)) {
          tables.push({ en: table.en, cn: table.cn })
          await api.updateVTMeta(vt_id, { candidate_tables: tables })
        }
      }
      for (const vt_id of toRemove) {
        const detail = await api.getVT(vt_id)
        const tables = (detail.source_tables_with_fields || [])
          .filter((s: any) => s.en !== table.en)
          .map((s: any) => ({ en: s.en, cn: s.cn }))
        await api.updateVTMeta(vt_id, { candidate_tables: tables })
      }
      message.success(`${table.en}: +${toAdd.length} / -${toRemove.length}`)
      emitScaffoldChanged({ source: 'categories_reassign', action: 'table_reassign', extra: { table_en: table.en } })
      // 刷新分类树数据以同步 detailNode
      const fresh = await api.getCategories()
      setData(fresh.categories)
      // 更新打开的 drawer 的节点引用
      if (detailNode) {
        const freshL1 = fresh.categories.find((l1: L1Node) => l1.name === detailNode.l1)
        const freshL2 = freshL1?.children.find((l2: L2Node) => l2.name === detailNode.l2.name)
        if (freshL2) setDetailNode({ l1: detailNode.l1, l2: freshL2 })
      }
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    } finally {
      setTableReassignBusy(null)
    }
  }

  const openVTEdit = (vt: VTRef) => {
    if (!detailNode) return
    vtEditForm.setFieldsValue({
      topic: vt.topic,
      table_type: vt.table_type,
      l_path: [detailNode.l1, detailNode.l2.name],
    })
    setVtEditOpen(vt)
  }

  const handleVTEditSave = async () => {
    if (!vtEditOpen) return
    const values = await vtEditForm.validateFields()
    const path: string[] = values.l_path || []
    try {
      const resp = await api.updateVTMeta(vtEditOpen.vt_id, {
        topic: values.topic,
        table_type: values.table_type,
        l2_path: path.filter(Boolean),
      })
      message.success(`已保存: ${vtEditOpen.vt_id}`)
      if (resp.next_action) setNextActionBanner(resp.next_action)
      emitScaffoldChanged({ source: 'categories-drawer', action: 'update_meta', extra: { vt_id: vtEditOpen.vt_id } })
      setVtEditOpen(null)
      vtEditForm.resetFields()
      reload()
      // 重新打开同一个 L2 drawer（刷新数据）
      setDetailNode(null)
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    }
  }

  const handleVTDelete = async (vt: VTRef) => {
    try {
      const resp = await api.deleteVT(vt.vt_id)
      message.success(`已删除 VT: ${vt.vt_id}`)
      if (resp.next_action) setNextActionBanner(resp.next_action)
      emitScaffoldChanged({ source: 'categories-drawer', action: 'delete_vt', extra: { vt_id: vt.vt_id } })
      reload()
      setDetailNode(null)
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    }
  }

  const handleBatchGenerateL2 = async () => {
    if (!detailNode) return
    setBatchGenLoading(true)
    setBatchGenResult(null)
    try {
      const resp = await api.generateSlotsForL2(detailNode.l1, detailNode.l2.name, { only_missing: true })
      setBatchGenResult(resp.results)
      const okCount = resp.results.filter((r) => r.ok).length
      message.success(`批量生成完成：成功 ${okCount} / 失败 ${resp.results.length - okCount}`)
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    } finally {
      setBatchGenLoading(false)
    }
  }

  const loadTableFields = async (table_en: string) => {
    if (tableFieldsCache[table_en]) return
    setTableFieldsCache((s) => ({ ...s, [table_en]: { loading: true, fields: [] } }))
    try {
      const resp = await api.getTableFields(table_en, 500)
      setTableFieldsCache((s) => ({ ...s, [table_en]: { loading: false, fields: resp.fields, total: resp.field_count } }))
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
      setTableFieldsCache((s) => ({ ...s, [table_en]: { loading: false, fields: [] } }))
    }
  }

  const reload = () => {
    setLoading(true)
    api.getCategories().then((r) => setData(r.categories)).finally(() => setLoading(false))
  }
  useEffect(reload, [])

  const handleAddL1 = async () => {
    const values = await form.validateFields()
    try {
      await api.addCategoryL1(values.name.trim())
      message.success(`已新增 L1: ${values.name}`)
      setAddL1Open(false)
      form.resetFields()
      reload()
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    }
  }

  const handleAddL2 = async () => {
    if (!addL2Open) return
    const values = await form.validateFields()
    try {
      await api.addCategoryL2(addL2Open, values.name.trim())
      message.success(`已新增 L2: ${addL2Open}/${values.name}`)
      setAddL2Open(null)
      form.resetFields()
      reload()
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    }
  }

  const handleRename = async () => {
    if (!renameOpen) return
    const values = await form.validateFields()
    try {
      let resp: any
      if (renameOpen.mode === 'l1') {
        resp = await api.renameCategoryL1(renameOpen.old, values.new_name.trim())
      } else {
        resp = await api.renameCategoryL2(renameOpen.l1!, renameOpen.old, values.new_name.trim())
      }
      message.success(`已重命名，级联更新了 ${resp.affected_vt_count} 张 VT`)
      if (resp.next_action) setNextActionBanner(resp.next_action)
      if (resp.affected_vt_count > 0) {
        emitScaffoldChanged({
          source: 'categories',
          action: renameOpen.mode === 'l1' ? 'rename_l1' : 'rename_l2',
          extra: { old: renameOpen.old, new: values.new_name, affected: resp.affected_vt_count },
        })
      }
      setRenameOpen(null)
      form.resetFields()
      reload()
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    }
  }

  const handleDeleteL1 = async (name: string) => {
    try {
      await api.deleteCategoryL1(name)
      message.success(`已删除 L1: ${name}`)
      reload()
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    }
  }

  const handleDeleteL2 = async (l1: string, l2: string) => {
    try {
      await api.deleteCategoryL2(l1, l2)
      message.success(`已删除 L2: ${l1}/${l2}`)
      reload()
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    }
  }

  if (loading) {
    return <div style={{ padding: 24 }}><Spin /></div>
  }

  const totalL1 = data.length
  const totalL2 = data.reduce((sum, c) => sum + c.children.length, 0)
  const totalVTs = data.reduce((sum, c) => sum + c.vt_count, 0)
  const totalTables = data.reduce((sum, c) => sum + c.table_count, 0)

  return (
    <div style={{ padding: 24 }}>
      <Title level={3} style={{ marginTop: 0 }}>分类树编辑</Title>
      <Paragraph type="secondary">
        管理 <Text code>data/phrase_2/二期表分类树.json</Text>。重命名会级联更新所有 VT 的 l2_path；删除前必须先清空引用的 VT。
        改动后运行 <Text code>run_pipeline.py --from slot_definitions</Text> 同步下游。
      </Paragraph>

      {nextActionBanner && (
        <Alert
          type="warning"
          showIcon
          closable
          message="分类树已修改并级联更新 scaffold"
          description={<Space direction="vertical">
            <Text>请在终端执行：</Text>
            <Text code copyable>{nextActionBanner}</Text>
          </Space>}
          style={{ marginBottom: 16 }}
          onClose={() => setNextActionBanner(null)}
        />
      )}

      <Space size={24} wrap style={{ marginBottom: 16 }}>
        <Statistic title="L1 节点" value={totalL1} />
        <Statistic title="L2 节点" value={totalL2} />
        <Statistic title="物理表" value={totalTables} />
        <Statistic title="虚拟表" value={totalVTs} />
      </Space>

      <Space style={{ marginBottom: 16 }}>
        <Button type="primary" icon={<PlusOutlined />} onClick={() => { form.resetFields(); setAddL1Open(true) }}>
          新增 L1
        </Button>
      </Space>

      <Collapse
        defaultActiveKey={data.map((c) => c.name)}
        items={data.map((c) => ({
          key: c.name,
          label: (
            <Space style={{ width: '100%', justifyContent: 'space-between' }}>
              <Space>
                <Text strong style={{ fontSize: 14 }}>{c.name}</Text>
                <Tag>{c.children.length} L2</Tag>
                <Tag color="purple">{c.table_count} 物理表</Tag>
                <Tag color="blue">{c.vt_count} VT</Tag>
                {!c.in_tree && <Tag color="orange">非树节点</Tag>}
              </Space>
              <Space>
                <Button
                  size="small"
                  icon={<PlusOutlined />}
                  onClick={(e) => { e.stopPropagation(); form.resetFields(); setAddL2Open(c.name) }}
                >
                  新增 L2
                </Button>
                <Button
                  size="small"
                  icon={<EditOutlined />}
                  onClick={(e) => { e.stopPropagation(); form.resetFields(); form.setFieldsValue({ new_name: c.name }); setRenameOpen({ mode: 'l1', old: c.name }) }}
                >
                  重命名
                </Button>
                <Popconfirm
                  title={`删除 L1 "${c.name}"？`}
                  description={c.vt_count > 0
                    ? <Text type="danger">还有 {c.vt_count} 张 VT 引用，无法删除</Text>
                    : '无 VT 引用，可以删除'}
                  disabled={c.vt_count > 0}
                  onConfirm={(e) => { e?.stopPropagation(); handleDeleteL1(c.name) }}
                  onCancel={(e) => e?.stopPropagation()}
                >
                  <Button
                    size="small"
                    danger
                    icon={<DeleteOutlined />}
                    disabled={c.vt_count > 0}
                    onClick={(e) => e.stopPropagation()}
                  >
                    删除
                  </Button>
                </Popconfirm>
              </Space>
            </Space>
          ),
          children: c.children.length === 0 ? (
            <Text type="secondary">该 L1 下暂无 L2 节点</Text>
          ) : (
            <Space direction="vertical" style={{ width: '100%' }}>
              {c.children.map((l2) => (
                <Card key={l2.name} size="small">
                  <Space style={{ width: '100%', justifyContent: 'space-between' }}>
                    <Space>
                      <Text>{l2.name}</Text>
                      <Tag color="purple">{l2.table_count} 物理表</Tag>
                      <Tag color="blue">{l2.vt_count} VT</Tag>
                      {!l2.in_tree && <Tag color="orange">非树节点（仅 scaffold 出现）</Tag>}
                    </Space>
                    <Space>
                      <Button
                        size="small"
                        icon={<EyeOutlined />}
                        onClick={() => setDetailNode({ l1: c.name, l2 })}
                      >
                        详情
                      </Button>
                      <Button
                        size="small"
                        icon={<EditOutlined />}
                        onClick={() => { form.resetFields(); form.setFieldsValue({ new_name: l2.name }); setRenameOpen({ mode: 'l2', old: l2.name, l1: c.name }) }}
                      >
                        重命名
                      </Button>
                      <Popconfirm
                        title={`删除 L2 "${c.name}/${l2.name}"？`}
                        description={l2.vt_count > 0
                          ? <Text type="danger">还有 {l2.vt_count} 张 VT 引用，无法删除</Text>
                          : '无 VT 引用，可以删除'}
                        disabled={l2.vt_count > 0}
                        onConfirm={() => handleDeleteL2(c.name, l2.name)}
                      >
                        <Button size="small" danger icon={<DeleteOutlined />} disabled={l2.vt_count > 0}>
                          删除
                        </Button>
                      </Popconfirm>
                    </Space>
                  </Space>
                </Card>
              ))}
            </Space>
          ),
        }))}
      />

      {/* 新增 L1 Modal */}
      <Modal title="新增 L1 分类" open={addL1Open} onOk={handleAddL1} onCancel={() => setAddL1Open(false)} destroyOnClose>
        <Form form={form} layout="vertical">
          <Form.Item name="name" label="L1 名称" rules={[{ required: true }]}>
            <Input placeholder="例：新业务域" />
          </Form.Item>
        </Form>
      </Modal>

      {/* 新增 L2 Modal */}
      <Modal
        title={`在 "${addL2Open}" 下新增 L2`}
        open={!!addL2Open}
        onOk={handleAddL2}
        onCancel={() => setAddL2Open(null)}
        destroyOnClose
      >
        <Form form={form} layout="vertical">
          <Form.Item name="name" label="L2 名称" rules={[{ required: true }]}>
            <Input placeholder="例：新子分类" />
          </Form.Item>
        </Form>
      </Modal>

      {/* 重命名 Modal */}
      <Modal
        title={renameOpen ? `重命名 ${renameOpen.mode.toUpperCase()}: ${renameOpen.old}` : ''}
        open={!!renameOpen}
        onOk={handleRename}
        onCancel={() => setRenameOpen(null)}
        destroyOnClose
      >
        <Alert type="info" showIcon style={{ marginBottom: 12 }}
          message="重命名会级联更新所有 VT 的 l2_path，之后需重跑 pipeline" />
        <Form form={form} layout="vertical">
          <Form.Item name="new_name" label="新名称" rules={[{ required: true }]}>
            <Input />
          </Form.Item>
        </Form>
      </Modal>

      {/* VT 内联编辑 Modal */}
      <Modal
        title={vtEditOpen ? `编辑 VT: ${vtEditOpen.vt_id}` : ''}
        open={!!vtEditOpen}
        onOk={handleVTEditSave}
        onCancel={() => { setVtEditOpen(null); vtEditForm.resetFields() }}
        destroyOnClose
        width={600}
      >
        <Form form={vtEditForm} layout="vertical">
          <Form.Item name="topic" label="topic（主题）" rules={[{ required: true }]}>
            <Input />
          </Form.Item>
          <Form.Item name="table_type" label="table_type" rules={[{ required: true }]}>
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
              options={data.map((c) => ({
                value: c.name,
                label: c.name,
                children: c.children.map((ch) => ({ value: ch.name, label: ch.name })),
              }))}
              changeOnSelect
              showSearch={{
                filter: (input, path) => path.some((p) => String(p.label).toLowerCase().includes(input.toLowerCase())),
              }}
            />
          </Form.Item>
          <Alert
            type="info"
            showIcon
            message="如需改 grain_desc / 源表，请去 VT 详情页操作"
          />
        </Form>
      </Modal>

      {/* L2 详情 Drawer：展示物理表 + VT */}
      <Drawer
        width={760}
        open={!!detailNode}
        onClose={() => setDetailNode(null)}
        title={detailNode ? `${detailNode.l1} / ${detailNode.l2.name}` : ''}
      >
        {detailNode && (
          <Space direction="vertical" style={{ width: '100%' }} size={16}>
            <Space size={16}>
              <Tag color="purple">{detailNode.l2.table_count} 物理表</Tag>
              <Tag color="blue">{detailNode.l2.vt_count} VT</Tag>
              {!detailNode.l2.in_tree && <Tag color="orange">非树节点</Tag>}
            </Space>

            <div>
              <Title level={5}>物理表（{detailNode.l2.tables.length}）</Title>
              <Text type="secondary" style={{ fontSize: 12 }}>点击行左侧箭头展开看字段 + 样例值</Text>
              {detailNode.l2.tables.length === 0 ? (
                <Text type="secondary">无</Text>
              ) : (
                <Table
                  size="small"
                  rowKey="en"
                  pagination={{ pageSize: 10 }}
                  dataSource={detailNode.l2.tables}
                  columns={[
                    { title: '表名', dataIndex: 'en', width: 240, render: (v) => <Text code style={{ fontSize: 11 }}>{v}</Text> },
                    { title: '中文名', dataIndex: 'cn', width: 160, ellipsis: true },
                    {
                      title: (
                        <Space size={4}>
                          <span>归属 VT（可多选）</span>
                          <Text type="secondary" style={{ fontSize: 10 }}>
                            选了之后双向同步到 VT 的 candidate_tables
                          </Text>
                        </Space>
                      ),
                      key: 'belongs_to',
                      render: (_: unknown, record: TableRef) => {
                        // 计算该 table 当前归属到哪些 VT
                        const currentVTs = detailNode.l2.vts
                          .filter((v) => (v.source_tables || []).some((t) => t.en === record.en))
                          .map((v) => v.vt_id)
                        return (
                          <Select
                            mode="multiple"
                            size="small"
                            style={{ minWidth: 260, width: '100%' }}
                            placeholder="未归属任何 VT"
                            value={currentVTs}
                            loading={tableReassignBusy === record.en}
                            disabled={tableReassignBusy !== null && tableReassignBusy !== record.en}
                            options={detailNode.l2.vts.map((v) => ({
                              value: v.vt_id,
                              label: v.topic,
                            }))}
                            onChange={(newVTIds: string[]) => {
                              reassignTable(record, currentVTs, newVTIds)
                            }}
                            maxTagCount="responsive"
                            optionFilterProp="label"
                          />
                        )
                      },
                    },
                  ]}
                  expandable={{
                    onExpand: (expanded, record) => {
                      if (expanded) loadTableFields(record.en)
                    },
                    expandedRowRender: (record) => {
                      const cache = tableFieldsCache[record.en]
                      if (!cache || cache.loading) {
                        return <Spin />
                      }
                      if (cache.fields.length === 0) {
                        return <Text type="secondary">暂无字段数据</Text>
                      }
                      return (
                        <div style={{ padding: '0 16px' }}>
                          <Text type="secondary" style={{ fontSize: 12 }}>
                            显示 {cache.fields.length}{cache.total && cache.fields.length < cache.total ? ` / ${cache.total}` : ''} 个字段
                          </Text>
                          <Table
                            size="small"
                            rowKey="field"
                            pagination={{ pageSize: 20 }}
                            dataSource={cache.fields}
                            columns={[
                              {
                                title: 'field',
                                dataIndex: 'field',
                                width: 180,
                                render: (v) => <Text code style={{ fontSize: 11 }}>{v}</Text>,
                              },
                              {
                                title: 'type',
                                dataIndex: 'type',
                                width: 90,
                                render: (v) => <Tag style={{ fontSize: 11 }}>{v}</Tag>,
                              },
                              {
                                title: 'comment',
                                dataIndex: 'comment',
                                width: 180,
                                ellipsis: true,
                              },
                              {
                                title: 'sample',
                                dataIndex: 'sample_data',
                                ellipsis: true,
                                render: (v: string) => (
                                  <Text type="secondary" style={{ fontSize: 11 }}>
                                    {v || '-'}
                                  </Text>
                                ),
                              },
                            ]}
                          />
                        </div>
                      )
                    },
                  }}
                />
              )}
            </div>

            <div>
              <Space style={{ width: '100%', justifyContent: 'space-between', marginBottom: 8 }}>
                <Title level={5} style={{ margin: 0 }}>虚拟表（{detailNode.l2.vts.length}）</Title>
                <Space>
                  {(() => {
                    const missing = detailNode.l2.vts.filter((v) => v.slot_status === 'no_slots').length
                    return (
                      <>
                        {missing > 0 && <Tag color="gold">⚠ {missing} 个未生成槽位</Tag>}
                        <Button
                          type="primary"
                          size="small"
                          loading={batchGenLoading}
                          disabled={missing === 0}
                          onClick={handleBatchGenerateL2}
                        >
                          批量 LLM 生成（仅未生成的）
                        </Button>
                      </>
                    )
                  })()}
                </Space>
              </Space>
              <Text type="secondary" style={{ fontSize: 12 }}>点击行左侧箭头看源表 / 槽位 / 覆盖率（3 tab）</Text>
              {batchGenResult && (
                <Alert
                  type="info"
                  style={{ marginTop: 8, marginBottom: 8 }}
                  message={`批量生成结果：${batchGenResult.filter((r) => r.ok).length} 成功 / ${batchGenResult.filter((r) => !r.ok).length} 失败`}
                  description={
                    <div style={{ maxHeight: 160, overflow: 'auto' }}>
                      {batchGenResult.map((r) => (
                        <div key={r.vt_id} style={{ fontSize: 11 }}>
                          {r.ok ? '✅' : '❌'} <Text code>{r.vt_id}</Text> {r.topic} · {r.elapsed_sec}s
                          {r.ok ? ` · ${r.slots?.length || 0} 槽位` : ` · ${r.error}`}
                        </div>
                      ))}
                      <Text type="secondary" style={{ fontSize: 11 }}>
                        结果仅为预览。进入每个 VT 详情页按「保存槽位」才真正写入。
                      </Text>
                    </div>
                  }
                  closable
                  onClose={() => setBatchGenResult(null)}
                />
              )}
              {detailNode.l2.vts.length === 0 ? (
                <Text type="secondary">无</Text>
              ) : (
                <Table
                  size="small"
                  rowKey="vt_id"
                  pagination={false}
                  dataSource={detailNode.l2.vts}
                  columns={[
                    {
                      title: 'vt_id',
                      dataIndex: 'vt_id',
                      width: 150,
                      render: (v) => (
                        <Link to={`/vt/${v}`} onClick={() => setDetailNode(null)}>
                          <Text code style={{ fontSize: 11 }}>{v}</Text>
                        </Link>
                      ),
                    },
                    {
                      title: 'topic',
                      render: (_: unknown, r: VTRef) => (
                        <Space>
                          <span>{r.topic}</span>
                          {r.slot_status === 'has_slots' ? (
                            <Tag color="green" style={{ fontSize: 10 }}>有槽位</Tag>
                          ) : (
                            <Tag color="gold" style={{ fontSize: 10 }}>⚠ 无槽位</Tag>
                          )}
                        </Space>
                      ),
                    },
                    { title: 'type', dataIndex: 'table_type', width: 70, render: (v) => <Tag>{v}</Tag> },
                    {
                      title: '源表',
                      dataIndex: 'source_table_count',
                      width: 60,
                      render: (v) => <Tag color="blue">{v}</Tag>,
                    },
                    {
                      title: '操作',
                      width: 150,
                      render: (_: unknown, r: VTRef) => (
                        <Space size={4}>
                          <Button size="small" icon={<EditOutlined />} onClick={() => openVTEdit(r)}>
                            编辑
                          </Button>
                          <Popconfirm
                            title={`删除 ${r.vt_id}？`}
                            description={
                              <div>
                                <div>从 scaffold 移除该 VT</div>
                                <div style={{ color: '#d46b08', marginTop: 4 }}>
                                  需运行 pipeline --from slot_definitions 同步下游
                                </div>
                              </div>
                            }
                            okText="删除"
                            okButtonProps={{ danger: true }}
                            onConfirm={() => handleVTDelete(r)}
                          >
                            <Button size="small" danger icon={<DeleteOutlined />}>删除</Button>
                          </Popconfirm>
                        </Space>
                      ),
                    },
                  ]}
                  expandable={{
                    onExpand: (expanded, record) => {
                      if (expanded) loadVTDetail(record.vt_id)
                    },
                    expandedRowRender: (record) => {
                      const detailCache = vtDetailCache[record.vt_id]
                      if (record.source_tables.length === 0 && !detailCache?.detail) {
                        return <Text type="secondary">该 VT 没有源表（可能是新建后未配置）</Text>
                      }
                      return (
                        <div style={{ padding: '0 16px' }}>
                          <Tabs
                            size="small"
                            items={[
                              {
                                key: 'sources',
                                label: `源表（${record.source_tables.length}）`,
                                children: (
                                  <Table
                                    size="small"
                                    rowKey="en"
                                    pagination={false}
                                    dataSource={record.source_tables}
                                    columns={[
                                      {
                                        title: '表名',
                                        dataIndex: 'en',
                                        width: 340,
                                        render: (v) => <Text code style={{ fontSize: 11 }}>{v}</Text>,
                                      },
                                      { title: '中文名', dataIndex: 'cn' },
                                    ]}
                                    expandable={{
                                      onExpand: (expanded, r) => {
                                        if (expanded) loadTableFields(r.en)
                                      },
                                      expandedRowRender: (r) => {
                                        const cache = tableFieldsCache[r.en]
                                        if (!cache || cache.loading) return <Spin />
                                        if (cache.fields.length === 0) return <Text type="secondary">暂无字段</Text>
                                        return (
                                          <Table
                                            size="small"
                                            rowKey="field"
                                            pagination={{ pageSize: 15 }}
                                            dataSource={cache.fields}
                                            columns={[
                                              { title: 'field', dataIndex: 'field', width: 160, render: (v) => <Text code style={{ fontSize: 11 }}>{v}</Text> },
                                              { title: 'type', dataIndex: 'type', width: 80, render: (v) => <Tag style={{ fontSize: 11 }}>{v}</Tag> },
                                              { title: 'comment', dataIndex: 'comment', ellipsis: true },
                                              {
                                                title: 'sample',
                                                dataIndex: 'sample_data',
                                                ellipsis: true,
                                                render: (v: string) => <Text type="secondary" style={{ fontSize: 11 }}>{v || '-'}</Text>,
                                              },
                                            ]}
                                          />
                                        )
                                      },
                                    }}
                                  />
                                ),
                              },
                              {
                                key: 'slots',
                                label: (
                                  <span>
                                    槽位
                                    {detailCache?.detail && (
                                      <Tag color="blue" style={{ marginLeft: 8, fontSize: 10 }}>
                                        {detailCache.detail.slots?.length || 0}
                                      </Tag>
                                    )}
                                  </span>
                                ),
                                children: detailCache?.loading ? (
                                  <Spin />
                                ) : !detailCache?.detail || (detailCache.detail.slots || []).length === 0 ? (
                                  <Alert
                                    type="warning"
                                    showIcon
                                    message="该 VT 还没有槽位"
                                    description="进入 VT 详情页点「LLM 生成槽位」按钮自动生成"
                                  />
                                ) : (
                                  <Table
                                    size="small"
                                    rowKey={(r: any) => r.name}
                                    pagination={false}
                                    dataSource={detailCache.detail.slots}
                                    columns={[
                                      {
                                        title: 'name',
                                        dataIndex: 'name',
                                        width: 180,
                                        render: (v) => <Text code style={{ fontSize: 11 }}>{v}</Text>,
                                      },
                                      {
                                        title: 'from',
                                        dataIndex: 'from',
                                        width: 80,
                                        render: (v) => <Tag color={v === 'base' ? 'magenta' : 'blue'}>{v}</Tag>,
                                      },
                                      { title: 'role', dataIndex: 'role', width: 100 },
                                      { title: 'cn_name', dataIndex: 'cn_name' },
                                      { title: 'logical_type', dataIndex: 'logical_type', width: 140 },
                                    ]}
                                  />
                                ),
                              },
                              {
                                key: 'coverage',
                                label: '覆盖率',
                                children: detailCache?.loading ? (
                                  <Spin />
                                ) : !detailCache?.detail?.source_tables_with_fields ? (
                                  <Text type="secondary">无数据</Text>
                                ) : (
                                  <Table
                                    size="small"
                                    rowKey="en"
                                    pagination={false}
                                    dataSource={detailCache.detail.source_tables_with_fields}
                                    columns={[
                                      {
                                        title: '表',
                                        dataIndex: 'en',
                                        render: (v, r: any) => (
                                          <Space direction="vertical" size={0}>
                                            <Text code style={{ fontSize: 11 }}>{v}</Text>
                                            <Text type="secondary" style={{ fontSize: 11 }}>{r.cn}</Text>
                                          </Space>
                                        ),
                                      },
                                      { title: '字段', dataIndex: 'field_count', width: 70, render: (v) => <Tag>{v}</Tag> },
                                      {
                                        title: '命中',
                                        width: 120,
                                        render: (_: unknown, r: any) => (
                                          <Text>{r.used_count}/{r.sample_size}</Text>
                                        ),
                                      },
                                      {
                                        title: '覆盖率',
                                        dataIndex: 'coverage_ratio',
                                        width: 180,
                                        render: (v: number) => {
                                          const pct = (v || 0) * 100
                                          return (
                                            <Progress
                                              percent={pct}
                                              size="small"
                                              status={pct >= 60 ? 'success' : pct >= 40 ? 'normal' : 'exception'}
                                              format={(p) => `${p?.toFixed(0)}%`}
                                            />
                                          )
                                        },
                                      },
                                    ]}
                                  />
                                ),
                              },
                            ]}
                          />
                        </div>
                      )
                    },
                  }}
                />
              )}
            </div>
          </Space>
        )}
      </Drawer>
    </div>
  )
}
