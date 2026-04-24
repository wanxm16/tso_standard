import { useEffect, useMemo, useState } from 'react'
import {
  Alert,
  Button,
  Card,
  Descriptions,
  Drawer,
  Form,
  Input,
  Modal,
  Popconfirm,
  Radio,
  Select,
  Space,
  Spin,
  Statistic,
  Table,
  Tag,
  Typography,
  message,
} from 'antd'
import { api } from '../api'
import type { SlotProposal, SlotProposalListResponse } from '../types'

const { Title, Text, Paragraph } = Typography

const SCOPE_COLOR: Record<string, string> = {
  base: 'magenta',
  domain: 'gold',
  vt_local: 'blue',
}

const STATUS_COLOR: Record<string, string> = {
  pending: 'default',
  accepted: 'green',
  rejected: 'red',
  renamed: 'cyan',
}

const STATUS_LABEL: Record<string, string> = {
  pending: '待审核',
  accepted: '已接受',
  rejected: '已拒绝',
  renamed: '已改名接受',
}

export default function NewSlotCandidates() {
  const [data, setData] = useState<SlotProposalListResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [filterScope, setFilterScope] = useState<string | undefined>()
  const [filterStatus, setFilterStatus] = useState<string | undefined>('pending')
  const [filterSource, setFilterSource] = useState<string | undefined>()
  const [detailOpen, setDetailOpen] = useState(false)
  const [detail, setDetail] = useState<SlotProposal | null>(null)
  const [renameOpen, setRenameOpen] = useState(false)
  const [renameForm] = Form.useForm()
  const [renameTarget, setRenameTarget] = useState<SlotProposal | null>(null)
  const [nextActionBanner, setNextActionBanner] = useState<string | null>(null)
  const [selectedKeys, setSelectedKeys] = useState<React.Key[]>([])
  const [batchSubmitting, setBatchSubmitting] = useState(false)

  const reload = () => {
    setLoading(true)
    api
      .slotProposals({
        scope: filterScope,
        status: filterStatus,
        source: filterSource,
        limit: 1000,
      })
      .then(setData)
      .finally(() => setLoading(false))
  }

  useEffect(reload, [filterScope, filterStatus, filterSource])

  const proposals = data?.proposals || []
  const meta = data?.meta || {}

  const handleAccept = async (p: SlotProposal) => {
    try {
      const resp = await api.applySlotProposal({ proposal_id: p.proposal_id, decision: 'accept' })
      message.success(`已接受: ${p.name}`)
      if (resp.next_action) setNextActionBanner(resp.next_action)
      reload()
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    }
  }

  const handleReject = async (p: SlotProposal) => {
    try {
      await api.applySlotProposal({ proposal_id: p.proposal_id, decision: 'reject' })
      message.success(`已拒绝: ${p.name}`)
      reload()
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    }
  }

  const handleBatchApply = async (decision: 'accept' | 'reject') => {
    const targets = proposals.filter((p) => selectedKeys.includes(p.proposal_id))
    if (targets.length === 0) {
      message.info('未选择任何候选')
      return
    }
    setBatchSubmitting(true)
    try {
      const results = await Promise.allSettled(
        targets.map((p) => api.applySlotProposal({ proposal_id: p.proposal_id, decision })),
      )
      const ok = results.filter((r) => r.status === 'fulfilled').length
      const fail = results.length - ok
      const verb = decision === 'accept' ? '接受' : '拒绝'
      if (fail === 0) {
        message.success(`批量${verb}成功：${ok} 条`)
      } else {
        message.warning(`批量${verb}：成功 ${ok}，失败 ${fail}`)
      }
      // 取 next_action（第一个成功回的）
      const firstOk = results.find((r) => r.status === 'fulfilled') as any
      if (firstOk?.value?.next_action) setNextActionBanner(firstOk.value.next_action)
      setSelectedKeys([])
      reload()
    } finally {
      setBatchSubmitting(false)
    }
  }

  const handleRenameSubmit = async () => {
    if (!renameTarget) return
    const values = await renameForm.validateFields()
    try {
      const resp = await api.applySlotProposal({
        proposal_id: renameTarget.proposal_id,
        decision: 'rename',
        renamed_to: values.renamed_to,
        renamed_cn_name: values.renamed_cn_name,
      })
      message.success(`已改名接受: ${values.renamed_to}`)
      if (resp.next_action) setNextActionBanner(resp.next_action)
      setRenameOpen(false)
      setRenameTarget(null)
      renameForm.resetFields()
      reload()
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    }
  }

  const columns = useMemo(
    () => [
      {
        title: '槽位名',
        dataIndex: 'name',
        width: 220,
        render: (v: string, r: SlotProposal) => (
          <Space direction="vertical" size={0}>
            <Text code>{v}</Text>
            <Text type="secondary" style={{ fontSize: 11 }}>{r.cn_name}</Text>
          </Space>
        ),
      },
      {
        title: 'scope',
        dataIndex: 'scope',
        width: 90,
        render: (v: string) => <Tag color={SCOPE_COLOR[v]}>{v}</Tag>,
      },
      {
        title: 'source',
        dataIndex: 'source',
        width: 130,
        render: (v: string) => <Tag style={{ fontSize: 11 }}>{v}</Tag>,
      },
      {
        title: 'role / type',
        width: 170,
        render: (_: unknown, r: SlotProposal) => (
          <Space direction="vertical" size={0}>
            <Text style={{ fontSize: 12 }}>{r.role}</Text>
            <Text type="secondary" style={{ fontSize: 11 }}>{r.logical_type}</Text>
          </Space>
        ),
      },
      {
        title: '支持 / VT',
        width: 120,
        sorter: (a: SlotProposal, b: SlotProposal) => a.support_count - b.support_count,
        defaultSortOrder: 'descend' as const,
        render: (_: unknown, r: SlotProposal) => (
          <Space size={4}>
            <Tag color={r.support_count >= 10 ? 'red' : r.support_count >= 5 ? 'gold' : 'default'}>
              {r.support_count} 字段
            </Tag>
            <Tag>{r.target_vt_ids.length} VT</Tag>
          </Space>
        ),
      },
      {
        title: '置信度',
        dataIndex: 'llm_naming_confidence',
        width: 90,
        render: (v: number | null, r: SlotProposal) => {
          if (v === null || v === undefined) return <Text type="secondary">-</Text>
          const color = v >= 0.75 ? 'green' : v >= 0.5 ? 'gold' : 'red'
          return (
            <Space direction="vertical" size={0}>
              <Tag color={color}>{v.toFixed(2)}</Tag>
              {r.cluster_cohesion !== null && (
                <Text type="secondary" style={{ fontSize: 10 }}>
                  coh={r.cluster_cohesion?.toFixed(2)}
                </Text>
              )}
            </Space>
          )
        },
      },
      {
        title: '近似已有槽位',
        width: 170,
        render: (_: unknown, r: SlotProposal) =>
          r.similar_existing_slots?.length ? (
            <Space direction="vertical" size={0}>
              {r.similar_existing_slots.slice(0, 2).map((s) => (
                <Text key={s.name} style={{ fontSize: 11 }}>
                  <Text code style={{ fontSize: 11 }}>{s.name}</Text>
                  {s.similarity && (
                    <Text type="secondary" style={{ marginLeft: 4 }}>
                      ({s.similarity.toFixed(2)})
                    </Text>
                  )}
                </Text>
              ))}
            </Space>
          ) : (
            <Text type="secondary">-</Text>
          ),
      },
      {
        title: '状态',
        dataIndex: 'status',
        width: 100,
        render: (v: string) => <Tag color={STATUS_COLOR[v]}>{STATUS_LABEL[v] || v}</Tag>,
      },
      {
        title: '操作',
        width: 220,
        render: (_: unknown, r: SlotProposal) => (
          <Space size={4}>
            <Button size="small" onClick={() => { setDetail(r); setDetailOpen(true) }}>
              详情
            </Button>
            {r.status === 'pending' && (
              <>
                <Button size="small" type="primary" onClick={() => handleAccept(r)}>
                  接受
                </Button>
                <Button size="small" onClick={() => { setRenameTarget(r); setRenameOpen(true); renameForm.setFieldsValue({ renamed_to: r.name, renamed_cn_name: r.cn_name }) }}>
                  改名
                </Button>
                <Button size="small" danger onClick={() => handleReject(r)}>
                  拒绝
                </Button>
              </>
            )}
          </Space>
        ),
      },
    ],
    []
  )

  return (
    <div style={{ padding: 24 }}>
      <Title level={3} style={{ marginTop: 0 }}>
        I-05b 新槽位候选（聚类 + LLM 命名 + 归一阶段建议合并）
      </Title>
      <Paragraph type="secondary">
        来源：① 对 needs_review/low_confidence/conflict 字段做 VT 内 HDBSCAN 聚类后 LLM 命名；
        ② 归一阶段 LLM 直接建议的 `llm_propose_new_slot`。
        接受后回写到 <Text code>data/slot_library/base_slots.yaml</Text> 或 <Text code>domain_slots.yaml</Text>，
        然后按提示运行 <Text code>run_pipeline.py --from slot_definitions</Text> 重跑归一。
      </Paragraph>

      {nextActionBanner && (
        <Alert
          type="warning"
          showIcon
          closable
          message="有 proposal 已回写，需要重跑归一才能生效"
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
        <Statistic title="总候选" value={meta.total || 0} />
        <Statistic title="当前筛选" value={data?.total_filtered || 0} />
        {meta.by_scope && (
          <Space size={8}>
            {Object.entries(meta.by_scope).map(([k, v]) => (
              <Tag key={k} color={SCOPE_COLOR[k]} style={{ fontSize: 12 }}>
                {k}: {v}
              </Tag>
            ))}
          </Space>
        )}
      </Space>

      <Card size="small" style={{ marginBottom: 16 }}>
        <Space wrap>
          <Space>
            <Text>scope:</Text>
            <Radio.Group
              value={filterScope || 'all'}
              onChange={(e) => setFilterScope(e.target.value === 'all' ? undefined : e.target.value)}
              size="small"
            >
              <Radio.Button value="all">全部</Radio.Button>
              <Radio.Button value="base">base</Radio.Button>
              <Radio.Button value="domain">domain</Radio.Button>
              <Radio.Button value="vt_local">vt_local</Radio.Button>
            </Radio.Group>
          </Space>

          <Space>
            <Text>状态:</Text>
            <Radio.Group
              value={filterStatus || 'all'}
              onChange={(e) => setFilterStatus(e.target.value === 'all' ? undefined : e.target.value)}
              size="small"
            >
              <Radio.Button value="all">全部</Radio.Button>
              <Radio.Button value="pending">待审核</Radio.Button>
              <Radio.Button value="accepted">已接受</Radio.Button>
              <Radio.Button value="rejected">已拒绝</Radio.Button>
              <Radio.Button value="renamed">已改名</Radio.Button>
            </Radio.Group>
          </Space>

          <Space>
            <Text>source:</Text>
            <Select
              size="small"
              style={{ width: 160 }}
              allowClear
              value={filterSource}
              onChange={setFilterSource}
              placeholder="全部"
              options={[
                { value: 'cluster', label: 'cluster' },
                { value: 'llm_suggestion', label: 'llm_suggestion' },
                { value: 'merged', label: 'merged' },
              ]}
            />
          </Space>
        </Space>
      </Card>

      <Card size="small">
        <Spin spinning={loading}>
          <Space style={{ marginBottom: 8 }}>
            <Popconfirm
              title={`批量接受 ${selectedKeys.length} 条？`}
              description="将写入 base_slots.yaml / domain_slots.yaml / slot_definitions.yaml"
              disabled={selectedKeys.length === 0}
              onConfirm={() => handleBatchApply('accept')}
            >
              <Button
                type="primary"
                disabled={selectedKeys.length === 0}
                loading={batchSubmitting}
              >
                批量接受（{selectedKeys.length}）
              </Button>
            </Popconfirm>
            <Popconfirm
              title={`批量拒绝 ${selectedKeys.length} 条？`}
              disabled={selectedKeys.length === 0}
              onConfirm={() => handleBatchApply('reject')}
            >
              <Button danger disabled={selectedKeys.length === 0} loading={batchSubmitting}>
                批量拒绝（{selectedKeys.length}）
              </Button>
            </Popconfirm>
            {selectedKeys.length > 0 && (
              <Button type="text" onClick={() => setSelectedKeys([])}>
                清空选择
              </Button>
            )}
          </Space>
          <Table
            rowKey="proposal_id"
            size="small"
            dataSource={proposals}
            columns={columns as any}
            pagination={{ pageSize: 30, showSizeChanger: true }}
            rowSelection={{
              selectedRowKeys: selectedKeys,
              onChange: setSelectedKeys,
              getCheckboxProps: (r: any) => ({ disabled: r.status !== 'pending' }),
            }}
          />
        </Spin>
      </Card>

      <Drawer
        open={detailOpen}
        title={detail ? `${detail.name} (${detail.cn_name})` : ''}
        width={760}
        onClose={() => setDetailOpen(false)}
      >
        {detail && (
          <Space direction="vertical" style={{ width: '100%' }} size={16}>
            <Descriptions column={2} size="small" bordered>
              <Descriptions.Item label="proposal_id">{detail.proposal_id}</Descriptions.Item>
              <Descriptions.Item label="scope">
                <Tag color={SCOPE_COLOR[detail.scope]}>{detail.scope}</Tag>
              </Descriptions.Item>
              <Descriptions.Item label="source">{detail.source}</Descriptions.Item>
              <Descriptions.Item label="logical_type">{detail.logical_type}</Descriptions.Item>
              <Descriptions.Item label="role">{detail.role}</Descriptions.Item>
              <Descriptions.Item label="support">
                {detail.support_count} 字段 / {detail.target_vt_ids.length} VT
              </Descriptions.Item>
              <Descriptions.Item label="LLM conf">
                {detail.llm_naming_confidence?.toFixed(2) ?? '-'}
              </Descriptions.Item>
              <Descriptions.Item label="聚类内聚度">
                {detail.cluster_cohesion?.toFixed(2) ?? '-'}
              </Descriptions.Item>
              <Descriptions.Item label="description" span={2}>
                {detail.description || '-'}
              </Descriptions.Item>
              <Descriptions.Item label="aliases" span={2}>
                <Space size={[0, 4]} wrap>
                  {detail.aliases.map((a) => (
                    <Tag key={a}>{a}</Tag>
                  ))}
                  {detail.aliases.length === 0 && <Text type="secondary">(无)</Text>}
                </Space>
              </Descriptions.Item>
            </Descriptions>

            <div>
              <Title level={5}>覆盖的 VT（{detail.target_vt_ids.length}）</Title>
              <Space size={[0, 4]} wrap>
                {detail.target_vt_ids.map((v) => (
                  <Tag key={v} color="blue" style={{ fontSize: 11 }}>{v}</Tag>
                ))}
              </Space>
            </div>

            {detail.similar_existing_slots?.length > 0 && (
              <div>
                <Title level={5}>近似的已有槽位（查重提醒）</Title>
                <Table
                  rowKey={(r) => r.name}
                  size="small"
                  pagination={false}
                  dataSource={detail.similar_existing_slots}
                  columns={[
                    { title: 'name', dataIndex: 'name', render: (v: string) => <Text code>{v}</Text> },
                    { title: '相似度', dataIndex: 'similarity', width: 90, render: (v) => v?.toFixed(3) ?? '-' },
                    { title: 'reason', dataIndex: 'reason' },
                  ]}
                />
              </div>
            )}

            <div>
              <Title level={5}>成员字段（最多 {detail.member_fields.length}）</Title>
              <Table
                rowKey={(r) => `${r.table_en}|${r.field_name}|${r.vt_id}`}
                size="small"
                pagination={false}
                dataSource={detail.member_fields}
                columns={[
                  { title: 'field', dataIndex: 'field_name', width: 160, render: (v) => <Text code>{v}</Text> },
                  { title: '注释', dataIndex: 'field_comment', width: 180 },
                  { title: 'vt', dataIndex: 'vt_id', width: 160, render: (v) => <Tag color="blue" style={{ fontSize: 11 }}>{v}</Tag> },
                  { title: 'top1_slot', dataIndex: 'top1_slot', width: 140, render: (v) => v ? <Text code style={{ fontSize: 11 }}>{v}</Text> : '-' },
                  { title: 'top1_score', dataIndex: 'top1_score', width: 90, render: (v) => v?.toFixed(3) ?? '-' },
                ]}
              />
            </div>
          </Space>
        )}
      </Drawer>

      <Modal
        title={renameTarget ? `改名接受: ${renameTarget.name}` : '改名接受'}
        open={renameOpen}
        onOk={handleRenameSubmit}
        onCancel={() => { setRenameOpen(false); setRenameTarget(null); renameForm.resetFields() }}
        destroyOnClose
      >
        <Form form={renameForm} layout="vertical">
          <Form.Item
            name="renamed_to"
            label="新的槽位 name (snake_case)"
            rules={[{ required: true, pattern: /^[a-z][a-z0-9_]*$/, message: 'snake_case 小写 + 数字 + 下划线' }]}
          >
            <Input placeholder="example_slot_name" />
          </Form.Item>
          <Form.Item name="renamed_cn_name" label="中文名">
            <Input />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  )
}
