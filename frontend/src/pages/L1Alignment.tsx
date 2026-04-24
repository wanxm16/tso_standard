import { useEffect, useState } from 'react'
import { Alert, Badge, Button, Card, Collapse, Form, Input, InputNumber, Modal, Space, Table, Tag, Typography, message } from 'antd'
import { api } from '../api'

const { Title, Text } = Typography
const { Panel } = Collapse

export default function L1Alignment() {
  const [data, setData] = useState<any>(null)
  const [regenOpen, setRegenOpen] = useState(false)
  const [regenBusy, setRegenBusy] = useState(false)
  const [applying, setApplying] = useState<string | null>(null)
  const [regenForm] = Form.useForm()

  const load = async () => {
    const d = await api.getL1Alignment()
    setData(d)
  }
  useEffect(() => { load() }, [])

  const handleRegen = async () => {
    const v = await regenForm.validateFields()
    setRegenBusy(true)
    try {
      await api.regenerateL1Alignment(v.only_l1 || undefined, v.threshold)
      message.success('L1 对齐已重跑')
      setRegenOpen(false)
      await load()
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    } finally {
      setRegenBusy(false)
    }
  }

  const handleApply = async (l1: string, cluster: any) => {
    const key = `${l1}#${cluster.cluster_id}`
    setApplying(key)
    try {
      const r = await api.applyL1Cluster({
        l1, cluster_id: cluster.cluster_id,
        reason: `L1 alignment: ${cluster.canonical_name}`,
      })
      if (r.skipped) message.info('已跳过：无需 rename')
      else message.success(`✅ 已应用 v${r.version}（${r.affected_slots} slot / ${r.affected_norm_rows} row）`)
      await load()
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    } finally {
      setApplying(null)
    }
  }

  if (!data || !data.exists) {
    return (
      <div style={{ padding: 24 }}>
        <Alert type="warning" message="L1 对齐提议尚未生成（W5-B）" showIcon />
        <Button type="primary" onClick={() => setRegenOpen(true)} style={{ marginTop: 16 }}>跑对齐</Button>
        <RegenModal open={regenOpen} onCancel={() => setRegenOpen(false)} onOk={handleRegen} busy={regenBusy} form={regenForm} />
      </div>
    )
  }

  const s = data.summary

  return (
    <div style={{ padding: 24 }}>
      <Space style={{ width: '100%', justifyContent: 'space-between', marginBottom: 16 }}>
        <Title level={4} style={{ margin: 0 }}>L1 槽位对齐（W5-B）</Title>
        <Space>
          <Tag>L1: {s.l1_count}</Tag>
          <Tag>clusters: {s.total_clusters}</Tag>
          <Tag color="red">可对齐: {s.alignable_clusters}</Tag>
          <Tag color="orange">需 rename VT: {s.total_vt_would_rename}</Tag>
          <Tag>threshold: {data.threshold}</Tag>
          <Button onClick={() => setRegenOpen(true)}>重跑（支持参数）</Button>
        </Space>
      </Space>

      <Alert
        type="info"
        showIcon
        message={'建议 W5-A 完成后再跑 W5-B：L1 跨 L2 对齐已经默认 L2 内部槽位已统一。'}
        description={'只对「size≥2 且跨 L2」的 cluster 生成 rename_plan（同 L2 内跨 VT 的遗漏由 W5-A 处理）。接受后执行 cascade rename，scope=l1，同步改 slot_definitions / field_normalization / alignment_log。'}
        style={{ marginBottom: 16 }}
      />

      <Collapse defaultActiveKey={data.proposals.map((p: any) => p.l1)}>
        {data.proposals.map((p: any) => (
          <Panel
            key={p.l1}
            header={
              <Space>
                <Text strong>{p.l1}</Text>
                <Tag>{p.l2_count} L2</Tag>
                <Tag>{p.vt_count} VT</Tag>
                <Tag>{p.candidate_count} ext slot</Tag>
                <Badge count={p.clusters.filter((c: any) => c.canonical_name).length} color="red" showZero title="可对齐 cluster 数" />
              </Space>
            }
          >
            {p.clusters.filter((c: any) => c.canonical_name).map((c: any) => {
              const key = `${p.l1}#${c.cluster_id}`
              const changed = (c.rename_plan || []).filter((r: any) => r.changed && !r.excluded_as_outlier).length
              return (
                <Card
                  key={key}
                  size="small"
                  style={{ marginBottom: 12 }}
                  title={
                    <Space>
                      <Text code strong>{c.canonical_name}</Text>
                      <Text>({c.canonical_cn_name})</Text>
                      <Tag color="purple">size {c.size}</Tag>
                      <Tag color="geekblue">跨 {c.l2_coverage?.length || 0} L2</Tag>
                      {c.confidence && <Text type="secondary" style={{ fontSize: 12 }}>conf {c.confidence.toFixed(2)}</Text>}
                      <Badge count={changed} color="red" showZero title="需 rename VT 数" />
                    </Space>
                  }
                  extra={
                    <Button
                      type="primary"
                      size="small"
                      loading={applying === key}
                      disabled={changed === 0}
                      onClick={() => handleApply(p.l1, c)}
                    >
                      接受并应用
                    </Button>
                  }
                >
                  <div style={{ marginBottom: 6 }}>
                    <Text type="secondary">{c.canonical_description}</Text>
                  </div>
                  {c.l2_coverage?.length > 0 && (
                    <div style={{ marginBottom: 6 }}>
                      <Text type="secondary" style={{ fontSize: 12 }}>覆盖 L2: </Text>
                      {c.l2_coverage.map((x: string, i: number) => <Tag key={i} color="geekblue">{x}</Tag>)}
                    </div>
                  )}
                  {c.canonical_synonyms?.length > 0 && (
                    <div style={{ marginBottom: 6 }}>
                      <Text type="secondary" style={{ fontSize: 12 }}>统一 synonyms: </Text>
                      {c.canonical_synonyms.map((x: string, i: number) => <Tag key={i}>{x}</Tag>)}
                    </div>
                  )}
                  {c.outliers?.length > 0 && (
                    <div style={{ marginBottom: 6 }}>
                      <Text type="secondary" style={{ fontSize: 12, color: '#fa8c16' }}>LLM 标为 outlier（将被排除）: </Text>
                      {c.outliers.map((x: string, i: number) => <Tag key={i} color="orange">{x}</Tag>)}
                    </div>
                  )}
                  <Table
                    size="small"
                    rowKey={(r: any, i?: number) => `${key}-${r.vt_id}-${i}`}
                    pagination={false}
                    dataSource={c.rename_plan}
                    columns={[
                      { title: 'vt_id', dataIndex: 'vt_id', width: 180 },
                      { title: 'L2', dataIndex: 'l2', width: 120 },
                      { title: 'before', dataIndex: 'before_name', render: (v: string) => <Text code>{v}</Text> },
                      { title: 'after', dataIndex: 'after_name', render: (v: string, r: any) => r.changed ? <Text code style={{ color: '#fa541c' }}>{v}</Text> : <Text code type="secondary">{v}</Text> },
                      {
                        title: '状态',
                        width: 110,
                        render: (_: any, r: any) => r.excluded_as_outlier ? <Tag color="orange">outlier</Tag> : r.changed ? <Tag color="red">改</Tag> : <Tag>=</Tag>,
                      },
                    ]}
                  />
                </Card>
              )
            })}
            {p.clusters.filter((c: any) => c.canonical_name).length === 0 && (
              <Text type="secondary">无跨 L2 cluster 可对齐</Text>
            )}
          </Panel>
        ))}
      </Collapse>

      <RegenModal open={regenOpen} onCancel={() => setRegenOpen(false)} onOk={handleRegen} busy={regenBusy} form={regenForm} />
    </div>
  )
}

function RegenModal({ open, onCancel, onOk, busy, form }: any) {
  return (
    <Modal title="重跑 L1 对齐" open={open} onCancel={onCancel} onOk={onOk} okButtonProps={{ loading: busy }} destroyOnHidden>
      <Form form={form} layout="vertical" initialValues={{ threshold: 0.18 }}>
        <Form.Item name="only_l1" label="only_l1（可选）" extra="只跑某个 L1，空=全量 ≥2 L2 的 L1">
          <Input placeholder="例: 主体主档" />
        </Form.Item>
        <Form.Item name="threshold" label="distance_threshold" extra="cosine 距离阈值，0.18 约等于相似度 > 0.82 合并">
          <InputNumber min={0.05} max={0.5} step={0.02} style={{ width: 160 }} />
        </Form.Item>
      </Form>
    </Modal>
  )
}
