import { useEffect, useState } from 'react'
import { Alert, Badge, Button, Card, Collapse, Form, Input, InputNumber, Modal, Space, Table, Tag, Typography, message } from 'antd'
import { api } from '../api'

const { Title, Text } = Typography
const { Panel } = Collapse

export default function L2Alignment() {
  const [data, setData] = useState<any>(null)
  const [regenOpen, setRegenOpen] = useState(false)
  const [regenBusy, setRegenBusy] = useState(false)
  const [applying, setApplying] = useState<string | null>(null)
  const [regenForm] = Form.useForm()

  const load = async () => {
    const d = await api.getL2Alignment()
    setData(d)
  }
  useEffect(() => { load() }, [])

  const handleRegen = async () => {
    const v = await regenForm.validateFields()
    setRegenBusy(true)
    try {
      await api.regenerateL2Alignment(v.only_l2 || undefined, v.threshold)
      message.success('L2 对齐已重跑')
      setRegenOpen(false)
      await load()
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    } finally {
      setRegenBusy(false)
    }
  }

  const handleApply = async (l1: string, l2: string, cluster: any) => {
    const key = `${l1}/${l2}#${cluster.cluster_id}`
    setApplying(key)
    try {
      const r = await api.applyL2Cluster({
        l1, l2, cluster_id: cluster.cluster_id,
        reason: `L2 alignment: ${cluster.canonical_name}`,
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
        <Alert type="warning" message="L2 对齐提议尚未生成" showIcon />
        <Button type="primary" onClick={() => setRegenOpen(true)} style={{ marginTop: 16 }}>跑对齐</Button>
        <RegenModal open={regenOpen} onCancel={() => setRegenOpen(false)} onOk={handleRegen} busy={regenBusy} form={regenForm} />
      </div>
    )
  }

  const s = data.summary

  return (
    <div style={{ padding: 24 }}>
      <Space style={{ width: '100%', justifyContent: 'space-between', marginBottom: 16 }}>
        <Title level={4} style={{ margin: 0 }}>L2 槽位对齐（W5-A）</Title>
        <Space>
          <Tag>L2: {s.l2_count}</Tag>
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
        message="POC 模式：建议先挑召回最差的 L2（见归因页）跑一次，阈值 0.18 → 余弦相似度 > 0.82 的槽位合并。"
        description={'LLM 给的 canonical_name 只是「建议」，你可以选择接受、拒绝或重跑改阈值。应用时对 cross-VT size≥2 的 cluster 做 cascade rename，同步改 slot_definitions / field_normalization / alignment_log。'}
        style={{ marginBottom: 16 }}
      />

      <Collapse defaultActiveKey={data.proposals.map((p: any) => `${p.l1}/${p.l2}`)}>
        {data.proposals.map((p: any) => (
          <Panel
            key={`${p.l1}/${p.l2}`}
            header={
              <Space>
                <Text strong>{p.l1} / {p.l2}</Text>
                <Tag>{p.vt_count} VT</Tag>
                <Tag>{p.candidate_count} ext slot</Tag>
                <Badge count={p.clusters.filter((c: any) => c.canonical_name).length} color="red" showZero title="可对齐 cluster 数" />
              </Space>
            }
          >
            {p.clusters.filter((c: any) => c.canonical_name).map((c: any) => {
              const key = `${p.l1}/${p.l2}#${c.cluster_id}`
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
                      onClick={() => handleApply(p.l1, p.l2, c)}
                    >
                      接受并应用
                    </Button>
                  }
                >
                  <div style={{ marginBottom: 6 }}>
                    <Text type="secondary">{c.canonical_description}</Text>
                  </div>
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
              <Text type="secondary">无跨 VT cluster 可对齐</Text>
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
    <Modal title="重跑 L2 对齐" open={open} onCancel={onCancel} onOk={onOk} okButtonProps={{ loading: busy }} destroyOnHidden>
      <Form form={form} layout="vertical" initialValues={{ threshold: 0.18 }}>
        <Form.Item name="only_l2" label="only_l2（可选）" extra="只跑某个 L2（输 L2 名字 或 'L1/L2' 全路径；空=跑全量 ≥2 VT 的 L2）">
          <Input placeholder="例: 人员主档 或 主体主档/人员主档" />
        </Form.Item>
        <Form.Item name="threshold" label="distance_threshold" extra="cosine 距离阈值，0.18 约等于相似度 > 0.82 合并。调低会更严（合并更少），调高会更松">
          <InputNumber min={0.05} max={0.5} step={0.02} style={{ width: 160 }} />
        </Form.Item>
      </Form>
    </Modal>
  )
}
