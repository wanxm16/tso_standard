import { useEffect, useState } from 'react'
import { Alert, Badge, Button, Card, Descriptions, Form, Input, InputNumber, Modal, Space, Switch, Table, Tag, Typography, message } from 'antd'
import { api } from '../api'

const { Title, Text, Paragraph } = Typography

export default function BasePromotion() {
  const [data, setData] = useState<any>(null)
  const [regenOpen, setRegenOpen] = useState(false)
  const [regenBusy, setRegenBusy] = useState(false)
  const [applying, setApplying] = useState<string | null>(null)
  const [appliedNames, setAppliedNames] = useState<Set<string>>(new Set())
  const [baseNames, setBaseNames] = useState<Set<string>>(new Set())
  const [regenForm] = Form.useForm()

  const load = async () => {
    const d = await api.getBasePromotion()
    setData(d)
    // 拉 slot-library 看哪些 canonical_name 已经在 base_slots 里了
    try {
      const lib = await api.slotLibrary()
      setBaseNames(new Set(lib.base_slots.map((s: any) => s.name)))
    } catch {}
  }
  useEffect(() => { load() }, [])

  const handleRegen = async () => {
    const v = await regenForm.validateFields()
    setRegenBusy(true)
    try {
      await api.regenerateBasePromotion({
        min_l1: v.min_l1,
        only_name: v.only_name || undefined,
        no_llm: v.no_llm,
      })
      message.success('base 提升候选已重跑')
      setRegenOpen(false)
      await load()
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    } finally {
      setRegenBusy(false)
    }
  }

  const handleApply = async (p: any) => {
    const key = p.canonical_name
    setApplying(key)
    try {
      const r = await api.applyBasePromotion({
        canonical_name: p.canonical_name,
        reason: `promote ${p.canonical_name} to base_slots`,
      })
      message.success(
        `✅ v${r.version}：base +${r.base_slots_added}，slot ${r.affected_slots}，row ${r.affected_norm_rows}`,
        5,
      )
      // 本地标记已应用：即使 yaml 快照没更新，卡片也立刻变灰
      setAppliedNames((prev) => {
        const next = new Set(prev)
        next.add(p.canonical_name)
        return next
      })
      setBaseNames((prev) => {
        const next = new Set(prev)
        next.add(p.canonical_name)
        return next
      })
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    } finally {
      setApplying(null)
    }
  }

  if (!data || !data.exists) {
    return (
      <div style={{ padding: 24 }}>
        <Alert type="warning" message="base 提升候选尚未生成（W5-C）" showIcon />
        <Button type="primary" onClick={() => setRegenOpen(true)} style={{ marginTop: 16 }}>跑 base 提升扫描</Button>
        <RegenModal open={regenOpen} onCancel={() => setRegenOpen(false)} onOk={handleRegen} busy={regenBusy} form={regenForm} />
      </div>
    )
  }

  const s = data.summary

  return (
    <div style={{ padding: 24 }}>
      <Space style={{ width: '100%', justifyContent: 'space-between', marginBottom: 16 }}>
        <Title level={4} style={{ margin: 0 }}>base 槽位提升（W5-C）</Title>
        <Space>
          <Tag>候选: {s.candidate_count}</Tag>
          <Tag>涉及 VT 次数: {s.total_vts}</Tag>
          <Tag>min_l1: {data.min_l1}</Tag>
          <Button onClick={() => setRegenOpen(true)}>重跑扫描</Button>
        </Space>
      </Space>

      <Alert
        type="info"
        showIcon
        message={'将跨 L1 复现（且不在 base_slots 里的）extended 槽位批量提升为 base。'}
        description={'接受后：base_slots.yaml 追加条目（保留注释，ruamel）；各 VT 对应 slot 的 from 由 extended 改为 base；mapped_fields / role 保留；cn_name/description/aliases/logical_type 统一由 base_slots 承担。revert 走 snapshot 文件级恢复。'}
        style={{ marginBottom: 16 }}
      />

      {data.proposals.length === 0 && (
        <Alert type="warning" showIcon message="暂无候选" description="尝试降低 min_l1（比如 1），或先跑 W5-A/W5-B 把各 VT 的 slot_name 对齐一致。" />
      )}

      {data.proposals.map((p: any) => {
        const isApplied = appliedNames.has(p.canonical_name) || baseNames.has(p.canonical_name)
        return (
        <Card
          key={p.canonical_name}
          size="small"
          style={{ marginBottom: 12, opacity: isApplied ? 0.5 : 1 }}
          title={
            <Space>
              <Text code strong>{p.canonical_name}</Text>
              {p.base_entry?.cn_name && <Text>({p.base_entry.cn_name})</Text>}
              <Tag color="purple">{p.l1_count} L1</Tag>
              <Tag color="geekblue">{p.vt_count} VT</Tag>
              {p.confidence != null && <Text type="secondary" style={{ fontSize: 12 }}>conf {Number(p.confidence).toFixed(2)}</Text>}
              {!p.base_entry && <Tag color="orange">未生成 base_entry</Tag>}
              {isApplied && <Tag color="green">已提升</Tag>}
            </Space>
          }
          extra={
            <Button
              type="primary"
              size="small"
              loading={applying === p.canonical_name}
              disabled={!p.base_entry || isApplied}
              onClick={() => handleApply(p)}
            >
              {isApplied ? '已在 base' : '提升为 base'}
            </Button>
          }
        >
          <div style={{ marginBottom: 8 }}>
            <Text type="secondary" style={{ fontSize: 12 }}>覆盖 L1: </Text>
            {p.l1_coverage.map((x: string, i: number) => <Tag key={i} color="blue">{x}</Tag>)}
          </div>

          {p.base_entry && (
            <Descriptions size="small" bordered column={2} style={{ marginBottom: 12 }}>
              <Descriptions.Item label="logical_type">{p.base_entry.logical_type}</Descriptions.Item>
              <Descriptions.Item label="role">{p.base_entry.role}</Descriptions.Item>
              <Descriptions.Item label="description" span={2}>
                <Paragraph style={{ marginBottom: 0 }}>{p.base_entry.description}</Paragraph>
              </Descriptions.Item>
              <Descriptions.Item label="aliases" span={2}>
                {(p.base_entry.aliases || []).map((x: string, i: number) => <Tag key={i}>{x}</Tag>)}
              </Descriptions.Item>
              <Descriptions.Item label="applicable_table_types" span={2}>
                {(p.base_entry.applicable_table_types || []).map((x: string, i: number) => <Tag key={i} color="cyan">{x}</Tag>)}
              </Descriptions.Item>
              {p.rationale && (
                <Descriptions.Item label="rationale" span={2}>
                  <Text type="secondary">{p.rationale}</Text>
                </Descriptions.Item>
              )}
            </Descriptions>
          )}

          <Text type="secondary" style={{ fontSize: 12 }}>受影响 VT（{p.members.length}）：</Text>
          <Table
            size="small"
            rowKey={(r: any, i?: number) => `${p.canonical_name}-${r.vt_id}-${i}`}
            pagination={false}
            dataSource={p.members}
            columns={[
              { title: 'vt_id', dataIndex: 'vt_id', width: 180 },
              { title: 'L1', dataIndex: 'l1', width: 100 },
              { title: 'L2', dataIndex: 'l2', width: 120 },
              { title: 'slot_index', dataIndex: 'slot_index', width: 80 },
              { title: '#mapped_fields', dataIndex: 'mapped_fields_count', width: 110 },
            ]}
          />
        </Card>
        )
      })}

      <RegenModal open={regenOpen} onCancel={() => setRegenOpen(false)} onOk={handleRegen} busy={regenBusy} form={regenForm} />
    </div>
  )
}

function RegenModal({ open, onCancel, onOk, busy, form }: any) {
  return (
    <Modal title="重跑 base 提升扫描" open={open} onCancel={onCancel} onOk={onOk} okButtonProps={{ loading: busy }} destroyOnHidden>
      <Form form={form} layout="vertical" initialValues={{ min_l1: 2, no_llm: false }}>
        <Form.Item name="min_l1" label="min_l1（最少覆盖的 L1 数）" extra="数据不足时可以临时设为 1 做 pipeline 验证">
          <InputNumber min={1} max={5} style={{ width: 120 }} />
        </Form.Item>
        <Form.Item name="only_name" label="only_name（可选）" extra="只跑某个 slot_name（调试用）">
          <Input placeholder="例: certificate_type" />
        </Form.Item>
        <Form.Item name="no_llm" label="跳过 LLM（只列候选）" valuePropName="checked">
          <Switch />
        </Form.Item>
      </Form>
    </Modal>
  )
}
