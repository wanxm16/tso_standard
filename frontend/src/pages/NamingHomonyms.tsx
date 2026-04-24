import { useEffect, useState } from 'react'
import { Alert, Badge, Button, Card, Collapse, Space, Table, Tag, Typography, message } from 'antd'
import { api } from '../api'

const { Title, Text } = Typography
const { Panel } = Collapse

const JUDGEMENT_TAG: Record<string, { color: string; label: string }> = {
  synonym: { color: 'green', label: '同义' },
  homonym: { color: 'red', label: '异义' },
  mixed: { color: 'orange', label: '混合' },
  unknown: { color: 'default', label: '未知' },
}

export default function NamingHomonyms() {
  const [data, setData] = useState<any>(null)
  const [loading, setLoading] = useState(false)
  const [applying, setApplying] = useState<string | null>(null)

  const load = async () => {
    const d = await api.getHomonymProposals()
    setData(d)
  }
  useEffect(() => { load() }, [])

  const handleApply = async (name: string, reason: string) => {
    setApplying(name)
    try {
      const r = await api.applyHomonym(name, reason)
      if (r.skipped) {
        message.info('跳过：该提议无需 rename')
      } else {
        message.success(`✅ 已应用（v${r.version}）· 改了 ${r.affected_slots} 个槽位 / ${r.affected_norm_rows} 行 normalization`)
      }
      await load()
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    } finally {
      setApplying(null)
    }
  }

  const regenerate = async () => {
    setLoading(true)
    try {
      await api.regenerateHomonyms()
      message.success('LLM 判断重跑完成')
      await load()
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    } finally {
      setLoading(false)
    }
  }

  if (!data || !data.exists) {
    return (
      <div style={{ padding: 24 }}>
        <Alert type="warning" message="同名异义提议未生成" description="先跑诊断，再跑 LLM 判断（阻塞约 1 分钟）" showIcon />
        <Button type="primary" onClick={regenerate} loading={loading} style={{ marginTop: 16 }}>
          跑 LLM 判断
        </Button>
      </div>
    )
  }

  const s = data.summary

  return (
    <div style={{ padding: 24 }}>
      <Space style={{ width: '100%', justifyContent: 'space-between', marginBottom: 16 }}>
        <Title level={4} style={{ margin: 0 }}>同名异义消歧 · 审核</Title>
        <Space>
          <Tag>候选: {s.total_candidates}</Tag>
          <Tag color="green">同义: {s.judged_synonym}</Tag>
          <Tag color="red">异义: {s.judged_homonym}</Tag>
          <Tag color="orange">混合: {s.judged_mixed}</Tag>
          <Tag>需 rename VT: {s.total_vt_needs_rename}/{s.total_vt_affected}</Tag>
          <Button onClick={regenerate} loading={loading}>重跑 LLM</Button>
        </Space>
      </Space>

      <Alert
        type="info"
        showIcon
        message="scope 乙 硬标准：base_slots 全局唯一 + 同 L2 内 extended 不重名"
        description={'LLM 已判「同义/异义/混合」。点「接受并应用」执行 cascade rename：同步更新 slot_definitions + field_normalization.parquet（若存在 reviewed.parquet 也会同步）+ 写 alignment_log + 快照。'}
        style={{ marginBottom: 16 }}
      />

      <Collapse>
        {data.proposals.map((p: any) => {
          const tag = JUDGEMENT_TAG[p.judgement] || JUDGEMENT_TAG.unknown
          const needsRenameCount = (p.member_proposals || []).filter((m: any) => m.changed).length
          return (
            <Panel
              key={p.name}
              header={
                <Space>
                  <Text code strong>{p.name}</Text>
                  <Tag color={tag.color}>{tag.label}</Tag>
                  <Text type="secondary" style={{ fontSize: 12 }}>conf {p.confidence?.toFixed?.(2)}</Text>
                  <Badge count={needsRenameCount} color={needsRenameCount > 0 ? 'red' : 'green'} showZero title="需 rename VT 数" />
                </Space>
              }
              extra={
                <Button
                  type="primary"
                  size="small"
                  loading={applying === p.name}
                  disabled={needsRenameCount === 0}
                  onClick={(e) => { e.stopPropagation(); handleApply(p.name, p.reason || '') }}
                >
                  接受并应用
                </Button>
              }
            >
              <div style={{ marginBottom: 8 }}>
                <Text type="secondary">LLM 理由：</Text>
                <Text>{p.reason}</Text>
              </div>
              <Card size="small" title={`分组（${p.groups?.length || 0}）`} style={{ marginBottom: 8 }}>
                {(p.groups || []).map((g: any, i: number) => (
                  <div key={i} style={{ marginBottom: 6 }}>
                    <Space>
                      <Text code>{g.suggested_name}</Text>
                      <Text type="secondary">←</Text>
                      {(g.variants || []).map((v: string, vi: number) => <Tag key={vi}>{v}</Tag>)}
                      {g.canonical_cn && <Text type="secondary" style={{ fontSize: 12 }}>（统一中文名: {g.canonical_cn}）</Text>}
                    </Space>
                  </div>
                ))}
              </Card>
              <Table
                size="small"
                rowKey={(r: any, i?: number) => `${p.name}-${r.vt_id}-${i}`}
                pagination={false}
                dataSource={p.member_proposals}
                columns={[
                  { title: 'vt_id', dataIndex: 'vt_id', width: 190 },
                  { title: 'L2', dataIndex: 'l2', width: 120 },
                  { title: 'cn_name', dataIndex: 'cn_name', width: 140 },
                  { title: 'before', dataIndex: 'before_name', render: (v: string) => <Text code>{v}</Text> },
                  { title: 'after', dataIndex: 'after_name', render: (v: string, r: any) => r.changed ? <Text code style={{ color: '#fa541c' }}>{v}</Text> : <Text code type="secondary">{v}</Text> },
                  { title: '变', dataIndex: 'changed', width: 50, render: (v: boolean) => v ? <Tag color="red">改</Tag> : <Tag>=</Tag> },
                ]}
              />
            </Panel>
          )
        })}
      </Collapse>
    </div>
  )
}
