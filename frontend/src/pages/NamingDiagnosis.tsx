import { useEffect, useState } from 'react'
import { Alert, Button, Card, Col, Row, Space, Statistic, Table, Tag, Typography, message } from 'antd'
import { api } from '../api'

const { Title, Text } = Typography

export default function NamingDiagnosis() {
  const [data, setData] = useState<any>(null)
  const [loading, setLoading] = useState(false)

  const load = async () => {
    const d = await api.getNamingDiagnosis()
    setData(d)
  }
  useEffect(() => { load() }, [])

  const regenerate = async () => {
    setLoading(true)
    try {
      await api.regenerateNamingDiagnosis()
      message.success('诊断重跑完成')
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
        <Alert type="warning" message="诊断报告尚未生成" description="点击下方按钮跑一次诊断脚本。" showIcon />
        <Button type="primary" onClick={regenerate} loading={loading} style={{ marginTop: 16 }}>
          跑诊断
        </Button>
      </div>
    )
  }

  const s = data.summary

  return (
    <div style={{ padding: 24 }}>
      <Space style={{ width: '100%', justifyContent: 'space-between', marginBottom: 16 }}>
        <Title level={4} style={{ margin: 0 }}>槽位命名诊断报告</Title>
        <Space>
          <Text type="secondary" style={{ fontSize: 12 }}>生成于 {data.generated_at}</Text>
          <Button onClick={regenerate} loading={loading}>重跑诊断</Button>
        </Space>
      </Space>

      <Row gutter={12} style={{ marginBottom: 16 }}>
        <Col span={4}><Card><Statistic title="VT 总数" value={s.vt_count} /></Card></Col>
        <Col span={4}><Card><Statistic title="base 槽位" value={s.base_slot_count} /></Card></Col>
        <Col span={4}><Card><Statistic title="extended 去重" value={s.extended_unique_names} /></Card></Col>
        <Col span={4}><Card><Statistic title="跨 VT 重名" value={s.extended_repeat_name_count} valueStyle={{ color: '#faad14' }} /></Card></Col>
        <Col span={4}><Card><Statistic title="L2 内冲突" value={s.l2_conflict_count} valueStyle={{ color: '#f5222d' }} /></Card></Col>
        <Col span={4}><Card><Statistic title="同名异义候选" value={s.homonym_candidate_count} valueStyle={{ color: '#722ed1' }} /></Card></Col>
      </Row>

      <Card title="已用字段数分布（W5-F 阈值校准用）" size="small" style={{ marginBottom: 16 }}>
        <Space>
          {data.used_field_histogram.map((h: any) => (
            <Tag key={h.range} color="blue">{h.range}: {h.count} 张 VT</Tag>
          ))}
        </Space>
        <div style={{ marginTop: 8 }}>
          <Text type="secondary" style={{ fontSize: 12 }}>
            W5-F 触发条件："已用字段数 ≤ 10"为主，"10~20 且源表数 ≤ 2"为辅 — 共 {s.w5f_trigger_count} 张 VT 符合。
          </Text>
        </div>
      </Card>

      <Card title={`L2 内 extended 重名冲突（${s.l2_conflict_count}）`} size="small" style={{ marginBottom: 16 }}>
        <Table
          size="small"
          rowKey={(r: any) => `${r.l1}_${r.l2}_${r.name}`}
          pagination={{ pageSize: 10 }}
          dataSource={data.l2_conflicts}
          columns={[
            { title: 'L1', dataIndex: 'l1', width: 140 },
            { title: 'L2', dataIndex: 'l2', width: 160 },
            { title: 'slot_name', dataIndex: 'name', width: 220, render: (v: string) => <Text code>{v}</Text> },
            { title: 'VT 数', dataIndex: 'vt_count', width: 80 },
            {
              title: 'cn_name variants',
              dataIndex: 'cn_name_variants',
              render: (v: string[]) => v.map((x, i) => <Tag key={i}>{x}</Tag>),
            },
          ]}
        />
      </Card>

      <Card title={`base-extended 冲突（${s.base_extended_conflict_count}）`} size="small" style={{ marginBottom: 16 }}>
        <Text type="secondary" style={{ fontSize: 12 }}>
          这些 slot_name 同时存在于 base_slots 和某 VT 的 extended 定义中 — 语义有歧义风险。
        </Text>
        <Table
          size="small"
          rowKey="name"
          pagination={{ pageSize: 10 }}
          dataSource={data.base_extended_conflicts}
          columns={[
            { title: 'slot_name', dataIndex: 'name', width: 220, render: (v: string) => <Text code>{v}</Text> },
            { title: '被 extended 引用的 VT 数', dataIndex: 'vt_count', width: 160 },
            {
              title: 'VT 列表',
              dataIndex: 'members',
              render: (ms: any[]) => ms.slice(0, 3).map((m, i) => <Tag key={i}>{m.vt_id} · {m.cn_name}</Tag>),
            },
          ]}
        />
      </Card>

      <Card title={`W5-F 触发 VT（${s.w5f_trigger_count}，字段稀缺待升级）`} size="small">
        <Table
          size="small"
          rowKey="vt_id"
          pagination={{ pageSize: 10 }}
          dataSource={data.w5f_candidates}
          columns={[
            { title: 'vt_id', dataIndex: 'vt_id', width: 200 },
            { title: 'topic', dataIndex: 'topic' },
            { title: '源表数', dataIndex: 'source_table_count', width: 90 },
            { title: '已用字段数', dataIndex: 'used_field_count', width: 100 },
            { title: '触发', dataIndex: 'trigger', width: 90, render: (v: string) => v === 'small' ? <Tag color="red">main</Tag> : <Tag color="orange">aux</Tag> },
          ]}
        />
      </Card>
    </div>
  )
}
