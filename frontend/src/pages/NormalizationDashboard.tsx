import { useEffect, useState } from 'react'
import { Card, Col, Row, Statistic, Spin, Typography, Progress, Table, Tag, Space } from 'antd'
import { Link } from 'react-router-dom'
import { api } from '../api'
import { STATUS_COLOR, STATUS_LABEL } from '../types'
import type { NormalizationStats } from '../types'

const { Title, Text } = Typography

export default function NormalizationDashboard() {
  const [stats, setStats] = useState<NormalizationStats | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    api.normStats().then(setStats).finally(() => setLoading(false))
  }, [])

  if (loading || !stats) {
    return (
      <div style={{ padding: 40, textAlign: 'center' }}>
        <Spin size="large" />
      </div>
    )
  }

  const total = stats.total_rows
  const statusRows = Object.entries(stats.by_status).map(([k, v]) => ({
    key: k,
    status: k,
    count: v,
    pct: ((v / total) * 100).toFixed(1),
  }))

  return (
    <div style={{ padding: 24 }}>
      <Title level={3} style={{ marginTop: 0 }}>
        字段归一化 Dashboard
      </Title>
      <Text type="secondary">
        来自 I-04 归槽决策。共 {stats.unique_fields} 个字段 × {stats.unique_vts} 张 VT = {total} 行归一结果。
      </Text>

      <Row gutter={16} style={{ marginTop: 24 }}>
        <Col span={6}>
          <Card>
            <Statistic title="总字段 × VT 行数" value={total} />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="自动通过"
              value={stats.by_status.auto_accepted || 0}
              suffix={<Text type="secondary" style={{ fontSize: 14 }}>
                ({(((stats.by_status.auto_accepted || 0) / total) * 100).toFixed(1)}%)
              </Text>}
              valueStyle={{ color: '#52c41a' }}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="LLM 兜底"
              value={stats.llm_applied}
              suffix={<Text type="secondary" style={{ fontSize: 14 }}>
                ({((stats.llm_applied / total) * 100).toFixed(1)}%)
              </Text>}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="已人工审核"
              value={stats.reviewed}
              suffix={<Text type="secondary" style={{ fontSize: 14 }}>
                / {total - (stats.by_status.auto_accepted || 0)}
              </Text>}
              valueStyle={{ color: stats.reviewed > 0 ? '#1677ff' : '#999' }}
            />
          </Card>
        </Col>
      </Row>

      <Row gutter={16} style={{ marginTop: 16 }}>
        <Col span={12}>
          <Card title="review_status 分布" size="small">
            {statusRows.map((r) => (
              <div key={r.key} style={{ marginBottom: 12 }}>
                <Space style={{ width: '100%', justifyContent: 'space-between' }}>
                  <Tag color={STATUS_COLOR[r.status]}>{STATUS_LABEL[r.status] || r.status}</Tag>
                  <Text>
                    {r.count} ({r.pct}%)
                  </Text>
                </Space>
                <Progress
                  percent={parseFloat(r.pct)}
                  showInfo={false}
                  strokeColor={STATUS_COLOR[r.status]}
                  size="small"
                />
              </div>
            ))}
          </Card>
        </Col>

        <Col span={12}>
          <Card title="冲突类型分布" size="small">
            <Table
              size="small"
              pagination={false}
              dataSource={Object.entries(stats.by_conflict_type).map(([k, v], i) => ({
                key: i,
                type: k,
                count: v,
              }))}
              columns={[
                { title: '冲突类型', dataIndex: 'type', render: (v: string) => <Tag color="volcano">{v}</Tag> },
                { title: '字段数', dataIndex: 'count' },
              ]}
            />
          </Card>
        </Col>
      </Row>

      <Card title="快速跳转" size="small" style={{ marginTop: 16 }}>
        <Space size="large">
          <Link to="/normalization/review?status=conflict">
            <Tag color="magenta" style={{ fontSize: 14, padding: '4px 12px' }}>
              审核冲突队列 ({stats.by_status.conflict || 0})
            </Tag>
          </Link>
          <Link to="/normalization/review?status=low_confidence">
            <Tag color="red" style={{ fontSize: 14, padding: '4px 12px' }}>
              低置信队列 ({stats.by_status.low_confidence || 0})
            </Tag>
          </Link>
          <Link to="/normalization/review?status=needs_review">
            <Tag color="gold" style={{ fontSize: 14, padding: '4px 12px' }}>
              需审核队列 ({stats.by_status.needs_review || 0})
            </Tag>
          </Link>
          <Link to="/normalization/new-slots">
            <Tag color="blue" style={{ fontSize: 14, padding: '4px 12px' }}>
              LLM 建议新槽位 ({stats.llm_propose_new_slot})
            </Tag>
          </Link>
        </Space>
      </Card>
    </div>
  )
}
