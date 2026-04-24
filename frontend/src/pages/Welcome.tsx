import { Card, Col, Row, Statistic, Tag, Typography } from 'antd'
import type { Stats } from '../types'

const { Title, Paragraph, Text } = Typography

export default function Welcome({ stats }: { stats: Stats | null }) {
  return (
    <div style={{ padding: 32, maxWidth: 960 }}>
      <Title level={3}>槽位 Review 工具</Title>
      <Paragraph type="secondary">
        左侧选择一张虚拟表开始 review。所有改动先进入内存，点击"保存改动"才写回磁盘，
        保存前自动备份到 <Text code>output/slot_definitions.yaml.bak</Text>。
      </Paragraph>

      {stats && (
        <Row gutter={16} style={{ marginTop: 24 }}>
          <Col span={6}>
            <Card>
              <Statistic title="虚拟表总数" value={stats.vt_count} />
            </Card>
          </Col>
          <Col span={6}>
            <Card>
              <Statistic title="槽位总数" value={stats.total_slots} />
            </Card>
          </Col>
          <Col span={6}>
            <Card>
              <Statistic
                title="base 复用率"
                value={(stats.overall_base_reuse_ratio * 100).toFixed(1)}
                suffix="%"
              />
            </Card>
          </Col>
          <Col span={6}>
            <Card>
              <Statistic
                title="平均槽位 / VT"
                value={stats.avg_slots_per_vt}
                precision={1}
              />
            </Card>
          </Col>
        </Row>
      )}

      <Title level={4} style={{ marginTop: 32 }}>操作指南</Title>
      <Paragraph>
        <ul>
          <li>左侧按 <Text code>table_type</Text> 分组的 VT 列表，支持搜索</li>
          <li>进入 VT 后可编辑 / 删除 / 新增槽位</li>
          <li><Text code>base</Text> 槽位只能改 role，name 由 base_slots 库控制</li>
          <li><Text code>extended</Text> 槽位可改全部字段；aliases 至少 3 个</li>
          <li>顶部"保存改动"按钮只有在有未保存改动时才亮起</li>
          <li><Tag color="red">待定</Tag> 类型的 VT 是脚手架阶段的 misplaced 表，建议重点 review</li>
        </ul>
      </Paragraph>
    </div>
  )
}
