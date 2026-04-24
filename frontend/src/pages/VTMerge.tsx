import { useEffect, useMemo, useState } from 'react'
import {
  Alert,
  Button,
  Card,
  Drawer,
  Popconfirm,
  Radio,
  Space,
  Spin,
  Statistic,
  Table,
  Tag,
  Typography,
  message,
} from 'antd'
import { api } from '../api'
import type { VTMergeCandidatesResponse, VTMergeGroup } from '../types'
import { emitScaffoldChanged } from '../lib/events'

const { Title, Text, Paragraph } = Typography

const STATUS_COLOR: Record<string, string> = {
  pending: 'default',
  merged: 'green',
  rejected: 'red',
}

const STATUS_LABEL: Record<string, string> = {
  pending: '待处理',
  merged: '已合并',
  rejected: '已拒绝',
}

export default function VTMerge() {
  const [data, setData] = useState<VTMergeCandidatesResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [statusFilter, setStatusFilter] = useState<string>('pending')
  const [selected, setSelected] = useState<VTMergeGroup | null>(null)
  const [primaryId, setPrimaryId] = useState<string>('')
  const [nextActionBanner, setNextActionBanner] = useState<string | null>(null)

  const reload = () => {
    setLoading(true)
    api.vtMergeCandidates(statusFilter === 'all' ? undefined : statusFilter)
      .then(setData)
      .finally(() => setLoading(false))
  }
  useEffect(reload, [statusFilter])

  const openDetail = (g: VTMergeGroup) => {
    setSelected(g)
    setPrimaryId(g.suggested_primary)
  }

  const handleApply = async () => {
    if (!selected || !primaryId) return
    const absorbed = selected.members.map((m) => m.vt_id).filter((id) => id !== primaryId)
    try {
      const resp = await api.applyVTMerge({
        group_id: selected.group_id,
        primary_vt_id: primaryId,
        absorbed_vt_ids: absorbed,
      })
      message.success(`已合并: 保留 ${primaryId}，吸收 ${absorbed.length} 张；新 VT 总数 ${resp.new_vt_total}`)
      if (resp.next_action) setNextActionBanner(resp.next_action)
      emitScaffoldChanged({ source: 'vt-merge', action: 'merge', extra: { primary: primaryId, absorbed } })
      setSelected(null)
      reload()
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    }
  }

  const handleReject = async (g: VTMergeGroup) => {
    try {
      await api.rejectVTMerge({ group_id: g.group_id })
      message.success(`已拒绝 ${g.group_id}`)
      if (selected?.group_id === g.group_id) setSelected(null)
      reload()
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    }
  }

  const stats = useMemo(() => {
    const meta = (data?.meta || {}) as any
    return {
      total_vts: meta.total_vts ?? 0,
      total_groups: meta.total_groups ?? 0,
      total_vts_involved: meta.total_vts_involved ?? 0,
    }
  }, [data])

  const columns = [
    {
      title: 'group_id',
      dataIndex: 'group_id',
      width: 110,
      render: (v: string) => <Text code style={{ fontSize: 11 }}>{v}</Text>,
    },
    {
      title: 'L2 / 类型',
      width: 320,
      render: (_: unknown, g: VTMergeGroup) => (
        <Space direction="vertical" size={0}>
          <Text>{g.l2_path.join(' / ')}</Text>
          <Tag color="blue" style={{ fontSize: 11 }}>{g.table_type}</Tag>
        </Space>
      ),
    },
    {
      title: '成员',
      width: 80,
      render: (_: unknown, g: VTMergeGroup) => <Tag>{g.members.length}</Tag>,
    },
    {
      title: '平均相似度',
      dataIndex: 'avg_score',
      width: 110,
      sorter: (a: VTMergeGroup, b: VTMergeGroup) => a.avg_score - b.avg_score,
      defaultSortOrder: 'descend' as const,
      render: (v: number) => (
        <Tag color={v >= 0.7 ? 'red' : v >= 0.55 ? 'orange' : 'gold'}>{v.toFixed(3)}</Tag>
      ),
    },
    {
      title: '成员 VT',
      render: (_: unknown, g: VTMergeGroup) => (
        <Space size={[0, 4]} wrap>
          {g.members.map((m) => (
            <Tag key={m.vt_id} color={m.vt_id === g.suggested_primary ? 'blue' : 'default'}
                 style={{ fontSize: 11 }}>
              {m.topic}
            </Tag>
          ))}
        </Space>
      ),
    },
    {
      title: '状态',
      dataIndex: 'status',
      width: 90,
      render: (v: string) => <Tag color={STATUS_COLOR[v]}>{STATUS_LABEL[v]}</Tag>,
    },
    {
      title: '操作',
      width: 180,
      render: (_: unknown, g: VTMergeGroup) => (
        <Space size={4}>
          <Button size="small" onClick={() => openDetail(g)}>详情</Button>
          {g.status === 'pending' && (
            <Popconfirm
              title={`确认拒绝 ${g.group_id}？`}
              description="拒绝后该组不会合并，也不再出现在待处理列表"
              onConfirm={() => handleReject(g)}
            >
              <Button size="small" danger>拒绝</Button>
            </Popconfirm>
          )}
        </Space>
      ),
    },
  ]

  if (loading || !data) {
    return <div style={{ padding: 24 }}><Spin /></div>
  }

  return (
    <div style={{ padding: 24 }}>
      <Title level={3} style={{ marginTop: 0 }}>VT 合并（I-13）</Title>
      <Paragraph type="secondary">
        检测到同 L2 + 同 table_type 下语义相近的 VT 组。人工审核每组：合并（保留一个主 VT，其他并入）或拒绝。
        合并会写回 <Text code>virtual_tables_scaffold_final.yaml</Text>（首次合并自动备份为 <Text code>..._before_merge.yaml</Text>），
        完成后运行 <Text code>run_pipeline.py --from slot_definitions</Text>。
      </Paragraph>

      {nextActionBanner && (
        <Alert
          type="warning"
          showIcon
          closable
          message="VT 合并已写入，需重跑下游才能生效"
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
        <Statistic title="当前 VT 总数" value={stats.total_vts} />
        <Statistic title="合并候选组" value={stats.total_groups} />
        <Statistic title="涉及 VT 数" value={stats.total_vts_involved} />
      </Space>

      <Space style={{ marginBottom: 16 }}>
        <Text>状态：</Text>
        <Radio.Group value={statusFilter} onChange={(e) => setStatusFilter(e.target.value)} size="small">
          <Radio.Button value="all">全部</Radio.Button>
          <Radio.Button value="pending">待处理</Radio.Button>
          <Radio.Button value="merged">已合并</Radio.Button>
          <Radio.Button value="rejected">已拒绝</Radio.Button>
        </Radio.Group>
      </Space>

      <Card size="small">
        <Table
          rowKey="group_id"
          size="small"
          dataSource={data.groups}
          columns={columns as any}
          pagination={{ pageSize: 20 }}
        />
      </Card>

      <Drawer
        width={960}
        open={!!selected}
        onClose={() => setSelected(null)}
        title={selected ? `${selected.group_id} · ${selected.l2_path.join(' / ')}` : ''}
        extra={
          selected?.status === 'pending' && (
            <Space>
              <Popconfirm
                title={`确认合并这 ${selected.members.length} 张 VT？`}
                description={<div>
                  <div>保留主 VT: <Text code>{primaryId}</Text></div>
                  <div style={{ color: '#d46b08', marginTop: 4 }}>
                    其余 {selected.members.length - 1} 张将从 scaffold 中删除，合并后需重跑 pipeline
                  </div>
                </div>}
                onConfirm={handleApply}
              >
                <Button type="primary">合并</Button>
              </Popconfirm>
              <Popconfirm title="拒绝这组？" onConfirm={() => selected && handleReject(selected)}>
                <Button danger>拒绝</Button>
              </Popconfirm>
            </Space>
          )
        }
      >
        {selected && (
          <Space direction="vertical" style={{ width: '100%' }} size={16}>
            <Card size="small" title="选择保留的主 VT">
              <Radio.Group value={primaryId} onChange={(e) => setPrimaryId(e.target.value)}
                           disabled={selected.status !== 'pending'}>
                <Space direction="vertical">
                  {selected.members.map((m) => (
                    <Radio key={m.vt_id} value={m.vt_id}>
                      <Space>
                        <Text strong>{m.topic}</Text>
                        <Text code style={{ fontSize: 11 }}>{m.vt_id}</Text>
                        {m.vt_id === selected.suggested_primary && <Tag color="blue">建议</Tag>}
                        <Tag>{m.source_table_count} 源表</Tag>
                        <Tag color="green">auto {m.field_hit_auto}</Tag>
                      </Space>
                    </Radio>
                  ))}
                </Space>
              </Radio.Group>
            </Card>

            <Card size="small" title="成员对比">
              <Table
                size="small"
                rowKey="vt_id"
                pagination={false}
                dataSource={selected.members}
                columns={[
                  { title: 'vt_id', dataIndex: 'vt_id', width: 150, render: (v) => <Text code style={{ fontSize: 11 }}>{v}</Text> },
                  { title: 'topic', dataIndex: 'topic', width: 240 },
                  { title: 'grain_desc', dataIndex: 'grain_desc', ellipsis: true },
                  { title: '源表', dataIndex: 'source_table_count', width: 80, render: (v) => <Tag>{v}</Tag> },
                  {
                    title: '归一命中',
                    width: 160,
                    render: (_: unknown, m: any) => (
                      <Space size={4}>
                        <Tag color="green">auto {m.field_hit_auto}</Tag>
                        <Tag color="gold">review {m.field_hit_needs_review}</Tag>
                      </Space>
                    ),
                  },
                ]}
              />
            </Card>

            <Card size="small" title="源表清单（union 预览）">
              <Space size={[0, 4]} wrap>
                {Array.from(new Set(selected.members.flatMap((m) => m.source_tables))).map((t) => (
                  <Tag key={t} style={{ fontSize: 11 }}>{t}</Tag>
                ))}
              </Space>
              <div style={{ marginTop: 8, color: '#666', fontSize: 12 }}>
                合并后主 VT 将拥有 {new Set(selected.members.flatMap((m) => m.source_tables)).size} 张源表
              </div>
            </Card>

            <Card size="small" title={`相似度证据（${selected.pairwise_evidence.length} 条边）`}>
              <Table
                size="small"
                rowKey={(r) => `${r.a}-${r.b}`}
                pagination={false}
                dataSource={selected.pairwise_evidence}
                columns={[
                  { title: 'A', dataIndex: 'a', render: (v) => <Text code style={{ fontSize: 11 }}>{v}</Text> },
                  { title: 'B', dataIndex: 'b', render: (v) => <Text code style={{ fontSize: 11 }}>{v}</Text> },
                  { title: 'embedding', dataIndex: 'embedding_sim', width: 100, render: (v) => v.toFixed(3) },
                  { title: 'source 覆盖', dataIndex: 'source_overlap', width: 100, render: (v) => v.toFixed(3) },
                  { title: 'jaccard', dataIndex: 'source_jaccard', width: 90, render: (v) => v.toFixed(3) },
                  { title: '组合', dataIndex: 'score', width: 90, render: (v) => <Tag color="blue">{v.toFixed(3)}</Tag> },
                  {
                    title: '触发',
                    dataIndex: 'triggers',
                    render: (v: string[]) => (
                      <Space size={[0, 2]} wrap>
                        {v.map((t) => <Tag key={t} style={{ fontSize: 10 }}>{t}</Tag>)}
                      </Space>
                    ),
                  },
                ]}
              />
            </Card>

            {selected.status !== 'pending' && (
              <Alert
                type={selected.status === 'merged' ? 'success' : 'error'}
                showIcon
                message={`${STATUS_LABEL[selected.status]} · ${selected.applied_at || selected.rejected_at}`}
                description={
                  selected.status === 'merged'
                    ? `保留 ${selected.applied_primary}，吸收 ${selected.applied_absorbed?.length || 0} 张`
                    : '该组已被拒绝，不再合并'
                }
              />
            )}
          </Space>
        )}
      </Drawer>
    </div>
  )
}
