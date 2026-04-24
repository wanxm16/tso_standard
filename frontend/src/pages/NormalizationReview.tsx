import { useCallback, useEffect, useMemo, useState } from 'react'
import {
  Button,
  Card,
  Descriptions,
  Drawer,
  Form,
  Input,
  message,
  Popconfirm,
  Radio,
  Select,
  Space,
  Spin,
  Switch,
  Table,
  Tag,
  Tooltip,
  Typography,
} from 'antd'
import { CheckCircleOutlined, FilterOutlined } from '@ant-design/icons'
import { useSearchParams } from 'react-router-dom'
import { api } from '../api'
import { STATUS_COLOR, STATUS_LABEL } from '../types'
import type { NormalizationItem } from '../types'

const { Text, Paragraph } = Typography

const STATUS_OPTIONS = [
  { value: '', label: '全部状态' },
  { value: 'conflict', label: '⚠️ 冲突' },
  { value: 'low_confidence', label: '🔴 低置信' },
  { value: 'needs_review', label: '🟡 需审核' },
  { value: 'auto_accepted', label: '🟢 自动通过' },
]

const PAGE_SIZE = 30

function ScoreBar({ score }: { score: number }) {
  const color = score >= 0.65 ? '#52c41a' : score >= 0.45 ? '#faad14' : '#ff4d4f'
  return (
    <Space direction="vertical" size={0}>
      <Text strong style={{ color }}>
        {score.toFixed(3)}
      </Text>
    </Space>
  )
}

export default function NormalizationReview() {
  const [searchParams, setSearchParams] = useSearchParams()
  const [items, setItems] = useState<NormalizationItem[]>([])
  const [total, setTotal] = useState(0)
  const [loading, setLoading] = useState(false)
  const [page, setPage] = useState(1)
  const [keyword, setKeyword] = useState('')
  const [statusFilter, setStatusFilter] = useState(searchParams.get('status') || 'conflict')
  const [onlyUnreviewed, setOnlyUnreviewed] = useState(true)
  const [selected, setSelected] = useState<NormalizationItem | null>(null)
  const [submitting, setSubmitting] = useState(false)
  const [form] = Form.useForm()

  const fetch = useCallback(() => {
    setLoading(true)
    api
      .normList({
        status: statusFilter || undefined,
        keyword: keyword || undefined,
        only_unreviewed: onlyUnreviewed,
        limit: PAGE_SIZE,
        offset: (page - 1) * PAGE_SIZE,
      })
      .then((r) => {
        setItems(r.items)
        setTotal(r.total)
      })
      .finally(() => setLoading(false))
  }, [statusFilter, keyword, onlyUnreviewed, page])

  useEffect(() => {
    fetch()
  }, [fetch])

  useEffect(() => {
    const s = searchParams.get('status')
    if (s && s !== statusFilter) {
      setStatusFilter(s)
      setPage(1)
    }
  }, [searchParams])

  const openDrawer = (record: NormalizationItem) => {
    setSelected(record)
    form.resetFields()
    form.setFieldsValue({ decision: 'accept_top1' })
  }

  const closeDrawer = () => {
    setSelected(null)
    form.resetFields()
  }

  const handleSubmit = async () => {
    if (!selected) return
    const values = await form.validateFields()
    setSubmitting(true)
    try {
      await api.normDecision({
        table_en: selected.table_en,
        field_name: selected.field_name,
        vt_id: selected.vt_id,
        decision: values.decision,
        selected_slot: values.selected_slot,
        new_slot_name: values.new_slot_name,
        new_slot_cn_name: values.new_slot_cn_name,
        reviewer_note: values.reviewer_note,
      })
      message.success('审核已记录')
      closeDrawer()
      fetch()
    } catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: string } }; message?: string }
      message.error(err.response?.data?.detail || err.message || '提交失败')
    } finally {
      setSubmitting(false)
    }
  }

  const columns = useMemo(
    () => [
      {
        title: '字段',
        key: 'field',
        width: 240,
        render: (_: unknown, r: NormalizationItem) => (
          <Space direction="vertical" size={0}>
            <Text strong>{r.field_name}</Text>
            <Text type="secondary" style={{ fontSize: 12 }}>
              {r.field_comment || '(无注释)'}
            </Text>
          </Space>
        ),
      },
      {
        title: 'VT',
        dataIndex: 'vt_id',
        width: 180,
        render: (v: string) => <Text code style={{ fontSize: 12 }}>{v}</Text>,
      },
      {
        title: '状态',
        dataIndex: 'review_status',
        width: 110,
        render: (v: string) => <Tag color={STATUS_COLOR[v]}>{STATUS_LABEL[v] || v}</Tag>,
      },
      {
        title: 'top1 分数',
        dataIndex: 'top1_score',
        width: 90,
        render: (v: number) => <ScoreBar score={v} />,
      },
      {
        title: 'top1 槽位',
        dataIndex: 'top1_slot',
        width: 160,
        render: (v: string, r: NormalizationItem) => (
          <Space direction="vertical" size={0}>
            <Tag color="blue">{v}</Tag>
            {r.top2_slot && (
              <Text type="secondary" style={{ fontSize: 11 }}>
                top2: {r.top2_slot} ({r.top2_score?.toFixed(2)})
              </Text>
            )}
          </Space>
        ),
      },
      {
        title: '冲突',
        dataIndex: 'conflict_types',
        width: 200,
        render: (v: string[]) =>
          v && v.length > 0 ? (
            <Space size={[0, 2]} wrap>
              {v.map((ct) => (
                <Tag key={ct} color="volcano" style={{ fontSize: 11, marginBottom: 2 }}>
                  {ct}
                </Tag>
              ))}
            </Space>
          ) : null,
      },
      {
        title: 'LLM',
        dataIndex: 'applied_llm',
        width: 70,
        render: (v: boolean, r: NormalizationItem) =>
          v ? (
            <Tooltip title={`${r.llm_trigger} · ${r.llm_reason?.slice(0, 80) || ''}`}>
              <Tag color="geekblue">LLM</Tag>
            </Tooltip>
          ) : null,
      },
      {
        title: '已审核',
        dataIndex: 'decision',
        width: 100,
        render: (v: string | null, r: NormalizationItem) =>
          v ? (
            <Tooltip title={`${v} → ${r.decision_slot} @ ${r.reviewed_at}`}>
              <Tag icon={<CheckCircleOutlined />} color="green">{v}</Tag>
            </Tooltip>
          ) : null,
      },
      {
        title: '操作',
        key: 'actions',
        width: 150,
        render: (_: unknown, r: NormalizationItem) => (
          <Space size={4}>
            <Button size="small" onClick={() => openDrawer(r)}>
              审核
            </Button>
            {r.decision && (
              <Popconfirm
                title="撤销这条审核决策？"
                description="会删掉 reviewed.parquet 对应行，下次 pipeline 跑会回到自动决策流程。已因此创建的 slot 不会被删除。"
                onConfirm={async () => {
                  try {
                    await api.undoNormReviewed({
                      table_en: r.table_en,
                      field_name: r.field_name,
                      vt_id: r.vt_id,
                    })
                    message.success('已撤销')
                    fetch()
                  } catch (e: any) {
                    message.error(e?.response?.data?.detail || String(e))
                  }
                }}
              >
                <Button size="small" danger>撤销</Button>
              </Popconfirm>
            )}
          </Space>
        ),
      },
    ],
    [fetch],
  )

  return (
    <div style={{ padding: 24 }}>
      <Card size="small" style={{ marginBottom: 16 }}>
        <Space wrap>
          <FilterOutlined />
          <Select
            value={statusFilter}
            style={{ width: 160 }}
            options={STATUS_OPTIONS}
            onChange={(v) => {
              setStatusFilter(v)
              setPage(1)
              if (v) setSearchParams({ status: v })
              else setSearchParams({})
            }}
          />
          <Input.Search
            placeholder="搜索字段名/注释/表名"
            value={keyword}
            onChange={(e) => setKeyword(e.target.value)}
            onSearch={() => { setPage(1); fetch() }}
            style={{ width: 280 }}
            allowClear
          />
          <Switch
            checked={onlyUnreviewed}
            onChange={(v) => { setOnlyUnreviewed(v); setPage(1) }}
          />
          <Text>只看未审核</Text>
          <Text type="secondary" style={{ marginLeft: 16 }}>
            共 {total} 条
          </Text>
        </Space>
      </Card>

      <Spin spinning={loading}>
        <Table
          size="small"
          rowKey={(r) => `${r.table_en}|${r.field_name}|${r.vt_id}`}
          dataSource={items}
          columns={columns}
          pagination={{
            current: page,
            pageSize: PAGE_SIZE,
            total,
            onChange: setPage,
            showTotal: (t) => `共 ${t} 条`,
          }}
        />
      </Spin>

      <Drawer
        title={selected ? `审核：${selected.field_name}` : ''}
        width={920}
        open={!!selected}
        onClose={closeDrawer}
        extra={
          <Space>
            <Button onClick={closeDrawer}>取消</Button>
            <Popconfirm
              title="把该字段加入全局黑名单？"
              description={
                <div>
                  <div>加入后该字段在 <Text code>is_technical_noise</Text> 中被标记为 True，不再参与归一打分。</div>
                  <div style={{ color: '#999', marginTop: 4 }}>
                    粒度: 默认加全局 exact_name（所有表中该名字都剔除）。需要精确到本表+字段的请直接编辑 yaml。
                  </div>
                  <div style={{ color: '#d46b08', marginTop: 4 }}>
                    生效需重跑: python3 scripts/run_pipeline.py --from field_features
                  </div>
                </div>
              }
              onConfirm={async () => {
                if (!selected) return
                try {
                  const resp = await api.addFieldBlacklist({
                    mode: 'exact_name',
                    value: selected.field_name,
                    reason: `来自 /normalization/review (${selected.table_en})`,
                  })
                  if (resp.added) {
                    message.success(`已剔除: ${selected.field_name}，请重跑 --from field_features`)
                  } else {
                    message.info('该字段名已在黑名单中')
                  }
                  closeDrawer()
                  fetch()
                } catch (e: any) {
                  message.error(e?.response?.data?.detail || String(e))
                }
              }}
            >
              <Button danger>标记为噪声（剔除）</Button>
            </Popconfirm>
            <Button type="primary" loading={submitting} onClick={handleSubmit}>
              提交
            </Button>
          </Space>
        }
      >
        {selected && (
          <>
            <Descriptions size="small" column={2} bordered style={{ marginBottom: 16 }}>
              <Descriptions.Item label="字段名">{selected.field_name}</Descriptions.Item>
              <Descriptions.Item label="类型">{selected.data_type}</Descriptions.Item>
              <Descriptions.Item label="注释" span={2}>
                {selected.field_comment || <Text type="secondary">(无)</Text>}
              </Descriptions.Item>
              <Descriptions.Item label="物理表">{selected.table_en}</Descriptions.Item>
              <Descriptions.Item label="L1 / L2">
                {selected.table_l1} / {selected.table_l2}
              </Descriptions.Item>
              <Descriptions.Item label="VT">{selected.vt_id}</Descriptions.Item>
              <Descriptions.Item label="状态">
                <Tag color={STATUS_COLOR[selected.review_status]}>
                  {STATUS_LABEL[selected.review_status]}
                </Tag>
              </Descriptions.Item>
              <Descriptions.Item label="注释命中关键词" span={2}>
                <Space size={[0, 4]} wrap>
                  {selected.comment_keywords.map((k) => (
                    <Tag key={k} color="cyan">{k}</Tag>
                  ))}
                </Space>
              </Descriptions.Item>
              <Descriptions.Item label="样例 pattern" span={2}>
                <Space size={[0, 4]} wrap>
                  {selected.sample_patterns.map((k) => (
                    <Tag key={k}>{k}</Tag>
                  ))}
                </Space>
              </Descriptions.Item>
              <Descriptions.Item label="样例值" span={2}>
                <Paragraph style={{ margin: 0, fontSize: 12 }}>
                  {selected.sample_values.join('  |  ') || <Text type="secondary">(无)</Text>}
                </Paragraph>
              </Descriptions.Item>
              <Descriptions.Item label="冲突类型" span={2}>
                {selected.conflict_types.length > 0 ? (
                  <Space size={[0, 4]} wrap>
                    {selected.conflict_types.map((c) => (
                      <Tag key={c} color="volcano">{c}</Tag>
                    ))}
                  </Space>
                ) : (
                  <Text type="secondary">(无)</Text>
                )}
              </Descriptions.Item>
            </Descriptions>

            <Card title="Top 3 候选槽位" size="small" style={{ marginBottom: 16 }}>
              {[1, 2, 3].map((i) => {
                const slot = selected[`top${i}_slot` as keyof NormalizationItem] as string | null
                const score = selected[`top${i}_score` as keyof NormalizationItem] as number | null
                if (!slot) return null
                return (
                  <div
                    key={i}
                    style={{
                      padding: 8,
                      marginBottom: 8,
                      border: '1px solid #f0f0f0',
                      borderRadius: 4,
                      background: i === 1 ? '#f6ffed' : undefined,
                    }}
                  >
                    <Space>
                      <Tag color={i === 1 ? 'green' : 'default'}>top{i}</Tag>
                      <Text strong>{slot}</Text>
                      {score !== null && <ScoreBar score={score} />}
                    </Space>
                  </div>
                )
              })}
            </Card>

            {selected.applied_llm && (
              <Card title="LLM 兜底建议" size="small" style={{ marginBottom: 16 }}>
                <Descriptions size="small" column={1}>
                  <Descriptions.Item label="触发场景">
                    <Tag color="geekblue">
                      {selected.llm_trigger === 'A' && 'A: 缩写+无注释'}
                      {selected.llm_trigger === 'B' && 'B: 规则-样例冲突'}
                      {selected.llm_trigger === 'C' && 'C: 注释模糊'}
                      {selected.llm_trigger === 'D' && 'D: top1/top2 小分差'}
                    </Tag>
                  </Descriptions.Item>
                  {selected.llm_suggested_slot && (
                    <Descriptions.Item label="LLM 建议槽位">
                      <Tag color="blue">{selected.llm_suggested_slot}</Tag>
                    </Descriptions.Item>
                  )}
                  {selected.llm_propose_new_slot && selected.llm_propose_new_slot !== 'null' && (
                    <Descriptions.Item label="LLM 建议新建槽位">
                      <Tag color="purple">{selected.llm_propose_new_slot}</Tag>
                    </Descriptions.Item>
                  )}
                  <Descriptions.Item label="LLM 理由">
                    <Text type="secondary">{selected.llm_reason}</Text>
                  </Descriptions.Item>
                </Descriptions>
              </Card>
            )}

            <Card title="审核决策" size="small">
              <Form form={form} layout="vertical">
                <Form.Item name="decision" label="决定" rules={[{ required: true }]}>
                  <Radio.Group>
                    <Space direction="vertical">
                      <Radio value="accept_top1">接受 top1 ({selected.top1_slot})</Radio>
                      {selected.top2_slot && (
                        <Radio value="use_top2">改用 top2 ({selected.top2_slot})</Radio>
                      )}
                      {selected.top3_slot && (
                        <Radio value="use_top3">改用 top3 ({selected.top3_slot})</Radio>
                      )}
                      <Radio value="use_slot">归入其他已有槽位…</Radio>
                      <Radio value="mark_new_slot">标记为新槽位…</Radio>
                      <Radio value="skip">跳过（暂不决策）</Radio>
                    </Space>
                  </Radio.Group>
                </Form.Item>

                <Form.Item noStyle shouldUpdate={(a, b) => a.decision !== b.decision}>
                  {({ getFieldValue }) => {
                    const v = getFieldValue('decision')
                    if (v === 'use_slot') {
                      return (
                        <Form.Item
                          label="其他已有槽位名"
                          name="selected_slot"
                          rules={[{ required: true }]}
                        >
                          <Input placeholder="如 gender / region_name / custom_slot_xxx" />
                        </Form.Item>
                      )
                    }
                    if (v === 'mark_new_slot') {
                      return (
                        <>
                          <Form.Item
                            label="新槽位名（snake_case）"
                            name="new_slot_name"
                            rules={[
                              { required: true },
                              { pattern: /^[a-z][a-z0-9_]*$/, message: 'snake_case' },
                            ]}
                          >
                            <Input placeholder="如 political_status" />
                          </Form.Item>
                          <Form.Item label="中文名" name="new_slot_cn_name">
                            <Input placeholder="如 政治面貌" />
                          </Form.Item>
                        </>
                      )
                    }
                    return null
                  }}
                </Form.Item>

                <Form.Item label="备注（可选）" name="reviewer_note">
                  <Input.TextArea rows={2} />
                </Form.Item>
              </Form>
            </Card>
          </>
        )}
      </Drawer>
    </div>
  )
}
