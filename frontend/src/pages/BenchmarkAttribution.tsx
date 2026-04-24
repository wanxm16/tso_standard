import { useEffect, useState } from 'react'
import { Alert, Button, Card, Col, Row, Space, Statistic, Table, Tabs, Tag, Tooltip, Typography, message } from 'antd'
import { Link } from 'react-router-dom'
import { api } from '../api'

const { Title, Text, Paragraph } = Typography

const CHANNEL_META: Record<string, { label: string; hint: string; tone: 'primary' | 'ref' | 'baseline' }> = {
  rerank: {
    label: 'rerank',
    hint: '二阶段：fusion 取 top10 候选池 → qwen3-max 精排取 top5（生产最终通道）',
    tone: 'primary',
  },
  fusion: {
    label: 'fusion',
    hint: '三路 RRF 融合：embedding + intent + slot_top3（默认生产链路）',
    tone: 'primary',
  },
  embedding: {
    label: 'embedding',
    hint: 'query 整段 embed × VT recall_text（topic+summary+typical_questions+字段 aliases）余弦',
    tone: 'primary',
  },
  intent: {
    label: 'intent',
    hint: 'LLM 先抽 query intent（intent_topics + required_fields），再 0.5×topic_sim + 0.5×field_score（2026-04-23 升级为 embedding 版字段匹配）',
    tone: 'primary',
  },
  slot_top3: {
    label: 'slot_top3',
    hint: 'Slot 级召回：query × 每个 slot 向量，按 VT 取 top3 slot 相似度平均',
    tone: 'ref',
  },
  slot_max: {
    label: 'slot_max',
    hint: 'Slot 级召回：每张 VT 只看最相关的那 1 个 slot（纯 max）',
    tone: 'ref',
  },
  fusion_slot: {
    label: 'fusion_slot',
    hint: 'RRF 融合：embedding + slot_top3（二路，对比用）',
    tone: 'ref',
  },
  fusion3: {
    label: 'fusion3',
    hint: '三路 RRF：embedding + intent + slot_top3（= 当前默认 fusion）',
    tone: 'ref',
  },
  fusion4: {
    label: 'fusion4',
    hint: '四路 RRF：embedding + intent + slot_top3 + multi_topic（实验对比：multi_topic 反而稀释主信号）',
    tone: 'ref',
  },
  multi_topic: {
    label: 'multi_topic',
    hint: 'query 按 intent_topics 拆成多个子 query 分别召回，用 RRF 合并（针对多主题 query，但单用效果弱于 embedding）',
    tone: 'ref',
  },
  fusion_v1: {
    label: 'fusion_v1',
    hint: '老版 fusion（仅 embedding + intent 两路）—— 保留做对比',
    tone: 'baseline',
  },
  rerank_emb: {
    label: 'rerank_emb',
    hint: '老版 rerank（用 embedding top10 作 LLM 候选池）—— 保留做对比',
    tone: 'baseline',
  },
  tfidf: {
    label: 'tfidf',
    hint: 'char n-gram 字符相似度基线 —— 对照用不参与生产',
    tone: 'baseline',
  },
}

export default function BenchmarkAttribution() {
  const [data, setData] = useState<any>(null)
  const [metrics, setMetrics] = useState<any>(null)
  const [channel, setChannel] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)

  const load = async (ch?: string | null) => {
    const d = ch
      ? await api.getBenchmarkAttributionByChannel(ch, 'fail', 500)
      : await api.getBenchmarkAttribution(true, 500)
    setData(d)
  }
  const loadMetrics = async () => {
    try {
      const m = await api.getBenchmarkMetrics()
      setMetrics(m)
    } catch { /* no-op */ }
  }
  const [topk, setTopk] = useState<any>(null)
  const loadTopk = async () => {
    try {
      const t = await api.getBenchmarkChannelTopk(5)
      setTopk(t)
    } catch { /* no-op */ }
  }
  useEffect(() => { load(); loadMetrics(); loadTopk() }, [])

  // 首次返回后把主通道作为默认筛选
  useEffect(() => {
    if (data?.summary?.primary_channel && channel === null) {
      const ch = data.summary.primary_channel
      setChannel(ch)
      load(ch)
    }
  }, [data?.summary?.primary_channel])

  const regenerate = async () => {
    setLoading(true)
    try {
      await api.regenerateBenchmarkAttribution()
      message.success('归因已重跑')
      await load(channel)
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    } finally {
      setLoading(false)
    }
  }


  if (!data || !data.exists) {
    return (
      <div style={{ padding: 24 }}>
        <Alert type="warning" message="归因产物尚未生成" description="点击下方按钮跑一次归因脚本（需先有 evaluation_details.parquet）" showIcon />
        <Button type="primary" onClick={regenerate} loading={loading} style={{ marginTop: 16 }}>
          跑归因
        </Button>
      </div>
    )
  }

  const s = data.summary
  const channels = s.channels || {}
  const primary = s.primary_channel
  const currentCh = channel || primary
  const currentChInfo = currentCh ? channels[currentCh] : null

  const channelOrder = ['rerank', 'embedding', 'fusion', 'tfidf', 'intent'].filter((c) => channels[c])

  return (
    <div style={{ padding: 24 }}>
      <Space style={{ width: '100%', justifyContent: 'space-between', marginBottom: 12 }}>
        <Title level={4} style={{ margin: 0 }}>benchmark 召回失败归因</Title>
        <Button onClick={regenerate} loading={loading}>重跑归因</Button>
      </Space>

      <Paragraph type="secondary" style={{ marginBottom: 16 }}>
        benchmark 共 <Text strong>{s.unique_queries}</Text> 条独立 query（跨切片去重）。下面 Tabs 依次展示多切片指标矩阵、Top VT 缺槽位、每条 query 在各通道的 top5 召回对比。
      </Paragraph>


      <Row gutter={12} style={{ marginBottom: 16 }}>
        <Col span={6}>
          <Card>
            <Statistic
              title={`当前通道 ${currentCh || '-'} · 归因行数`}
              value={currentChInfo ? `${currentChInfo.rows} 行 / ${currentChInfo.unique_queries} 独立 query` : '-'}
              valueStyle={{ fontSize: 18 }}
            />
          </Card>
        </Col>
        {currentChInfo && (
          <Col span={10}>
            <Card>
              <Space size={32}>
                <Statistic
                  title={`当前通道 · ${CHANNEL_META[currentCh || '']?.label || currentCh} top5 recall`}
                  value={((currentChInfo.top5_recall ?? 0) * 100).toFixed(1)}
                  suffix="%"
                  valueStyle={{ color: (currentChInfo.top5_recall ?? 0) >= 0.5 ? '#52c41a' : (currentChInfo.top5_recall ?? 0) >= 0.3 ? '#fa8c16' : '#f5222d' }}
                />
                <Statistic
                  title="命中 / 失败"
                  value={`${currentChInfo.top5_hits} / ${currentChInfo.failed}`}
                  valueStyle={{ fontSize: 20 }}
                />
              </Space>
            </Card>
          </Col>
        )}
        <Col span={8}>
          <Card>
            <div style={{ fontSize: 13, color: '#888', marginBottom: 8 }}>各通道 top5 recall 对比</div>
            <Space wrap size={[8, 8]}>
              {channelOrder.map((c) => {
                const ci = channels[c]
                const rate = ((ci.top5_recall ?? 0) * 100).toFixed(1)
                const meta = CHANNEL_META[c] || { label: c, hint: '', tone: 'baseline' }
                const isPrimary = c === primary
                const isCurrent = c === currentCh
                const color = isCurrent ? 'processing' : meta.tone === 'baseline' ? 'default' : undefined
                return (
                  <Tooltip key={c} title={meta.hint}>
                    <Tag
                      color={color}
                      style={{ cursor: 'pointer', fontSize: 12, padding: '4px 8px' }}
                      onClick={() => {
                        setChannel(c)
                        load(c)
                      }}
                    >
                      {meta.label} {rate}%{isPrimary ? ' ★' : ''}
                    </Tag>
                  </Tooltip>
                )
              })}
            </Space>
          </Card>
        </Col>
      </Row>

      <Tabs
        defaultActiveKey="metrics"
        style={{ marginBottom: 16 }}
        items={[
          {
            key: 'metrics',
            label: '多切片指标矩阵',
            children: metrics?.exists && metrics.slices?.length > 0 ? (
              <div>
                <Paragraph type="secondary" style={{ fontSize: 12, marginBottom: 12 }}>
                  每格 <Text strong>table_recall</Text>（expected 表中被召回的比例）；颜色：≥50% 绿 / ≥30% 橙 / 否则灰。
                  切片：all=json+csv 去重后 82 条；default=json 22 条；csv=csv 60 条；csv_flag1=csv 中 flag=1 的 10 条高优先级 query。
                </Paragraph>
                <Space direction="vertical" size={16} style={{ width: '100%' }}>
                  {metrics.slices.map((slc: any) => {
                    const channelsInSlice = Object.keys(slc.channels || {})
                    const chanOrder = [
                      'rerank', 'fusion', 'embedding', 'intent',
                      'slot_top3', 'fusion_v1', 'rerank_emb',
                      'multi_topic', 'fusion4', 'slot_max', 'fusion_slot',
                      'fusion3', 'tfidf',
                    ].filter((c) => channelsInSlice.includes(c))
                    const rows = chanOrder.map((ch) => ({ channel: ch, byK: slc.channels[ch] || {} }))
                    const ks = ['k=5', 'k=10']
                    const colorFor = (v: number) =>
                      v >= 0.5 ? '#52c41a' : v >= 0.3 ? '#fa8c16' : '#8c8c8c'
                    return (
                      <div key={slc.slice_key}>
                        <Text strong style={{ fontSize: 13 }}>
                          {slc.slice_label}
                        </Text>
                        <Table
                          size="small"
                          pagination={false}
                          rowKey="channel"
                          dataSource={rows}
                          style={{ marginTop: 4 }}
                          columns={[
                            {
                              title: 'channel',
                              dataIndex: 'channel',
                              width: 140,
                              render: (c: string) => {
                                const meta = CHANNEL_META[c]
                                const isBase = meta?.tone === 'baseline'
                                const isRef = meta?.tone === 'ref'
                                return (
                                  <Tooltip title={meta?.hint || c} styles={{ root: { maxWidth: 480 } }}>
                                    <Tag
                                      color={isBase ? 'default' : isRef ? 'geekblue' : 'blue'}
                                      style={{ cursor: 'help' }}
                                    >
                                      {c}
                                    </Tag>
                                  </Tooltip>
                                )
                              },
                            },
                            ...ks.map((k) => ({
                              title: k === 'k=5' ? 'top5 recall' : 'top10 recall',
                              dataIndex: ['byK', k],
                              width: 130,
                              render: (_v: any, r: any) => {
                                const p = r.byK?.[k]
                                if (!p) return <Text type="secondary">—</Text>
                                const rec = p.table_recall_rate
                                return (
                                  <span style={{ fontSize: 13, color: colorFor(rec), fontWeight: 600 }}>
                                    {(rec * 100).toFixed(1)}%
                                  </span>
                                )
                              },
                            })),
                          ]}
                        />
                      </div>
                    )
                  })}
                </Space>
              </div>
            ) : (
              <Text type="secondary">evaluation.json 尚未生成或为空，请先跑 I-08 评估</Text>
            ),
          },
          {
            key: 'top_vts',
            label: `Top VT 缺槽位（${s.top_vts_missing?.length || 0}）`,
            children: (
              <div>
                <Paragraph type="secondary" style={{ marginBottom: 8, fontSize: 12 }}>
                  这些 VT 是 benchmark 失败 query 的高频候选，但 aliases 覆盖不够。点进去可以补充槽位别名。
                </Paragraph>
                <Space wrap>
                  {(s.top_vts_missing || []).map((v: any) => (
                    <Link key={v.vt_id} to={`/vt/${v.vt_id}`}>
                      <Tag color="red" style={{ cursor: 'pointer' }}>
                        {v.vt_id} · 缺 {v.missing_keyword_count}
                      </Tag>
                    </Link>
                  ))}
                </Space>
              </div>
            ),
          },
          {
            key: 'channel_topk',
            label: `通道召回对比${topk?.queries ? `（${topk.queries.length}）` : ''}`,
            children: topk?.exists ? (
              <div>
                <Paragraph type="secondary" style={{ fontSize: 12, marginBottom: 12 }}>
                  每条 query 在 <Text strong>embedding / fusion / rerank</Text> 三个通道的 top{topk.topk} 召回 VT 对比。
                  ✓ 表示该 VT 命中了 expected 表之一。按「最差 recall」升序：最需要排查的在最前。
                </Paragraph>
                <Table
                  size="small"
                  rowKey="query_text"
                  pagination={{ pageSize: 15 }}
                  dataSource={topk.queries}
                  expandable={{
                    expandedRowRender: (r: any) => (
                      <div style={{ padding: 8 }}>
                        <div style={{ marginBottom: 8 }}>
                          <Text strong>expected tables（{r.n_expected}）：</Text>
                          <Space size={[4, 4]} wrap style={{ marginLeft: 4 }}>
                            {r.expected_tables.map((t: string) => (
                              <Tag key={t} color="blue" style={{ fontSize: 11 }}>{t}</Tag>
                            ))}
                          </Space>
                        </div>
                        <div style={{ marginBottom: 8 }}>
                          <Text strong>expected VT：</Text>
                          <Space size={[4, 4]} wrap style={{ marginLeft: 4 }}>
                            {r.expected_vt_ids.map((v: string) => (
                              <Link key={v} to={`/vt/${v}`}>
                                <Tag color="purple" style={{ fontSize: 11 }}>{v}</Tag>
                              </Link>
                            ))}
                          </Space>
                        </div>
                        {topk.channels.map((ch: string) => {
                          const cd = r.channels[ch]
                          if (!cd) return null
                          return (
                            <div key={ch} style={{ marginBottom: 6 }}>
                              <Space size={8}>
                                <Tag color={ch === 'rerank' ? 'magenta' : ch === 'fusion' ? 'blue' : ch === 'embedding' ? 'cyan' : 'default'}>
                                  {ch}
                                </Tag>
                                <Tag color={cd.hit ? 'green' : 'red'} style={{ fontSize: 11 }}>
                                  {cd.hit ? 'hit' : 'miss'} · recall {(cd.recall * 100).toFixed(0)}%
                                </Tag>
                              </Space>
                              <Space size={[4, 4]} wrap style={{ marginLeft: 8, marginTop: 4 }}>
                                {cd.top_vts.map((v: any, i: number) => (
                                  <Link key={i} to={`/vt/${v.vt_id}`}>
                                    <Tag
                                      color={v.is_expected ? 'green' : 'default'}
                                      style={{ fontSize: 11, cursor: 'pointer' }}
                                    >
                                      {v.is_expected && '✓ '}
                                      {v.vt_id.slice(-8)} · {v.topic || '?'}
                                    </Tag>
                                  </Link>
                                ))}
                              </Space>
                            </div>
                          )
                        })}
                      </div>
                    ),
                  }}
                  columns={[
                    {
                      title: 'query',
                      dataIndex: 'query_text',
                      ellipsis: true,
                      render: (t: string) => <Text style={{ fontSize: 12 }}>{t}</Text>,
                    },
                    {
                      title: 'expected',
                      dataIndex: 'n_expected',
                      width: 80,
                      align: 'right' as const,
                      render: (v: number) => `${v} 张`,
                    },
                    ...topk.channels.map((ch: string) => ({
                      title: ch,
                      dataIndex: ['channels', ch],
                      width: 110,
                      render: (cd: any) => {
                        if (!cd) return <Text type="secondary">—</Text>
                        const rec = (cd.recall * 100).toFixed(0)
                        const color = cd.recall >= 0.5 ? '#52c41a' : cd.recall >= 0.3 ? '#fa8c16' : '#8c8c8c'
                        return (
                          <Space size={4}>
                            <span style={{ color: cd.hit ? '#52c41a' : '#bfbfbf' }}>{cd.hit ? '✓' : '✗'}</span>
                            <span style={{ color, fontWeight: 600, fontSize: 12 }}>{rec}%</span>
                          </Space>
                        )
                      },
                    })),
                    {
                      title: 'best',
                      dataIndex: 'best_recall',
                      width: 80,
                      align: 'right' as const,
                      defaultSortOrder: 'ascend' as const,
                      sorter: (a: any, b: any) => a.best_recall - b.best_recall,
                      render: (v: number) => {
                        const color = v >= 0.5 ? '#52c41a' : v >= 0.3 ? '#fa8c16' : '#f5222d'
                        return <span style={{ color, fontWeight: 600 }}>{(v * 100).toFixed(0)}%</span>
                      },
                    },
                  ]}
                />
              </div>
            ) : (
              <Text type="secondary">evaluation_details.parquet 尚未生成</Text>
            ),
          },
        ]}
      />

    </div>
  )
}
