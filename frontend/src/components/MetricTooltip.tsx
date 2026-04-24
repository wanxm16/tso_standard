import { Tooltip, Tag, Typography } from 'antd'
import { QuestionCircleOutlined } from '@ant-design/icons'

const { Text } = Typography

export type MetricKind = 'usage' | 'mapped' | 'usage_mapped'

const METRIC_SPECS: Record<MetricKind, { label: string; short: string; formula: string; nuance: string }> = {
  usage: {
    label: '使用率',
    short: '在 SQL 中真实被使用的字段占比',
    formula: '分子 = usage_count > 0 的字段数；分母 = 该表 sample 字段总数',
    nuance: '口径：单纯看"这个字段有没有出现在 benchmark query 的 SQL 里"。高使用率 = 该表常被查；低 = 冷字段多',
  },
  mapped: {
    label: '审核映射率',
    short: '人工已审核且确认归到某个槽位的字段占比',
    formula: '分子 = reviewed.decision ∈ {map, rewrite} 的字段数；分母 = 该表 sample 字段总数',
    nuance: '口径：mark_noise / skip / 未审 均不计入分子。反映"人工审核完成度 + 成功归槽率"。跟"使用率"相互独立（未用的字段也可能被人工映射）',
  },
  usage_mapped: {
    label: '已用映射率',
    short: '已用字段里被人工映射到槽位的比例',
    formula: '分子 = usage_count > 0 且 reviewed.decision ∈ {map, rewrite}；分母 = usage_count > 0 的字段数',
    nuance: '口径：等于"审核映射率"口径，但分母换成"已用"。这是最能反映"Text2SQL 召回层对热字段的覆盖度"的指标',
  },
}

/** 三率指标的统一 Tooltip。使用方式：
 *   <MetricTooltip kind="usage"><span>使用率：xx/yy (zz%)</span></MetricTooltip>
 * 或者配合 Tag：
 *   <MetricTooltip kind="usage" asLabel />  <Tag>...</Tag>
 */
export function MetricTooltip({
  kind,
  children,
  asLabel = false,
  showIcon = true,
}: {
  kind: MetricKind
  children?: React.ReactNode
  asLabel?: boolean
  showIcon?: boolean
}) {
  const spec = METRIC_SPECS[kind]
  const content = (
    <div style={{ maxWidth: 360, fontSize: 12, lineHeight: 1.7 }}>
      <div style={{ fontWeight: 600, marginBottom: 4 }}>{spec.label}</div>
      <div><Tag style={{ fontSize: 11 }}>定义</Tag> {spec.short}</div>
      <div><Tag style={{ fontSize: 11 }}>公式</Tag> {spec.formula}</div>
      <div style={{ marginTop: 4, color: '#ffe58f' }}>
        <Tag color="gold" style={{ fontSize: 11 }}>注意</Tag> {spec.nuance}
      </div>
    </div>
  )
  if (asLabel) {
    return (
      <Tooltip title={content}>
        <span>
          <strong>{spec.label}：</strong>
          {showIcon && <QuestionCircleOutlined style={{ color: '#bfbfbf', fontSize: 11, marginLeft: 2 }} />}
        </span>
      </Tooltip>
    )
  }
  return <Tooltip title={content}>{children}</Tooltip>
}

/** 一行展示三率完整说明，用在页面头部 */
export function MetricLegend() {
  return (
    <div style={{ display: 'flex', gap: 16, fontSize: 12, color: '#8c8c8c' }}>
      {(['usage', 'mapped', 'usage_mapped'] as MetricKind[]).map((k) => (
        <MetricTooltip key={k} kind={k}>
          <span>
            <Tag style={{ fontSize: 11 }}>{METRIC_SPECS[k].label}</Tag>
            {METRIC_SPECS[k].short}
          </span>
        </MetricTooltip>
      ))}
    </div>
  )
}
