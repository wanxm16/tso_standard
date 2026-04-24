export interface MappedField {
  table_en: string
  field_name: string
  field_comment?: string
}

export interface Slot {
  name: string
  from: 'base' | 'extended'
  role: string
  cn_name?: string
  logical_type?: string
  aliases?: string[]
  applicable_table_types?: string[]
  llm_reason?: string
  mapped_fields?: MappedField[]
  source?: string
}

export interface BaseSlot {
  name: string
  cn_name: string
  logical_type: string
  role: string
  description: string
  aliases: string[]
  sample_patterns: Array<{ name: string; regex: string }>
  applicable_table_types: string[]
}

export interface VTSummary {
  vt_id: string
  topic: string
  table_type: string
  l2_path: string[]
  grain_desc: string
  source_table_count: number
  slot_count: number
  slot_status?: 'has_slots' | 'no_slots'
  is_pending: boolean
}

export interface SourceTableField {
  field: string
  type: string
  comment: string
  sample_data: string
  used: boolean          // usage_count > 0（字段是否有 SQL 使用记录）
  usage_count?: number
  sql_count?: number
  mapped_slot: string | null
  match_reason: string
  review_status?: string | null
  selected_score?: number | null
  top2_slot?: string | null
  top2_score?: number | null
}

export interface SourceTableWithFields {
  en: string
  cn: string
  field_count: number
  fields_sample: SourceTableField[]
  sample_size: number
  used_count: number
  coverage_ratio: number
  mapped_count: number
  mapped_ratio: number
  used_mapped_count: number
  used_mapped_ratio: number
}

export interface ReviewHint {
  llm_suggested_l2: string
  current_l2: string
  items?: Array<{
    en: string
    cn: string
    suggested_l2: string
    reason: string
  }>
}

export interface VTDetail {
  vt_id: string
  topic: string
  table_type: string
  l2_path: string[]
  grain_desc: string
  source_table_count: number
  slots: Slot[]
  summary: string
  is_pending: boolean
  source_tables_with_fields: SourceTableWithFields[]
  review_hint?: ReviewHint | null
}

export interface Stats {
  vt_count: number
  total_slots: number
  total_base_refs: number
  total_extended: number
  overall_base_reuse_ratio: number
  avg_slots_per_vt: number
  last_edited_at?: string
}

export const ROLE_OPTIONS = [
  'subject',
  'subject_id',
  'relation_subject',
  'display',
  'time',
  'location',
  'filter',
  'measure',
  'source',
  'description',
] as const

export const TABLE_TYPE_COLOR: Record<string, string> = {
  主档: 'blue',
  事件: 'green',
  关系: 'purple',
  聚合: 'cyan',
  标签: 'orange',
  待定: 'red',
}

// ========== I-05 归一审核 ==========

export interface NormalizationItem {
  table_en: string
  field_name: string
  field_comment: string
  vt_id: string
  review_status: 'auto_accepted' | 'needs_review' | 'low_confidence' | 'conflict'
  selected_slot: string
  selected_score: number
  selected_slot_from: string
  top1_slot: string
  top1_score: number
  top2_slot: string | null
  top2_score: number | null
  top3_slot: string | null
  top3_score: number | null
  score_gap_top1_top2: number
  conflict_types: string[]
  applied_llm: boolean
  llm_trigger: string | null
  llm_suggested_slot: string | null
  llm_propose_new_slot: string | null
  llm_reason: string | null
  decision: string | null
  decision_slot: string | null
  reviewed_at: string | null
  reviewer_note: string | null
  sample_values: string[]
  comment_keywords: string[]
  sample_patterns: string[]
  data_type: string
  table_l1: string
  table_l2: string
}

export interface NormalizationStats {
  total_rows: number
  unique_fields: number
  unique_vts: number
  by_status: Record<string, number>
  by_conflict_type: Record<string, number>
  llm_applied: number
  llm_propose_new_slot: number
  reviewed: number
}

export interface NewSlotCandidate {
  name: string
  cn_name: string
  support_count: number
  vt_count: number
  example_fields: string[]
  example_vts: string[]
  example_comments: string[]
}

export interface FieldMapping {
  table_en: string
  field_name: string
  field_comment: string
  selected_slot: string
  selected_score: number
  review_status: string
  top1_slot: string
  top1_score: number
  top2_slot: string | null
  top2_score: number | null
  conflict_types: string[]
  applied_llm: boolean
  llm_suggested_slot: string | null
  decision: string | null
  decision_slot: string | null
}

// ========== I-13 VT 合并 ==========

export interface VTMergeMember {
  vt_id: string
  topic: string
  grain_desc: string
  source_table_count: number
  source_tables: string[]
  field_hit_auto: number
  field_hit_needs_review: number
  field_hit_total: number
}

export interface VTMergeEdge {
  a: string
  b: string
  embedding_sim: number
  source_overlap: number
  source_jaccard: number
  score: number
  triggers: string[]
}

export interface VTMergeGroup {
  group_id: string
  l1_path: string
  l2_path: string[]
  table_type: string
  avg_score: number
  suggested_primary: string
  members: VTMergeMember[]
  pairwise_evidence: VTMergeEdge[]
  status: 'pending' | 'merged' | 'rejected'
  created_at: string
  applied_at?: string
  rejected_at?: string
  applied_primary?: string
  applied_absorbed?: string[]
}

export interface VTMergeCandidatesResponse {
  meta: Record<string, unknown>
  total: number
  groups: VTMergeGroup[]
}

// ========== I-12 槽位库 ==========

export interface SlotLibraryEntry {
  name: string
  cn_name: string
  logical_type: string
  role: string
  description: string
  aliases: string[]
  sample_patterns: unknown[]
  applicable_table_types: string[]
  used_by_vt_count: number
  field_hit_count: number
  auto_accepted_count: number
  needs_review_count: number
  low_confidence_count: number
  conflict_count: number
}

export interface ExtendedSlotEntry extends Omit<SlotLibraryEntry, 'used_by_vt_count' | 'low_confidence_count'> {
  low_confidence_count?: number
}

export interface ExtendedSlotGroup {
  vt_id: string
  topic: string
  table_type: string
  l2_path: string[]
  extended_slots: ExtendedSlotEntry[]
}

export interface SlotLibraryResponse {
  base_slots: SlotLibraryEntry[]
  domain_slots: SlotLibraryEntry[]
  extended_by_vt: ExtendedSlotGroup[]
  stats: {
    base_count: number
    domain_count: number
    extended_total: number
    vt_count: number
  }
}

export interface SlotFieldHit {
  table_en: string
  field_name: string
  field_comment: string
  vt_id: string
  review_status: string
  selected_score: number | null
  top1_slot: string | null
  top2_slot: string | null
  top2_score: number | null
}

// ========== I-05b 槽位 proposal ==========

export interface SlotProposalMember {
  table_en: string
  field_name: string
  field_comment?: string
  vt_id: string
  top1_slot?: string | null
  top1_score?: number
}

export interface SlotProposalSimilar {
  name: string
  reason?: string
  similarity?: number
}

export interface SlotProposal {
  proposal_id: string
  source: string // cluster | llm_suggestion | merged:...
  scope: 'base' | 'domain' | 'vt_local'
  target_vt_ids: string[]
  target_domain: string | null
  name: string
  cn_name: string
  logical_type: string
  role: string
  description: string
  aliases: string[]
  sample_patterns: unknown[]
  support_count: number
  member_fields: SlotProposalMember[]
  cluster_cohesion: number | null
  llm_naming_confidence: number | null
  similar_existing_slots: SlotProposalSimilar[]
  status: 'pending' | 'accepted' | 'rejected' | 'renamed'
  created_at: string
  applied_at?: string
  renamed_to?: string
  renamed_cn_name?: string
}

export interface SlotProposalListResponse {
  meta: {
    generated_at?: string
    total?: number
    by_scope?: Record<string, number>
    by_source?: Record<string, number>
    by_status?: Record<string, number>
  }
  total_filtered: number
  proposals: SlotProposal[]
}

export interface SlotProposalApplyResponse {
  ok: boolean
  applied_to: string | null
  new_status: string
  next_action: string | null
}

export interface SlotProposalLogEntry {
  ts: string
  proposal_id: string
  decision: string
  renamed_to?: string
  renamed_cn_name?: string
  target_yaml?: string
  applied_to?: string
  reviewer_note?: string
  proposal_snapshot?: {
    name?: string
    cn_name?: string
    scope?: string
    source?: string
    support_count?: number
  }
}

export const STATUS_COLOR: Record<string, string> = {
  auto_accepted: 'green',
  needs_review: 'gold',
  low_confidence: 'red',
  conflict: 'magenta',
}

export const STATUS_LABEL: Record<string, string> = {
  auto_accepted: '自动通过',
  needs_review: '需审核',
  low_confidence: '低置信',
  conflict: '冲突',
}
