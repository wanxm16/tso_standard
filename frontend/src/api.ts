import axios from 'axios'
import type {
  BaseSlot,
  FieldMapping,
  NewSlotCandidate,
  NormalizationItem,
  NormalizationStats,
  Slot,
  SlotFieldHit,
  SlotLibraryResponse,
  SlotProposalApplyResponse,
  SlotProposalListResponse,
  SlotProposalLogEntry,
  Stats,
  VTDetail,
  VTSummary,
} from './types'

const client = axios.create({
  baseURL: 'http://127.0.0.1:8001',
  timeout: 30000,
})

export const api = {
  stats: () => client.get<Stats>('/api/stats').then((r) => r.data),
  listVTs: () => client.get<VTSummary[]>('/api/virtual-tables').then((r) => r.data),
  getVT: (vt_id: string) =>
    client.get<VTDetail>(`/api/virtual-tables/${vt_id}`).then((r) => r.data),
  getBaseSlots: () => client.get<BaseSlot[]>('/api/base-slots').then((r) => r.data),
  updateSlots: (
    vt_id: string,
    slots: Slot[],
    summary?: string,
    seed_mappings?: Array<{ table_en: string; field_name: string; slot_name: string }>,
  ) =>
    client
      .put(`/api/virtual-tables/${vt_id}/slots`, { slots, summary, seed_mappings })
      .then((r) => r.data),
  reviewLog: (limit = 50) =>
    client.get(`/api/review-log?limit=${limit}`).then((r) => r.data),

  // ========== I-05 归一审核 ==========
  normStats: () => client.get<NormalizationStats>('/api/normalization/stats').then((r) => r.data),

  normList: (params: {
    status?: string
    vt_id?: string
    slot?: string
    keyword?: string
    only_unreviewed?: boolean
    limit?: number
    offset?: number
  }) =>
    client
      .get<{ total: number; items: NormalizationItem[] }>('/api/normalization', {
        params,
      })
      .then((r) => r.data),

  normDecision: (payload: {
    table_en: string
    field_name: string
    vt_id: string
    decision: string
    selected_slot?: string
    new_slot_name?: string
    new_slot_cn_name?: string
    reviewer_note?: string
  }) =>
    client
      .post<{ ok: boolean; decision_slot: string | null; reviewed_count: number }>(
        '/api/normalization/decision',
        payload,
      )
      .then((r) => r.data),

  promoteExtendedToBase: (payload: {
    name: string
    cn_name: string
    role: string
    logical_type?: string
    description?: string
    aliases?: string[]
    applicable_table_types?: string[]
  }) =>
    client.post<{ ok: boolean; name: string; base_count: number }>(
      '/api/base-slots/promote-from-extended',
      payload,
    ).then((r) => r.data),

  undoNormReviewed: (payload: { table_en: string; field_name: string; vt_id: string }) =>
    client
      .delete<{ ok: boolean; removed: number; remaining: number }>(
        '/api/normalization/reviewed',
        { data: payload },
      )
      .then((r) => r.data),

  llmSuggestSlotName: (payload: {
    table_en?: string
    field_name?: string
    vt_id?: string
    user_cn_name?: string
  }) =>
    client
      .post<{
        name: string
        cn_name: string
        aliases: string[]
        reason: string
        duplicate_of_existing: boolean
      }>('/api/normalization/llm-suggest-slot-name', payload, { timeout: 60_000 })
      .then((r) => r.data),

  newSlotCandidates: (limit = 50) =>
    client
      .get<NewSlotCandidate[]>(`/api/normalization/new-slot-candidates?limit=${limit}`)
      .then((r) => r.data),

  fieldMapping: (vt_id: string, table_en?: string) =>
    client
      .get<FieldMapping[]>('/api/normalization/field-mapping', {
        params: { vt_id, table_en },
      })
      .then((r) => r.data),

  // ========== I-05b 新槽位 proposals ==========
  slotProposals: (params?: { scope?: string; status?: string; source?: string; limit?: number }) =>
    client
      .get<SlotProposalListResponse>('/api/slots/proposals', { params })
      .then((r) => r.data),

  applySlotProposal: (payload: {
    proposal_id: string
    decision: 'accept' | 'reject' | 'rename'
    renamed_to?: string
    renamed_cn_name?: string
    target_yaml?: string
    reviewer_note?: string
  }) =>
    client
      .post<SlotProposalApplyResponse>('/api/slots/proposals/apply', payload)
      .then((r) => r.data),

  slotProposalsLog: (limit = 100) =>
    client
      .get<SlotProposalLogEntry[]>('/api/slots/proposals/log', { params: { limit } })
      .then((r) => r.data),

  // ========== 字段黑名单 ==========
  fieldBlacklist: () =>
    client.get('/api/fields/blacklist').then((r) => r.data),

  addFieldBlacklist: (payload: {
    mode: 'exact_name' | 'pattern' | 'pair'
    value?: string
    table_en?: string
    field_name?: string
    reason?: string
  }) =>
    client
      .post<{ ok: boolean; added: boolean; next_action: string | null }>(
        '/api/fields/blacklist',
        payload,
      )
      .then((r) => r.data),

  removeFieldBlacklist: (payload: {
    mode: 'exact_name' | 'pattern' | 'pair'
    value?: string
    table_en?: string
    field_name?: string
  }) =>
    client
      .request<{ ok: boolean; removed: boolean }>({
        method: 'DELETE',
        url: '/api/fields/blacklist',
        data: payload,
      })
      .then((r) => r.data),

  // 扫描 VT 源表字段，返回匹配黑名单规则的候选
  blacklistScanForVT: (vt_id: string) =>
    client
      .post<{
        ok: boolean
        vt_id: string
        total_scanned: number
        matched: number
        candidates: Array<{
          table_en: string
          field_name: string
          field_comment: string
          sample: string
          data_type: string
          match_reason: string
          match_type: string
          already_in_exact: boolean
        }>
      }>(`/api/virtual-tables/${vt_id}/blacklist-scan`)
      .then((r) => r.data),

  // 批量加入 exact_names；已在的自动跳过
  batchAddFieldBlacklist: (field_names: string[], reason?: string) =>
    client
      .post<{
        ok: boolean
        added_count: number
        skipped_count: number
        added: string[]
        skipped: string[]
      }>('/api/fields/blacklist/batch', { field_names, reason })
      .then((r) => r.data),

  // ========== I-12 槽位库 ==========
  slotLibrary: () =>
    client.get<SlotLibraryResponse>('/api/slot-library').then((r) => r.data),

  slotLibraryFields: (name: string, limit = 30) =>
    client
      .get<SlotFieldHit[]>(`/api/slot-library/base/${encodeURIComponent(name)}/fields`, {
        params: { limit },
      })
      .then((r) => r.data),

  createBaseSlot: (payload: {
    name: string
    cn_name: string
    role: string
    logical_type?: string
    description?: string
    aliases?: string[]
    applicable_table_types?: string[]
  }) =>
    client
      .post<{ ok: boolean; name: string; next_action: string | null }>(
        '/api/slot-library/base',
        payload,
      )
      .then((r) => r.data),

  suggestBaseSlot: (payload: { cn_name: string; cn_aliases: string[]; role: string }) =>
    client
      .post<{
        name: string
        description: string
        aliases: string[]
        logical_type: string
        warnings: string[]
      }>('/api/slot-library/base/suggest', payload)
      .then((r) => r.data),

  deleteBaseSlot: (name: string) =>
    client
      .delete<{ ok: boolean; deleted: string; next_action: string | null }>(
        `/api/slot-library/base/${encodeURIComponent(name)}`,
      )
      .then((r) => r.data),

  editBaseSlot: (name: string, payload: { aliases_add?: string[]; aliases_remove?: string[] }) =>
    client
      .put<{ ok: boolean; added: string[]; removed: string[]; next_action: string | null }>(
        `/api/slot-library/base/${encodeURIComponent(name)}`,
        payload,
      )
      .then((r) => r.data),

  // ========== I-13 VT 合并 ==========
  vtMergeCandidates: (status?: string) =>
    client.get('/api/vt-merge/candidates', { params: { status } }).then((r) => r.data),

  applyVTMerge: (payload: {
    group_id: string
    primary_vt_id: string
    absorbed_vt_ids: string[]
    reviewer_note?: string
  }) => client.post('/api/vt-merge/apply', payload).then((r) => r.data),

  rejectVTMerge: (payload: { group_id: string; reviewer_note?: string }) =>
    client.post('/api/vt-merge/reject', payload).then((r) => r.data),

  // ========== I-14 scaffold 全编辑 ==========
  listAllTables: () =>
    client
      .get<Array<{ en: string; cn: string; field_count: number; in_scaffold?: boolean }>>('/api/tables')
      .then((r) => r.data),

  updateVTMeta: (
    vt_id: string,
    payload: {
      topic?: string
      grain_desc?: string
      table_type?: string
      l2_path?: string[]
      candidate_tables?: Array<{ en: string; cn?: string }>
      reason?: string
      clear_review_hint?: boolean
    },
  ) =>
    client
      .put<{ ok: boolean; vt_id: string; next_action: string | null }>(
        `/api/virtual-tables/${encodeURIComponent(vt_id)}/meta`,
        payload,
      )
      .then((r) => r.data),

  createVT: (payload: {
    topic: string
    grain_desc?: string
    table_type?: string
    l2_path: string[]
    candidate_tables?: Array<{ en: string; cn?: string }>
    reason?: string
  }) =>
    client
      .post<{ ok: boolean; vt_id: string; new_vt_total: number; next_action: string | null }>(
        '/api/virtual-tables',
        payload,
      )
      .then((r) => r.data),

  deleteVT: (vt_id: string) =>
    client
      .delete<{ ok: boolean; deleted_vt_id: string; new_vt_total: number; next_action: string | null }>(
        `/api/virtual-tables/${encodeURIComponent(vt_id)}`,
      )
      .then((r) => r.data),

  // ========== I-15 分类树 ==========
  getCategories: () =>
    client.get<{
      categories: Array<{
        name: string
        vt_count: number
        in_tree: boolean
        children: Array<{ name: string; vt_count: number; in_tree: boolean }>
      }>
    }>('/api/categories').then((r) => r.data),

  addCategoryL1: (name: string) =>
    client.post('/api/categories/l1', { name }).then((r) => r.data),

  addCategoryL2: (l1_name: string, name: string) =>
    client.post('/api/categories/l2', { l1_name, name }).then((r) => r.data),

  renameCategoryL1: (old_name: string, new_name: string) =>
    client.put('/api/categories/l1/rename', { old_name, new_name }).then((r) => r.data),

  renameCategoryL2: (l1_name: string, old_name: string, new_name: string) =>
    client.put('/api/categories/l2/rename', { l1_name, old_name, new_name }).then((r) => r.data),

  deleteCategoryL1: (name: string) =>
    client.delete(`/api/categories/l1/${encodeURIComponent(name)}`).then((r) => r.data),

  deleteCategoryL2: (l1: string, l2: string) =>
    client.delete(`/api/categories/l2/${encodeURIComponent(l1)}/${encodeURIComponent(l2)}`).then((r) => r.data),

  getTableFields: (table_en: string, limit = 200) =>
    client
      .get<{
        table_en: string
        field_count: number
        fields: Array<{ field: string; type: string; comment: string; sample_data: string }>
      }>(`/api/tables/${encodeURIComponent(table_en)}/fields`, { params: { limit } })
      .then((r) => r.data),

  // ========== I-16 Imp3: L2 批量 LLM 生成槽位 ==========
  generateSlotsForL2: (
    l1: string,
    l2: string,
    payload: { only_missing?: boolean; include_empty_source?: boolean } = { only_missing: true },
  ) =>
    client
      .post<{
        ok: boolean
        total?: number
        message?: string
        results: Array<{
          vt_id: string
          topic: string
          ok: boolean
          slots?: any[]
          summary?: string
          warnings?: string[]
          error?: string
          elapsed_sec: number
        }>
      }>(
        `/api/categories/${encodeURIComponent(l1)}/${encodeURIComponent(l2)}/generate-slots`,
        payload,
        { timeout: 600_000 },  // 批量可能跑很久（N 张 VT × ~10s）
      )
      .then((r) => r.data),

  // ========== I-16 Gap3: 单 VT LLM 生成槽位 ==========
  generateSlotsForVT: (vt_id: string) =>
    client
      .post<{
        ok: boolean
        vt_id: string
        slots: Array<{
          name: string
          from: 'base' | 'extended'
          role: string
          cn_name?: string
          logical_type?: string
          aliases?: string[]
          applicable_table_types?: string[]
          llm_reason?: string
        }>
        summary: string
        warnings: string[]
        elapsed_sec: number
      }>(
        `/api/virtual-tables/${encodeURIComponent(vt_id)}/generate-slots`,
        undefined,
        { timeout: 120_000 },  // LLM 调用冷缓存可能 30-60s
      )
      .then((r) => r.data),

  // ========== Pipeline 重跑（异步）==========
  startPipelineJob: (from_step: string) =>
    client
      .post<{ ok: boolean; job_id: string; from_step: string }>('/api/pipeline/jobs', { from_step })
      .then((r) => r.data),

  rerunVTNormalization: (vt_id: string) =>
    client
      .post<{ ok: boolean; job_id: string; vt_id: string }>(
        `/api/virtual-tables/${encodeURIComponent(vt_id)}/rerun-normalization`,
      )
      .then((r) => r.data),

  getPipelineJob: (job_id: string) =>
    client
      .get<{
        job_id: string
        from_step: string
        state: 'running' | 'done' | 'failed'
        started_at: string
        ended_at?: string
        current_step: string
        log_tail: string[]
        return_code: number | null
        error?: string
      }>(`/api/pipeline/jobs/${encodeURIComponent(job_id)}`)
      .then((r) => r.data),

  listPipelineJobs: () =>
    client.get<Array<any>>('/api/pipeline/jobs').then((r) => r.data),

  pipelineDirtyCheck: () =>
    client
      .get<{ dirty: boolean; dirty_sources: string[]; from_step: string }>(
        '/api/pipeline/dirty-check',
      )
      .then((r) => r.data),

  extendSlotsForVT: (vt_id: string, opts?: { include_unconfirmed?: boolean }) =>
    client
      .post<{
        ok: boolean
        vt_id: string
        new_slots: Array<{
          name: string
          source: 'base' | 'extended'
          from: 'base' | 'extended'
          role: string
          cn_name: string
          logical_type: string
          aliases: string[]
          applicable_table_types: string[]
          covers_fields: string[]
          covers_mapped_fields: Array<{ table_en: string; field_name: string; field_comment: string }>
          llm_reason: string
        }>
        skipped_reason: Record<string, string>
        summary: string
        warnings: string[]
        total_fields: number
        covered_count: number
        uncovered_count: number
        uncovered_sample_shown?: number
        skipped_confirmed?: number
        skipped_auto_mapped?: number
        included_unconfirmed?: number
        include_unconfirmed_mode?: boolean
        elapsed_sec: number
      }>(
        `/api/virtual-tables/${encodeURIComponent(vt_id)}/extend-slots`,
        { include_unconfirmed: opts?.include_unconfirmed ?? true },
        { timeout: 120_000 },
      )
      .then((r) => r.data),

  // ========== W5-0 ==========

  getNamingDiagnosis: () =>
    client.get<any>('/api/naming/diagnosis').then((r) => r.data),

  regenerateNamingDiagnosis: () =>
    client.post<any>('/api/naming/diagnosis/regenerate', undefined, { timeout: 120_000 }).then((r) => r.data),

  getHomonymProposals: () =>
    client.get<any>('/api/naming/homonyms').then((r) => r.data),

  regenerateHomonyms: () =>
    client.post<any>('/api/naming/homonyms/regenerate', undefined, { timeout: 300_000 }).then((r) => r.data),

  applyHomonym: (proposal_name: string, reason?: string, overrides?: any[]) =>
    client
      .post<any>('/api/naming/homonyms/apply', { proposal_name, reason, overrides }, { timeout: 60_000 })
      .then((r) => r.data),

  getAlignmentLog: (limit = 100) =>
    client.get<any>(`/api/alignment/log?limit=${limit}`).then((r) => r.data),

  revertAlignment: (target_version: number) =>
    client.post<any>('/api/alignment/revert', { target_version }).then((r) => r.data),

  getBenchmarkAttribution: (only_failed = true, limit = 500) =>
    client.get<any>(`/api/benchmark/attribution?only_failed=${only_failed}&limit=${limit}`).then((r) => r.data),

  regenerateBenchmarkAttribution: () =>
    client.post<any>('/api/benchmark/attribution/regenerate', undefined, { timeout: 300_000 }).then((r) => r.data),

  getL2Alignment: () =>
    client.get<any>('/api/alignment/l2').then((r) => r.data),

  regenerateL2Alignment: (only_l2?: string, threshold = 0.18) =>
    client.post<any>('/api/alignment/l2/regenerate', { only_l2, threshold }, { timeout: 600_000 }).then((r) => r.data),

  applyL2Cluster: (payload: { l1: string; l2: string; cluster_id: number; rename_plan_override?: any[]; reason?: string }) =>
    client.post<any>('/api/alignment/l2/apply', payload, { timeout: 60_000 }).then((r) => r.data),

  getL1Alignment: () =>
    client.get<any>('/api/alignment/l1').then((r) => r.data),

  regenerateL1Alignment: (only_l1?: string, threshold = 0.18) =>
    client.post<any>('/api/alignment/l1/regenerate', { only_l1, threshold }, { timeout: 900_000 }).then((r) => r.data),

  applyL1Cluster: (payload: { l1: string; cluster_id: number; rename_plan_override?: any[]; reason?: string }) =>
    client.post<any>('/api/alignment/l1/apply', payload, { timeout: 60_000 }).then((r) => r.data),

  getBasePromotion: () =>
    client.get<any>('/api/alignment/base').then((r) => r.data),

  regenerateBasePromotion: (payload: { min_l1?: number; only_name?: string; no_llm?: boolean }) =>
    client.post<any>('/api/alignment/base/regenerate', payload, { timeout: 900_000 }).then((r) => r.data),

  applyBasePromotion: (payload: {
    canonical_name: string
    base_entry_override?: any
    member_vt_ids?: string[]
    reason?: string
  }) =>
    client.post<any>('/api/alignment/base/apply', payload, { timeout: 60_000 }).then((r) => r.data),

  getBenchmarkMetrics: () =>
    client.get<any>('/api/benchmark/metrics').then((r) => r.data),

  getBenchmarkChannelTopk: (topk = 5) =>
    client.get<any>('/api/benchmark/channel-topk', { params: { topk } }).then((r) => r.data),

  getBenchmarkAttributionByChannel: (
    channel: string,
    mode: 'fail' | 'hit' | 'all' = 'fail',
    limit = 500,
  ) =>
    client
      .get<any>('/api/benchmark/attribution', {
        params: { channel, mode, limit },
      })
      .then((r) => r.data),

  getTechFieldCandidates: () =>
    client.get<any>('/api/blacklist/auto-detect').then((r) => r.data),

  regenerateTechFieldCandidates: (payload: { no_llm?: boolean; min_score?: number; llm_low?: number; llm_high?: number }) =>
    client.post<any>('/api/blacklist/auto-detect/regenerate', payload, { timeout: 1_800_000 }).then((r) => r.data),

  applyTechFieldBlacklist: (payload: {
    items: Array<{ candidate_id: string; action: string; value: string; reason?: string }>
  }) =>
    client.post<any>('/api/blacklist/auto-detect/apply', payload, { timeout: 60_000 }).then((r) => r.data),

  getCurrentBlacklist: (limit = 2000) =>
    client.get<any>(`/api/blacklist/current?limit=${limit}`).then((r) => r.data),

  toggleBlacklistWhitelist: (payload: { table_en: string; field_name: string; reason?: string; remove?: boolean }) =>
    client.post<any>('/api/blacklist/whitelist', payload).then((r) => r.data),
}
