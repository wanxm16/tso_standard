/**
 * 跨页面的事件总线（简易版）—— 基于 window CustomEvent。
 * 用于：任何页面改动了 scaffold（新建/删除/编辑 VT / 分类重命名 / VT 合并）
 * 后，通知 sidebar / VT 列表等组件刷新数据。
 */

const EVENT_NAME = 'tso:scaffold-changed'

export interface ScaffoldChangedDetail {
  source: string // 来源页面：'categories' | 'vt-editor' | 'vt-merge' | 'create-vt' | ...
  action: string // 动作类型：'rename_l1' | 'rename_l2' | 'delete_vt' | 'merge' | ...
  extra?: Record<string, unknown>
}

export function emitScaffoldChanged(detail: ScaffoldChangedDetail): void {
  window.dispatchEvent(new CustomEvent<ScaffoldChangedDetail>(EVENT_NAME, { detail }))
}

export function onScaffoldChanged(listener: (detail: ScaffoldChangedDetail) => void): () => void {
  const handler = (e: Event) => {
    const ce = e as CustomEvent<ScaffoldChangedDetail>
    listener(ce.detail)
  }
  window.addEventListener(EVENT_NAME, handler)
  return () => window.removeEventListener(EVENT_NAME, handler)
}
