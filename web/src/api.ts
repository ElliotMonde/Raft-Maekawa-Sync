// Thin wrappers around POST /api/action and GET /api/nodes.

type ActionResult = { ok: boolean; task_id?: string; error?: string }

async function action(body: object): Promise<ActionResult> {
  const res = await fetch('/api/action', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  return res.json()
}

export const submitTask = (data: string) =>
  action({ action: 'submit_task', data })

export const submitTaskOnce = (nodeId: number, data: string) =>
  action({ action: 'submit_task_once', node_id: nodeId, data })

export const killNode = (nodeId: number) =>
  action({ action: 'kill_node', node_id: nodeId })

export const startNode = (nodeId: number, nodeAddr: string, dashAddr: string) =>
  action({ action: 'start_node', node_id: nodeId, node_addr: nodeAddr, dash_addr: dashAddr })

export const stopNode = (nodeId: number) =>
  action({ action: 'stop_node', node_id: nodeId })

export interface NodeInfo { id: number; addr: string; managed: boolean }
export interface NodesResponse {
  nodes: NodeInfo[]
  can_add_workers: boolean
}

export async function fetchNodes(): Promise<NodesResponse> {
  const res = await fetch('/api/nodes')
  return res.json()
}
