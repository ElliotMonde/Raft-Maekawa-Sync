import { useState } from 'react'
import { fetchNodes, killNode, startNode, submitTask, submitTaskOnce } from '../api'
import { useStore, type NodeRole, type TaskState } from '../store'

interface ScenarioDef {
  id: string
  title: string
  desc: string
  category: 'raft' | 'maekawa' | 'combined'
  run: (flash: (msg: string, err?: boolean) => void) => Promise<void>
}

function sleep(ms: number) { return new Promise(r => setTimeout(r, ms)) }

function currentLeaderId(): number | null {
  for (const node of useStore.getState().nodes.values()) {
    if (node.role === 'leader') {
      return node.id
    }
  }
  return null
}

async function waitForTaskProgress(taskIds: string[], timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const tasks = useStore.getState().tasks
    for (const taskId of taskIds) {
      const task = tasks.get(taskId)
      if (task && (task.status === 'in_progress' || task.assigned_to > 0)) {
        return true
      }
    }
    await sleep(100)
  }
  return false
}

async function waitFor<T>(check: () => T | null, timeoutMs: number, intervalMs = 100): Promise<T | null> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const result = check()
    if (result != null) {
      return result
    }
    await sleep(intervalMs)
  }
  return null
}

function currentFollowerId(excludeId?: number): number | null {
  const candidates = Array.from(useStore.getState().nodes.values())
    .filter(node => node.id >= 1 && node.id <= 3 && node.id !== excludeId && node.role !== 'leader' && node.role !== 'down')
    .sort((a, b) => a.id - b.id)
  return candidates[0]?.id ?? null
}

function taskById(taskId: string): TaskState | null {
  return useStore.getState().tasks.get(taskId) ?? null
}

async function waitForNodeRole(nodeId: number, role: NodeRole, timeoutMs: number): Promise<boolean> {
  const found = await waitFor(() => {
    const node = useStore.getState().nodes.get(nodeId)
    return node?.role === role ? true : null
  }, timeoutMs)
  return found === true
}

async function waitForTask(taskId: string, predicate: (task: TaskState) => boolean, timeoutMs: number): Promise<TaskState | null> {
  return waitFor(() => {
    const task = taskById(taskId)
    if (task && predicate(task)) {
      return task
    }
    return null
  }, timeoutMs)
}

async function getNodeAddr(nodeId: number): Promise<string | null> {
  const res = await fetchNodes()
  return res.nodes.find(node => node.id === nodeId)?.addr ?? null
}

function makeScenarios(flash: (msg: string, err?: boolean) => void): ScenarioDef[] {
  return [
    // ── RAFT ──────────────────────────────────────────────────────────────
    {
      id: 'single-task',
      title: 'Single Task',
      desc: 'Submit one task → Raft leader replicates → worker executes',
      category: 'raft',
      async run() {
        flash('Submitting task…')
        const r = await submitTask('demo-task-001')
        flash(r.ok ? `✓ Task ${r.task_id?.slice(0, 8)} queued` : `✗ ${r.error}`, !r.ok)
      },
    },
    {
      id: 'parallel-tasks',
      title: 'Parallel Tasks ×5',
      desc: 'Submit 5 tasks at once → observe Maekawa workers contending for CS',
      category: 'raft',
      async run() {
        flash('Submitting 5 parallel tasks…')
        const results = await Promise.all(
          Array.from({ length: 5 }, (_, i) => submitTask(`par-task-${i + 1}`))
        )
        const ok = results.filter(r => r.ok).length
        flash(`✓ ${ok}/5 tasks queued`)
      },
    },
    {
      id: 'burst-tasks',
      title: 'Burst Tasks ×9',
      desc: 'Stress test: 9 simultaneous tasks → all workers fight for CS',
      category: 'raft',
      async run() {
        flash('Submitting 9 tasks…')
        const results = await Promise.all(
          Array.from({ length: 9 }, (_, i) => submitTask(`burst-${i + 1}`))
        )
        const ok = results.filter(r => r.ok).length
        flash(`✓ ${ok}/9 tasks queued`)
      },
    },

    // ── RAFT ELECTION ──────────────────────────────────────────────────────
    {
      id: 'leader-failure',
      title: 'Leader Failure → Re-election',
      desc: 'Kill the current leader → watch remaining nodes elect a new one',
      category: 'raft',
      async run(flash) {
        const leaderId = currentLeaderId()
        if (leaderId == null) {
          flash('✗ No leader is elected right now', true)
          return
        }
        flash(`Killing leader N${leaderId}…`)
        const r = await killNode(leaderId)
        if (!r.ok) { flash(`✗ ${r.error}`, true); return }
        flash(`✓ N${leaderId} killed — watch the cluster elect a new leader (3–5s)`)
      },
    },
    {
      id: 'leader-failure-with-tasks',
      title: 'Leader Failure During Tasks',
      desc: 'Submit tasks then kill leader → failover + task requeue',
      category: 'combined',
      async run(flash) {
        flash('Submitting 3 tasks…')
        const results = await Promise.all([
          submitTask('failover-task-1'),
          submitTask('failover-task-2'),
          submitTask('failover-task-3'),
        ])
        const taskIds = results
          .filter((r): r is typeof r & { task_id: string } => !!r.ok && !!r.task_id)
          .map(r => r.task_id)
        if (taskIds.length === 0) {
          flash('✗ Tasks were not accepted by the cluster', true)
          return
        }
        flash('Waiting for one task to become in-flight…')
        const sawProgress = await waitForTaskProgress(taskIds, 5000)
        if (!sawProgress) {
          flash('✗ No submitted task reached in-progress state in time', true)
          return
        }

        const leaderId = currentLeaderId()
        if (leaderId == null) {
          flash('✗ No leader is elected right now', true)
          return
        }
        flash(`Killing leader N${leaderId} during active work…`)
        const r = await killNode(leaderId)
        if (!r.ok) { flash(`✗ ${r.error}`, true); return }
        flash(`✓ Leader N${leaderId} killed — watch failover and task recovery`)
      },
    },
    {
      id: 'submit-during-no-leader',
      title: 'Submit During No Leader',
      desc: 'Kill leader → show one-shot submit failure → wait for new leader → retry successfully',
      category: 'combined',
      async run(flash) {
        const leaderId = currentLeaderId()
        if (leaderId == null) {
          flash('✗ No leader is elected right now', true)
          return
        }
        const followerId = currentFollowerId(leaderId)
        if (followerId == null) {
          flash('✗ No follower is available for the no-leader submit demo', true)
          return
        }

        flash(`Killing leader N${leaderId}…`)
        const kill = await killNode(leaderId)
        if (!kill.ok) {
          flash(`✗ ${kill.error}`, true)
          return
        }

        flash(`Trying one-shot submit through follower N${followerId}…`)
        const failedSubmit = await submitTaskOnce(followerId, 'no-leader-window')
        if (failedSubmit.ok) {
          flash('✗ Unexpectedly accepted one-shot submit during failover', true)
          return
        }

        flash('✓ Submit failed honestly. Waiting for new leader…')
        const newLeader = await waitFor(() => {
          const id = currentLeaderId()
          return id != null && id !== leaderId ? id : null
        }, 8000)
        if (newLeader == null) {
          flash('✗ No new leader elected in time', true)
          return
        }

        const retried = await submitTask('after-no-leader-window')
        flash(retried.ok ? `✓ Retry succeeded on new leader N${newLeader}` : `✗ ${retried.error}`, !retried.ok)
      },
    },

    // ── MAEKAWA ────────────────────────────────────────────────────────────
    {
      id: 'two-workers-mutex',
      title: 'Mutual Exclusion: 2 Workers',
      desc: 'Submit 2 tasks → observe REQUEST/GRANT/DEFER/RELEASE flow',
      category: 'maekawa',
      async run(flash) {
        flash('Submitting 2 contending tasks…')
        await Promise.all([submitTask('mutex-a'), submitTask('mutex-b')])
        flash('✓ Watch W1 and W2 negotiate via Maekawa')
      },
    },
    {
      id: 'cs-holder-dies',
      title: 'CS Holder Dies',
      desc: 'Submit task, wait for CS, kill that worker → voters eventually release',
      category: 'maekawa',
      async run(flash) {
        flash('Submitting task for W4…')
        await submitTask('cs-holder-kill')
        await sleep(1200)
        flash('Killing W4 (node 4) while it may be in CS…')
        const r = await killNode(4)
        flash(r.ok ? '✓ W4 killed — watch voters release locks and re-elect CS' : `✗ ${r.error}`, !r.ok)
      },
    },
    {
      id: 'inquire-yield',
      title: 'INQUIRE/YIELD Deadlock Prevention',
      desc: 'Submit 5 tasks → trigger INQUIRE/YIELD protocol under contention',
      category: 'maekawa',
      async run(flash) {
        flash('Submitting 5 contending tasks…')
        await Promise.all(
          Array.from({ length: 5 }, (_, i) => submitTask(`iq-${i + 1}`))
        )
        flash('✓ Watch INQUIRE/YIELD arrows in Maekawa panel')
      },
    },
    {
      id: 'worker-heartbeat-timeout',
      title: 'Worker Heartbeat Timeout',
      desc: 'Kill a worker → wait for worker_down → show regrid → submit another task',
      category: 'maekawa',
      async run(flash) {
        const workerId = 5
        flash(`Killing W${workerId - 3}…`)
        const killed = await killNode(workerId)
        if (!killed.ok) {
          flash(`✗ ${killed.error}`, true)
          return
        }

        const sawDown = await waitForNodeRole(workerId, 'down', 6000)
        if (!sawDown) {
          flash('✗ Worker did not time out to DOWN in time', true)
          return
        }

        flash('✓ worker_down observed. Submit a follow-up task on the reduced quorum…')
        const resp = await submitTask('post-worker-timeout')
        flash(resp.ok ? `✓ ${resp.task_id?.slice(0, 8)} queued after regrid` : `✗ ${resp.error}`, !resp.ok)
      },
    },
    {
      id: 'worker-recovery',
      title: 'Worker Recovery',
      desc: 'Restart the timed-out worker → show worker_up → submit another task',
      category: 'maekawa',
      async run(flash) {
        const workerId = 5
        const addr = await getNodeAddr(workerId)
        if (!addr) {
          flash('✗ No address known for the worker', true)
          return
        }

        flash(`Restarting W${workerId - 3}…`)
        const started = await startNode(workerId, addr, window.location.host)
        if (!started.ok) {
          flash(`✗ ${started.error}`, true)
          return
        }

        const sawUp = await waitFor(() => {
          const node = useStore.getState().nodes.get(workerId)
          return node && node.role !== 'down' ? true : null
        }, 6000)
        if (sawUp == null) {
          flash('✗ Worker did not come back in time', true)
          return
        }

        flash('✓ worker_up observed. Submit a follow-up task on the restored quorum…')
        const resp = await submitTask('post-worker-recovery')
        flash(resp.ok ? `✓ ${resp.task_id?.slice(0, 8)} queued after recovery` : `✗ ${resp.error}`, !resp.ok)
      },
    },
    {
      id: 'claimed-worker-dies',
      title: 'Claimed Worker Dies -> Task Recovered',
      desc: 'Kill the worker that claimed a task → wait for timeout → show reassignment/recovery',
      category: 'maekawa',
      async run(flash) {
        flash('Submitting recovery target task…')
        const submitted = await submitTask('claimed-worker-recovery')
        if (!submitted.ok || !submitted.task_id) {
          flash(`✗ ${submitted.error ?? 'task was not accepted'}`, true)
          return
        }

        const claimed = await waitForTask(
          submitted.task_id,
          task => task.status === 'in_progress' && task.assigned_to > 0,
          8000,
        )
        if (!claimed) {
          flash('✗ Task never reached in-progress state', true)
          return
        }

        const crashedWorker = claimed.assigned_to
        flash(`Killing claiming worker W${crashedWorker - 3}…`)
        const killed = await killNode(crashedWorker)
        if (!killed.ok) {
          flash(`✗ ${killed.error}`, true)
          return
        }

        const sawDown = await waitForNodeRole(crashedWorker, 'down', 6000)
        if (!sawDown) {
          flash('✗ Crashed worker did not time out to DOWN in time', true)
          return
        }

        const recovered = await waitForTask(
          submitted.task_id,
          task => task.status === 'completed' || (task.assigned_to > 0 && task.assigned_to !== crashedWorker),
          12000,
        )
        if (!recovered) {
          flash('✗ Task was not reassigned or completed in time', true)
          return
        }

        if (recovered.status === 'completed') {
          flash('✓ Task recovered and completed after worker crash')
          return
        }
        flash(`✓ Task recovered and moved to W${recovered.assigned_to - 3}`)
      },
    },
    {
      id: 'no-starvation',
      title: 'No Starvation Test',
      desc: 'Submit 8 tasks in rapid succession → verify all eventually complete',
      category: 'maekawa',
      async run(flash) {
        flash('Submitting 8 sequential tasks…')
        for (let i = 1; i <= 8; i++) {
          await submitTask(`starve-${i}`)
          await sleep(200)
        }
        flash('✓ 8 tasks submitted — watch all complete without starvation')
      },
    },

    // ── COMBINED ───────────────────────────────────────────────────────────
    {
      id: 'full-e2e',
      title: 'Full End-to-End',
      desc: 'User → Raft leader → workers → Maekawa CS → result → leader commit',
      category: 'combined',
      async run(flash) {
        flash('Starting full E2E scenario…')
        await Promise.all([
          submitTask('e2e-render-001'),
          submitTask('e2e-render-002'),
          submitTask('e2e-render-003'),
        ])
        flash('✓ 3 E2E tasks in flight — watch Raft replication + Maekawa CS')
      },
    },
    {
      id: 'regrid',
      title: 'Regrid (Kill + Recover Worker)',
      desc: 'Kill W4 → quorum recomputed → submit tasks → revive W4 → regrid',
      category: 'combined',
      async run(flash) {
        flash('Killing worker 4 to trigger regrid…')
        const r1 = await killNode(4)
        if (!r1.ok) { flash(`✗ ${r1.error}`, true); return }
        await sleep(800)
        flash('W4 killed — submitting tasks on reduced quorum…')
        await submitTask('regrid-task-1')
        flash('✓ Quorum reduced. Start W4 to see regrid and rebalance.')
      },
    },
    {
      id: 'raft-node-restart',
      title: 'Raft Node Restart',
      desc: 'Stop a follower → keep processing tasks → restart it → watch it rejoin and catch up',
      category: 'combined',
      async run(flash) {
        const leaderId = currentLeaderId()
        const targetId = currentFollowerId(leaderId ?? undefined)
        if (targetId == null) {
          flash('✗ No Raft follower is available to restart', true)
          return
        }

        flash(`Stopping follower N${targetId}…`)
        const killed = await killNode(targetId)
        if (!killed.ok) {
          flash(`✗ ${killed.error}`, true)
          return
        }
        const sawDown = await waitForNodeRole(targetId, 'down', 4000)
        if (!sawDown) {
          flash('✗ Follower did not go down in time', true)
          return
        }

        const interim = await submitTask('during-raft-restart')
        if (!interim.ok) {
          flash(`✗ ${interim.error}`, true)
          return
        }

        const addr = await getNodeAddr(targetId)
        if (!addr) {
          flash('✗ No address known for the Raft node', true)
          return
        }

        flash(`Restarting N${targetId}…`)
        const started = await startNode(targetId, addr, window.location.host)
        if (!started.ok) {
          flash(`✗ ${started.error}`, true)
          return
        }

        const rejoined = await waitFor(() => {
          const node = useStore.getState().nodes.get(targetId)
          return node && node.role !== 'down' ? node : null
        }, 8000)
        if (!rejoined) {
          flash('✗ Restarted Raft node did not rejoin in time', true)
          return
        }

        flash(`✓ N${targetId} rejoined as ${rejoined.role}. Submit another task to show catch-up…`)
        const finalSubmit = await submitTask('after-raft-restart')
        flash(finalSubmit.ok ? `✓ ${finalSubmit.task_id?.slice(0, 8)} queued after restart` : `✗ ${finalSubmit.error}`, !finalSubmit.ok)
      },
    },
  ]
}

const CATEGORY_LABEL: Record<string, string> = {
  raft:     'Raft Cluster',
  maekawa:  'Maekawa Mutex',
  combined: 'Combined / E2E',
}

const CATEGORY_COLOR: Record<string, string> = {
  raft:     'var(--accent)',
  maekawa:  'var(--cs)',
  combined: 'var(--leader)',
}

interface Props {
  flash: (msg: string, err?: boolean) => void
}

export function ScenarioRunner({ flash }: Props) {
  const [running, setRunning] = useState<string | null>(null)
  const scenarios = makeScenarios(flash)
  const categories = ['raft', 'maekawa', 'combined'] as const

  async function runScenario(s: ScenarioDef) {
    if (running) return
    setRunning(s.id)
    try {
      await s.run(flash)
    } catch (err) {
      flash(`✗ ${String(err)}`, true)
    } finally {
      setRunning(null)
    }
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
      {categories.map(cat => (
        <div key={cat} className="scenario-section">
          <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 2 }}>
            <span style={{ width: 6, height: 6, borderRadius: '50%', background: CATEGORY_COLOR[cat], flexShrink: 0 }} />
            <span className="section-label" style={{ color: CATEGORY_COLOR[cat] }}>{CATEGORY_LABEL[cat]}</span>
          </div>
          {scenarios.filter(s => s.category === cat).map(s => (
            <button
              key={s.id}
              className="scenario"
              disabled={running !== null}
              onClick={() => runScenario(s)}
              style={{ opacity: running && running !== s.id ? 0.5 : 1 }}
            >
              <span className="scenario-title">
                {running === s.id ? '⟳ ' : ''}{s.title}
              </span>
              <span className="scenario-desc">{s.desc}</span>
            </button>
          ))}
        </div>
      ))}
    </div>
  )
}
