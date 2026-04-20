import { useRef, useEffect, useState } from 'react'
import { motion } from 'framer-motion'
import { useStore, workerLabel, type NodeState, type TaskState } from '../store'
import { MessageArrow } from './MessageArrow'
import { useNodePositions } from '../NodePositionContext'

const ROLE_COLOR: Record<string, string> = {
  leader:    'var(--leader)',
  candidate: 'var(--candidate)',
  follower:  '#4a5568',
  down:      'var(--border)',
}

const ROLE_BG: Record<string, string> = {
  leader:    '#1a1200',
  candidate: '#1a0e00',
  follower:  'var(--bg2)',
  down:      'var(--bg3)',
}

const NODE_R = 44

interface Point { x: number; y: number }

function circlePositions(n: number, cx: number, cy: number, r: number): Point[] {
  return Array.from({ length: n }, (_, i) => {
    const angle = (2 * Math.PI * i) / n - Math.PI / 2
    return { x: cx + r * Math.cos(angle), y: cy + r * Math.sin(angle) }
  })
}

function RaftNodeCircle({ node, pos, onKill, registerRef }: {
  node: NodeState; pos: Point; onKill: (id: number) => void
  registerRef: (el: SVGCircleElement | null) => void
}) {
  const isLeader    = node.role === 'leader'
  const isCandidate = node.role === 'candidate'
  const isDown      = node.role === 'down'
  const color       = ROLE_COLOR[node.role] ?? 'var(--border)'
  const bg          = ROLE_BG[node.role] ?? 'var(--bg2)'

  return (
    <motion.g animate={{ opacity: isDown ? 0.4 : 1 }} transition={{ duration: 0.3 }}>
      {/* Pulsing leader halo */}
      {isLeader && (
        <motion.circle
          cx={pos.x} cy={pos.y} r={NODE_R + 10}
          fill="none" stroke="var(--leader)" strokeWidth={1.5}
          animate={{ opacity: [0.3, 0.9, 0.3], r: [NODE_R + 8, NODE_R + 13, NODE_R + 8] }}
          transition={{ duration: 1.5, repeat: Infinity, ease: 'easeInOut' }}
        />
      )}

      {/* Candidate spin ring */}
      {isCandidate && (
        <motion.circle
          cx={pos.x} cy={pos.y} r={NODE_R + 7}
          fill="none" stroke="var(--candidate)" strokeWidth={1}
          strokeDasharray="6 4"
          animate={{ rotate: 360 }}
          style={{ originX: `${pos.x}px`, originY: `${pos.y}px` }}
          transition={{ duration: 2, repeat: Infinity, ease: 'linear' }}
        />
      )}

      {/* Main circle — ref registered for cross-panel overlay */}
      <motion.circle
        ref={registerRef}
        cx={pos.x} cy={pos.y} r={NODE_R}
        fill={bg}
        stroke={color}
        strokeWidth={isLeader ? 2.5 : 1.5}
        animate={{ fill: bg, stroke: color }}
        transition={{ duration: 0.3 }}
      />

      {/* Node ID */}
      <text x={pos.x} y={pos.y - 13} textAnchor="middle" fontSize={17} fontWeight={800}
        fill={isDown ? 'var(--text-dim)' : color}>
        N{node.id}
      </text>

      {/* Role */}
      <text x={pos.x} y={pos.y + 4} textAnchor="middle" fontSize={9} fontWeight={700}
        fill={color} letterSpacing="0.06em">
        {node.role.toUpperCase()}
      </text>

      {/* Term */}
      <text x={pos.x} y={pos.y + 17} textAnchor="middle" fontSize={9} fill="var(--text-dim)">
        term {node.term}
      </text>

      {/* voted_for */}
      {!isDown && node.voted_for > 0 && (
        <text x={pos.x} y={pos.y + 28} textAnchor="middle" fontSize={8} fill="var(--candidate)">
          voted→N{node.voted_for}
        </text>
      )}

      {/* Kill/Down button */}
      {!isDown ? (
        <foreignObject x={pos.x - 24} y={pos.y + NODE_R + 4} width={48} height={18}>
          <button
            onClick={() => onKill(node.id)}
            style={{
              width: '100%', height: '100%', fontSize: 9, cursor: 'pointer',
              background: 'transparent', color: 'var(--danger)',
              border: '1px solid var(--danger)', borderRadius: 4, padding: 0,
            }}
          >
            kill
          </button>
        </foreignObject>
      ) : (
        <text x={pos.x} y={pos.y + NODE_R + 16} textAnchor="middle" fontSize={9} fill="var(--danger)">
          DOWN
        </text>
      )}
    </motion.g>
  )
}

function LogStrip({ node, x, y, maxWidth }: { node: NodeState; x: number; y: number; maxWidth: number }) {
  const len = Math.min(node.log_length, Math.floor(maxWidth / 14))
  if (len === 0) return null
  return (
    <g>
      <text x={x} y={y - 2} fontSize={8} fill="var(--text-dim)">log</text>
      {Array.from({ length: len }, (_, i) => {
        const idx = i + 1
        const committed = idx <= node.commit_index
        return (
          <motion.rect
            key={idx}
            x={x + i * 13}
            y={y + 4}
            width={11} height={8} rx={1.5}
            fill={committed ? 'var(--cs)' : 'var(--waiting)'}
            initial={{ scaleY: 0 }}
            animate={{ scaleY: 1 }}
            style={{ originY: `${y + 8}px` }}
            transition={{ type: 'spring', stiffness: 400, damping: 20, delay: i * 0.02 }}
          />
        )
      })}
    </g>
  )
}

function TaskRibbon({ tasks, x, y, width }: { tasks: TaskState[]; x: number; y: number; width: number }) {
  if (tasks.length === 0) return null
  const visible = tasks.slice(-Math.floor(width / 96))
  const STATUS_COLOR: Record<string, string> = {
    pending:     'var(--text-dim)',
    in_progress: 'var(--waiting)',
    completed:   'var(--cs)',
    failed:      'var(--danger)',
  }
  return (
    <g>
      <text x={x} y={y + 10} fontSize={9} fill="var(--text-dim)" fontWeight={600} letterSpacing="0.06em">TASKS</text>
      {visible.map((t, i) => {
        const tx = x + i * 94
        const color = STATUS_COLOR[t.status] ?? 'var(--text-dim)'
        return (
          <g key={t.id} transform={`translate(${tx}, ${y + 16})`}>
            <rect width={88} height={24} rx={4}
              fill={t.status === 'completed' ? '#0f2d1a' : t.status === 'failed' ? '#1a0a0a' : t.status === 'in_progress' ? '#0d1f3c' : 'var(--bg3)'}
              stroke={color} strokeWidth={0.75}
            />
            <text x={6} y={10} fontSize={8} fill="var(--text-dim)">{t.id.slice(0, 10)}</text>
            <text x={6} y={20} fontSize={9} fontWeight={700} fill={color}>
              {t.status.replace('_', ' ').toUpperCase()}{t.assigned_to > 0 ? ` →${workerLabel(t.assigned_to)}` : ''}
            </text>
          </g>
        )
      })}
    </g>
  )
}

export function RaftView({ onKill }: { onKill: (id: number) => void }) {
  const nodes  = useStore(s => s.nodes)
  const tasks  = useStore(s => s.tasks)
  const flying = useStore(s => s.flying)
  const svgRef = useRef<SVGSVGElement>(null)
  const [dims, setDims] = useState({ w: 600, h: 480 })
  const { register } = useNodePositions()

  useEffect(() => {
    const obs = new ResizeObserver(entries => {
      const e = entries[0].contentRect
      setDims({ w: e.width, h: e.height })
    })
    if (svgRef.current) obs.observe(svgRef.current)
    return () => obs.disconnect()
  }, [])

  const nodeList  = Array.from(nodes.values()).filter(n => n.id <= 3)
  const cx = dims.w / 2
  const cy = dims.h * 0.42
  const r  = Math.min(dims.w * 0.28, dims.h * 0.30, 150)
  const positions = circlePositions(nodeList.length, cx, cy, r)
  const posMap    = new Map(nodeList.map((n, i) => [n.id, positions[i]]))
  const taskList  = Array.from(tasks.values())

  // Raft-only flying messages — exclude cross-panel (those go to CrossPanelOverlay)
  const raftFlying = flying.filter(m => m.from >= 1 && m.from <= 3 && m.to >= 1 && m.to <= 3)

  const taskY = dims.h - 60

  return (
    <svg ref={svgRef} width="100%" height="100%" style={{ display: 'block' }}>

      {/* Connection lines between nodes */}
      {nodeList.map((a, i) =>
        nodeList.slice(i + 1).map(b => {
          const pa = positions[i]
          const pb = positions[nodeList.indexOf(b)]
          return (
            <line key={`${a.id}-${b.id}`}
              x1={pa.x} y1={pa.y} x2={pb.x} y2={pb.y}
              stroke="var(--border)" strokeWidth={1} opacity={0.4}
            />
          )
        })
      )}

      {/* Nodes */}
      {nodeList.map((node, i) => {
        const pos = positions[i]
        return (
          <g key={node.id}>
            <RaftNodeCircle
              node={node} pos={pos} onKill={onKill}
              registerRef={(el) => register(node.id, el)}
            />
            <LogStrip node={node} x={pos.x - 44} y={pos.y + NODE_R + 24} maxWidth={88} />
          </g>
        )
      })}

      {/* Task ribbon at bottom */}
      <TaskRibbon tasks={taskList} x={8} y={taskY} width={dims.w - 16} />

      {/* Flying messages — Raft to Raft */}
      {raftFlying.map(msg => {
        const from = posMap.get(msg.from)
        const to   = posMap.get(msg.to)
        if (!from || !to) return null
        return <MessageArrow key={msg.id} msg={msg} from={from} to={to} />
      })}

      {/* Legend */}
      <g transform={`translate(8, ${dims.h - 18})`}>
        {[
          { color: 'var(--leader)',    label: 'Leader' },
          { color: 'var(--candidate)', label: 'Candidate' },
          { color: '#4a5568',          label: 'Follower' },
          { color: 'var(--cs)',        label: 'Committed' },
          { color: 'var(--waiting)',   label: 'Pending' },
        ].map(({ color, label }, i) => (
          <g key={label} transform={`translate(${i * 96}, 0)`}>
            <circle cx={5} cy={0} r={4} fill={color} />
            <text x={12} y={4} fontSize={9} fill="var(--text-dim)">{label}</text>
          </g>
        ))}
      </g>
    </svg>
  )
}
