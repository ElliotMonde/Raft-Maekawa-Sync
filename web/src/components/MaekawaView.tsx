import React, { useRef, useEffect, useState, useMemo } from 'react'
import { motion } from 'framer-motion'
import { useStore, type NodeState, computeQuorum } from '../store'
import { MessageArrow } from './MessageArrow'
import { useNodePositions } from '../NodePositionContext'

const NODE_R = 42

interface Point { x: number; y: number }

// Arrange n workers in a sqrt(n)×sqrt(n) grid
function gridPositions(n: number, cx: number, cy: number, spacing: number): Point[] {
  const cols = Math.ceil(Math.sqrt(n))
  const rows = Math.ceil(n / cols)
  const startX = cx - ((cols - 1) * spacing) / 2
  const startY = cy - ((rows - 1) * spacing) / 2
  return Array.from({ length: n }, (_, i) => ({
    x: startX + (i % cols) * spacing,
    y: startY + Math.floor(i / cols) * spacing,
  }))
}

// Draw dashed grid lines (row and column separators) for quorum visualization
function QuorumGridLines({ positions, cols, color }: { positions: Point[]; cols: number; color: string }) {
  if (positions.length < 4) return null
  const rows = Math.ceil(positions.length / cols)
  const lines: React.ReactElement[] = []

  // Horizontal row separators
  for (let r = 0; r < rows - 1; r++) {
    const p1 = positions[r * cols]
    const pEnd = positions[Math.min((r + 1) * cols - 1, positions.length - 1)]
    const y = (positions[r * cols].y + positions[(r + 1) * cols]?.y) / 2
    if (!p1 || isNaN(y)) continue
    lines.push(
      <line key={`h${r}`}
        x1={p1.x - NODE_R} y1={y} x2={pEnd.x + NODE_R} y2={y}
        stroke={color} strokeWidth={0.5} strokeDasharray="4 6" opacity={0.25}
      />
    )
  }

  // Vertical column separators
  for (let c = 0; c < cols - 1; c++) {
    const p1 = positions[c]
    const pEnd = positions[Math.min((rows - 1) * cols + c, positions.length - 1)]
    if (!p1 || !pEnd) continue
    const x = (positions[c].x + positions[c + 1].x) / 2
    lines.push(
      <line key={`v${c}`}
        x1={x} y1={p1.y - NODE_R} x2={x} y2={pEnd.y + NODE_R}
        stroke={color} strokeWidth={0.5} strokeDasharray="4 6" opacity={0.25}
      />
    )
  }

  return <g>{lines}</g>
}

function WorkerNodeCircle({
  node, pos, inQuorum, isVoterFor, waiting, onKill, registerRef,
}: {
  node: NodeState
  pos: Point
  inQuorum: boolean
  isVoterFor: number
  waiting: boolean
  onKill: (id: number) => void
  registerRef: (el: SVGCircleElement | null) => void
}) {
  const isDown = node.role === 'down'
  const isCS   = node.in_cs

  const strokeCol = isDown ? 'var(--border)' : isCS ? 'var(--cs)' : inQuorum ? 'var(--accent)' : '#4a5568'
  const fillCol   = isDown ? 'var(--bg3)' : isCS ? '#0f2d1a' : waiting ? '#0d1f3c' : 'var(--bg2)'
  const textCol   = isDown ? 'var(--text-dim)' : isCS ? 'var(--cs)' : waiting ? 'var(--waiting)' : 'var(--text)'

  const workerLabel = node.id >= 4 ? `W${node.id - 3}` : `W${node.id}`

  return (
    <motion.g animate={{ opacity: isDown ? 0.35 : 1 }} transition={{ duration: 0.3 }}>
      {/* Quorum dashed ring — node is in the CS holder's quorum */}
      {inQuorum && !isDown && (
        <motion.circle
          cx={pos.x} cy={pos.y} r={NODE_R + 10}
          fill="none" stroke="var(--accent)" strokeWidth={1.5} strokeDasharray="5 4"
          animate={{ opacity: [0.5, 1, 0.5] }}
          transition={{ duration: 1.2, repeat: Infinity, ease: 'easeInOut' }}
        />
      )}

      {/* CS green glow */}
      {isCS && (
        <motion.circle
          cx={pos.x} cy={pos.y} r={NODE_R + 7}
          fill="none" stroke="var(--cs)" strokeWidth={3}
          animate={{ opacity: [0.4, 1, 0.4], r: [NODE_R + 5, NODE_R + 9, NODE_R + 5] }}
          transition={{ duration: 0.9, repeat: Infinity, ease: 'easeInOut' }}
        />
      )}

      {/* Waiting pulse ring */}
      {waiting && !isCS && !isDown && (
        <motion.circle
          cx={pos.x} cy={pos.y} r={NODE_R + 4}
          fill="none" stroke="var(--waiting)" strokeWidth={1}
          animate={{ opacity: [0.2, 0.6, 0.2] }}
          transition={{ duration: 1.2, repeat: Infinity }}
        />
      )}

      {/* Main body — ref registered for cross-panel overlay */}
      <motion.circle
        ref={registerRef}
        cx={pos.x} cy={pos.y} r={NODE_R}
        fill={fillCol} stroke={strokeCol} strokeWidth={isCS ? 2.5 : 1.5}
        animate={{ fill: fillCol, stroke: strokeCol }}
        transition={{ duration: 0.3 }}
      />

      {/* Worker ID */}
      <text x={pos.x} y={pos.y - 12} textAnchor="middle" fontSize={15} fontWeight={800} fill={textCol}>
        {workerLabel}
      </text>

      {/* State label */}
      <text x={pos.x} y={pos.y + 3} textAnchor="middle" fontSize={9} fontWeight={700} fill={textCol} letterSpacing="0.06em">
        {isDown ? 'DOWN' : isCS ? 'IN CS' : waiting ? 'WAITING' : 'IDLE'}
      </text>

      {/* Lamport clock */}
      <text x={pos.x} y={pos.y + 16} textAnchor="middle" fontSize={9} fill="var(--text-dim)">
        t={node.clock}
      </text>

      {/* Voted-for indicator */}
      {isVoterFor > 0 && !isDown && (
        <text x={pos.x} y={pos.y + 27} textAnchor="middle" fontSize={8} fill="var(--cs)">
          ✓ voted W{isVoterFor <= 3 ? isVoterFor : isVoterFor - 3}
        </text>
      )}

      {/* Kill button */}
      {!isDown ? (
        <foreignObject x={pos.x - 22} y={pos.y + NODE_R + 4} width={44} height={16}>
          <button
            onClick={() => onKill(node.id)}
            style={{
              width: '100%', height: '100%', fontSize: 9, cursor: 'pointer',
              background: 'transparent', color: 'var(--danger)',
              border: '1px solid var(--danger)', borderRadius: 3, padding: 0,
            }}
          >
            kill
          </button>
        </foreignObject>
      ) : (
        <text x={pos.x} y={pos.y + NODE_R + 14} textAnchor="middle" fontSize={8} fill="var(--danger)">
          DOWN
        </text>
      )}
    </motion.g>
  )
}

export function MaekawaView({ onKill }: { onKill: (id: number) => void }) {
  const nodes   = useStore(s => s.nodes)
  const flying  = useStore(s => s.flying)
  const svgRef  = useRef<SVGSVGElement>(null)
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

  const nodeList = useMemo(
    () => Array.from(nodes.values()).filter(n => n.id >= 4).sort((a, b) => a.id - b.id),
    [nodes]
  )

  // Active = not down. Grid layout and quorum are based on active only.
  const activeList = nodeList.filter(nd => nd.role !== 'down')
  const activeWorkerIds = activeList.map(nd => nd.id)

  const nActive = activeList.length
  const nTotal  = nodeList.length
  const cols    = nActive > 0 ? Math.ceil(Math.sqrt(nActive)) : 1

  // Active nodes get a fresh grid; DOWN nodes are placed off to the side (dimmed)
  const spacing      = Math.min(dims.w / (cols + 1.5), dims.h / (Math.ceil(nActive / cols) + 1.5), 140)
  const activePositions = gridPositions(nActive, dims.w / 2, dims.h * 0.45, spacing)

  // Build posMap: active nodes get real grid slots, down nodes stacked at bottom-left
  const posMap = new Map<number, Point>()
  let downIdx = 0
  for (const nd of nodeList) {
    const ai = activeList.indexOf(nd)
    if (ai !== -1) {
      posMap.set(nd.id, activePositions[ai])
    } else {
      posMap.set(nd.id, { x: 40 + downIdx * 60, y: dims.h - 40 })
      downIdx++
    }
  }

  // Compute each active node's quorum client-side (backend doesn't broadcast quorum)
  const quorumMap = new Map<number, Set<number>>()
  for (const id of activeWorkerIds) {
    quorumMap.set(id, new Set(computeQuorum(id, activeWorkerIds)))
  }

  const csNode     = nodeList.find(nd => nd.in_cs)
  const waitingIds = new Set(nodeList.filter(nd => !nd.in_cs && nd.role !== 'down' && nd.clock > 0).map(nd => nd.id))

  const voterMap = new Map<number, number>()
  for (const nd of nodeList) {
    if (nd.voted_for > 0) voterMap.set(nd.id, nd.voted_for)
  }

  // Maekawa-only flying messages — exclude cross-panel (those go to CrossPanelOverlay)
  const maekawaFlying = flying.filter(m => m.from >= 4 && m.to >= 4)

  return (
    <svg ref={svgRef} width="100%" height="100%" style={{ display: 'block' }}>

      {/* Grid overlay lines — only for active nodes */}
      <QuorumGridLines positions={activePositions} cols={cols} color="var(--accent)" />

      {/* Quorum connection lines from CS holder */}
      {csNode && Array.from(quorumMap.get(csNode.id) ?? []).map(qid => {
        const from = posMap.get(csNode.id)
        const to   = posMap.get(qid)
        if (!from || !to || qid === csNode.id) return null
        return (
          <motion.line key={`q${qid}`}
            x1={from.x} y1={from.y} x2={to.x} y2={to.y}
            stroke="var(--cs)" strokeWidth={1.5} strokeDasharray="6 4"
            animate={{ opacity: [0.4, 0.9, 0.4] }}
            transition={{ duration: 1.2, repeat: Infinity }}
          />
        )
      })}

      {/* Info bar at top */}
      <text x={dims.w / 2} y={26} textAnchor="middle" fontSize={11} fontWeight={700}
        fill="var(--text-dim)" letterSpacing="0.04em">
        {nTotal > 0
          ? `${nActive} ACTIVE · ${nTotal} TOTAL · ${cols}×${Math.ceil(nActive / cols)} GRID · QUORUM SIZE ${nActive > 0 ? computeQuorum(activeWorkerIds[0], activeWorkerIds).length : 0}`
          : 'NO WORKERS'}
      </text>

      {/* CS/quorum status line */}
      {csNode ? (
        <text x={dims.w / 2} y={44} textAnchor="middle" fontSize={10} fill="var(--cs)">
          W{csNode.id - 3} IN CRITICAL SECTION · quorum: [{Array.from(quorumMap.get(csNode.id) ?? []).map(q => `W${q - 3}`).join(', ')}]
        </text>
      ) : (
        <text x={dims.w / 2} y={44} textAnchor="middle" fontSize={10} fill="var(--text-dim)">
          No node in critical section
        </text>
      )}

      {/* Worker nodes */}
      {nodeList.map((node) => {
        const pos = posMap.get(node.id)!
        // Show quorum ring only when someone is in CS and this node is in their quorum
        const inQuorum = node.role !== 'down' && !!csNode && csNode.id !== node.id && (quorumMap.get(csNode.id)?.has(node.id) ?? false)
        const isVoterFor = voterMap.get(node.id) ?? -1
        const waiting   = waitingIds.has(node.id)
        return (
          <WorkerNodeCircle
            key={node.id}
            node={node}
            pos={pos}
            inQuorum={inQuorum}
            isVoterFor={isVoterFor}
            waiting={waiting}
            onKill={onKill}
            registerRef={(el) => register(node.id, el)}
          />
        )
      })}

      {/* Flying messages — Maekawa */}
      {maekawaFlying.map(msg => {
        const from = posMap.get(msg.from)
        const to   = posMap.get(msg.to)
        if (!from || !to) return null
        return <MessageArrow key={msg.id} msg={msg} from={from} to={to} durationMs={600} arcHeight={28} />
      })}

      {/* State legend */}
      <g transform={`translate(8, ${dims.h - 52})`}>
        {[
          { color: 'var(--cs)',      label: 'In CS' },
          { color: 'var(--waiting)', label: 'Waiting' },
          { color: 'var(--accent)',  label: 'Quorum member' },
          { color: '#4a5568',        label: 'Idle' },
          { color: 'var(--border)',  label: 'Down' },
        ].map(({ color, label }, i) => (
          <g key={label} transform={`translate(${i * 98}, 0)`}>
            <circle cx={5} cy={5} r={4} fill={color} />
            <text x={13} y={9} fontSize={9} fill="var(--text-dim)">{label}</text>
          </g>
        ))}
      </g>

      {/* Message legend */}
      <g transform={`translate(8, ${dims.h - 18})`}>
        {[
          { color: '#f85149', label: 'REQ' },
          { color: '#2ea043', label: 'GRANT' },
          { color: '#f0a500', label: 'DEFER' },
          { color: '#a371f7', label: 'INQUIRE' },
          { color: '#ffa657', label: 'YIELD' },
          { color: '#6e7681', label: 'RELEASE' },
        ].map(({ color, label }, i) => (
          <g key={label} transform={`translate(${i * 80}, 0)`}>
            <circle cx={5} cy={0} r={4} fill={color} />
            <text x={13} y={4} fontSize={9} fill="var(--text-dim)">{label}</text>
          </g>
        ))}
      </g>
    </svg>
  )
}
