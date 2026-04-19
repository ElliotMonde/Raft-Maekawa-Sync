import { useEffect, useRef, useState } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { useStore, type FlyingMessage, type DashEventType } from '../store'
import { useNodePositions } from '../NodePositionContext'

// Only these event types cross the Raft↔Maekawa boundary
const CROSS_TYPES = new Set<DashEventType>([
  'task_assigned',   // Raft leader → worker (broadcast via committed log)
  'task_done',       // worker → Raft leader (ReportTaskSuccess)
  'task_failed',     // worker → Raft leader (ReportTaskFailure)
])

function isCross(msg: FlyingMessage): boolean {
  if (!CROSS_TYPES.has(msg.type)) return false
  const fromRaft = msg.from >= 1 && msg.from <= 3
  const toWorker = msg.to >= 4
  const fromWorker = msg.from >= 4
  const toRaft = msg.to >= 1 && msg.to <= 3
  return (fromRaft && toWorker) || (fromWorker && toRaft)
}

const COLOR: Partial<Record<DashEventType, string>> = {
  task_assigned: '#f0a500',   // amber — leader dispatching work
  task_done:     '#2ea043',   // green — success back to leader
  task_failed:   '#f85149',   // red   — failure back to leader
}

const LABEL: Partial<Record<DashEventType, string>> = {
  task_assigned: 'TASK',
  task_done:     'DONE',
  task_failed:   'FAIL',
}

interface Point { x: number; y: number }

function CrossArrow({
  msg,
  from,
  to,
  speed,
  onDone,
}: {
  msg: FlyingMessage
  from: Point
  to: Point
  speed: number
  onDone: () => void
}) {
  const dur = 900 / speed
  const color = COLOR[msg.type] ?? '#58a6ff'
  const label = LABEL[msg.type] ?? msg.type

  // Arc: push midpoint toward center of screen for a nice curve
  const mx = (from.x + to.x) / 2
  const my = (from.y + to.y) / 2 - 50

  useEffect(() => {
    const t = setTimeout(onDone, dur + 200)
    return () => clearTimeout(t)
  }, [dur, onDone])

  return (
    <motion.g style={{ pointerEvents: 'none' }}>
      {/* Trailing line that fades as the dot moves */}
      <motion.path
        d={`M ${from.x} ${from.y} Q ${mx} ${my} ${to.x} ${to.y}`}
        fill="none"
        stroke={color}
        strokeWidth={1.5}
        strokeDasharray="4 4"
        initial={{ pathLength: 0, opacity: 0.6 }}
        animate={{ pathLength: 1, opacity: 0 }}
        transition={{ duration: dur / 1000, ease: 'easeInOut' }}
      />
      {/* Travelling dot */}
      <motion.circle
        r={6}
        fill={color}
        filter={`drop-shadow(0 0 4px ${color})`}
        initial={{ cx: from.x, cy: from.y, opacity: 1 }}
        animate={{
          cx: [from.x, mx, to.x],
          cy: [from.y, my, to.y],
          opacity: [1, 1, 0],
        }}
        transition={{ duration: dur / 1000, ease: 'easeInOut' }}
      />
      {/* Label riding above the dot */}
      <motion.text
        textAnchor="middle"
        fontSize={9}
        fontWeight={800}
        fontFamily="var(--mono)"
        fill={color}
        initial={{ x: from.x, y: from.y - 10, opacity: 1 }}
        animate={{
          x: [from.x, mx, to.x],
          y: [from.y - 10, my - 10, to.y - 10],
          opacity: [1, 1, 0],
        }}
        transition={{ duration: dur / 1000, ease: 'easeInOut' }}
      >
        {label}
      </motion.text>
    </motion.g>
  )
}

export function CrossPanelOverlay() {
  const flying = useStore(s => s.flying)
  const removeFlying = useStore(s => s.removeFlying)
  const speed = useStore(s => s.speed)
  const { getPos } = useNodePositions()

  // We need to re-resolve positions on every render tick since the SVGs
  // may resize. We snapshot positions at render time for each cross msg.
  const [, tick] = useState(0)
  const rafRef = useRef<number>(0)

  // Kick a re-render on every animation frame so positions stay fresh
  useEffect(() => {
    let running = true
    function frame() {
      if (!running) return
      tick(n => n + 1)
      rafRef.current = requestAnimationFrame(frame)
    }
    rafRef.current = requestAnimationFrame(frame)
    return () => {
      running = false
      cancelAnimationFrame(rafRef.current)
    }
  }, [])

  const crossMsgs = flying.filter(isCross)

  return (
    <svg
      style={{
        position: 'fixed',
        inset: 0,
        width: '100vw',
        height: '100vh',
        pointerEvents: 'none',
        zIndex: 100,
        overflow: 'visible',
      }}
    >
      <AnimatePresence>
        {crossMsgs.map(msg => {
          const from = getPos(msg.from)
          const to = getPos(msg.to)
          if (!from || !to) return null
          return (
            <CrossArrow
              key={msg.id}
              msg={msg}
              from={from}
              to={to}
              speed={speed}
              onDone={() => removeFlying(msg.id)}
            />
          )
        })}
      </AnimatePresence>
    </svg>
  )
}
