import { motion, AnimatePresence } from 'framer-motion'
import { useEffect } from 'react'
import { useStore, type FlyingMessage, type DashEventType } from '../store'

interface Point { x: number; y: number }

const COLOR: Record<string, string> = {
  vote_request:   '#f77f00',
  vote_granted:   '#2ea043',
  heartbeat:      '#388bfd',
  log_replicated: '#58a6ff',
  log_committed:  '#2ea043',
  task_assigned:  '#f0a500',
  lock_request:   '#f85149',
  lock_grant:     '#2ea043',
  lock_defer:     '#f0a500',
  lock_inquire:   '#a371f7',
  lock_yield:     '#ffa657',
  lock_release:   '#6e7681',
  cs_enter:       '#2ea043',
  cs_exit:        '#6e7681',
}

const LABEL: Partial<Record<DashEventType, string>> = {
  vote_request:   'VOTE?',
  vote_granted:   'VOTE✓',
  heartbeat:      '♥',
  log_replicated: 'LOG',
  log_committed:  'COMMIT',
  task_assigned:  'TASK',
  lock_request:   'REQ',
  lock_grant:     'GRANT',
  lock_defer:     'DEFER',
  lock_inquire:   'INQ?',
  lock_yield:     'YIELD',
  lock_release:   'REL',
}

interface Props {
  msg: FlyingMessage
  from: Point
  to: Point
  durationMs?: number
  arcHeight?: number
}

export function MessageArrow({ msg, from, to, durationMs = 800, arcHeight = 35 }: Props) {
  const removeFlying = useStore(s => s.removeFlying)
  const speed = useStore(s => s.speed)
  const dur = durationMs / speed

  useEffect(() => {
    const t = setTimeout(() => removeFlying(msg.id), dur + 150)
    return () => clearTimeout(t)
  }, [msg.id, dur, removeFlying])

  const color = COLOR[msg.type] ?? '#58a6ff'
  const label = LABEL[msg.type as DashEventType] ?? msg.type.replace('_', ' ')

  // Arc midpoint: perpendicular offset from midpoint of segment
  const mx = (from.x + to.x) / 2
  const my = (from.y + to.y) / 2 - arcHeight

  return (
    <AnimatePresence>
      <motion.g key={msg.id} style={{ pointerEvents: 'none' }}>
        <motion.circle
          cx={from.x} cy={from.y} r={5}
          fill={color}
          initial={{ cx: from.x, cy: from.y, opacity: 1 }}
          animate={{
            cx: [from.x, mx, to.x],
            cy: [from.y, my, to.y],
            opacity: [1, 1, 0],
          }}
          transition={{ duration: dur / 1000, ease: 'easeInOut' }}
        />
        <motion.text
          x={from.x} y={from.y - 8}
          textAnchor="middle"
          fontSize={9}
          fill={color}
          fontWeight={700}
          fontFamily="var(--mono)"
          initial={{ x: from.x, y: from.y - 8, opacity: 1 }}
          animate={{
            x: [from.x, mx, to.x],
            y: [from.y - 8, my - 8, to.y - 8],
            opacity: [1, 1, 0],
          }}
          transition={{ duration: dur / 1000, ease: 'easeInOut' }}
        >
          {label}
        </motion.text>
      </motion.g>
    </AnimatePresence>
  )
}
