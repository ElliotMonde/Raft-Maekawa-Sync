import { useEffect, useRef } from 'react'
import { useStore, type DashEvent } from './store'

const WS_URL = `ws://${window.location.host}/ws`
const RECONNECT_MS = 2000

export function useWebSocket() {
  const applyEvent = useStore(s => s.applyEvent)
  const setConnected = useStore(s => s.setWsConnected)
  const wsRef = useRef<WebSocket | null>(null)
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  useEffect(() => {
    function connect() {
      const ws = new WebSocket(WS_URL)
      wsRef.current = ws

      ws.onopen = () => setConnected(true)
      ws.onclose = () => {
        setConnected(false)
        timerRef.current = setTimeout(connect, RECONNECT_MS)
      }
      ws.onerror = () => ws.close()
      ws.onmessage = (ev) => {
        try {
          const e = JSON.parse(ev.data as string) as DashEvent
          applyEvent(e)
        } catch { /* ignore malformed */ }
      }
    }

    connect()
    
    return () => {
      wsRef.current?.close()
      if (timerRef.current) clearTimeout(timerRef.current)
    }
  }, [applyEvent, setConnected])

  function send(payload: unknown) {
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify(payload))
    }
  }

  return { send }
}
