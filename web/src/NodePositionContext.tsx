import { createContext, useContext, useRef, useCallback, type ReactNode } from 'react'

interface NodePositionCtx {
  register: (id: number, el: SVGCircleElement | null) => void
  getPos: (id: number) => { x: number; y: number } | undefined
}

const Ctx = createContext<NodePositionCtx>({
  register: () => {},
  getPos: () => undefined,
})

// We store refs to the DOM elements so positions are always fresh (no re-render needed)
export function NodePositionProvider({ children }: { children: ReactNode }) {
  const elementsRef = useRef<Map<number, SVGCircleElement>>(new Map())

  const register = useCallback((id: number, el: SVGCircleElement | null) => {
    if (el) {
      elementsRef.current.set(id, el)
    } else {
      elementsRef.current.delete(id)
    }
  }, [])

  const getPos = useCallback((id: number) => {
    const el = elementsRef.current.get(id)
    if (!el) return undefined
    const r = el.getBoundingClientRect()
    return { x: r.left + r.width / 2, y: r.top + r.height / 2 }
  }, [])

  return <Ctx.Provider value={{ register, getPos }}>{children}</Ctx.Provider>
}

export function useNodePositions() {
  return useContext(Ctx)
}
