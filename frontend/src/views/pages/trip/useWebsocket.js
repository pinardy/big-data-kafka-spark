import { useEffect } from 'react'

export const useWebSocket = (url, onMessage) => {
  useEffect(() => {
    const socket = new WebSocket(url)

    socket.onopen = () => {
      console.log('WebSocket connection established')
    }

    socket.onmessage = (event) => {
      if (onMessage) {
        onMessage(event.data)
      }
    }

    socket.onerror = (error) => {
      console.error('WebSocket error:', error)
    }

    socket.onclose = () => {
      console.log('WebSocket connection closed')
    }

    return () => {
      socket.close()
    }
  }, [url, onMessage])
}
