import React, { useState, useEffect } from 'react'
import useWebSocket from 'react-use-websocket'

const Trip = () => {
  const websocketUrl = 'ws://localhost:8000/ws/live-data'

  const [latestData, setLatestData] = useState({
    bookingid: '',
    speed: 0,
    time: '',
    label: null,
  })

  const { lastMessage } = useWebSocket(websocketUrl, {
    shouldReconnect: () => true,
    reconnectAttempts: 10,
    reconnectInterval: 5000,
  })

  // Handle incoming WebSocket messages
  useEffect(() => {
    if (lastMessage !== null) {
      try {
        const parsed = JSON.parse(lastMessage.data)
        console.log('Streamed data:', parsed)

        if (parsed.bookingid == 1) {
          setLatestData((prev) => ({
            ...prev,
            bookingid: parsed.bookingid || prev.bookingid,
            speed: parsed.speed || prev.speed,
            time: parsed.time || prev.time,
            label: parsed.label !== undefined ? parsed.label : prev.label,
          }))
        }
      } catch (err) {
        console.error('Error parsing websocket data:', err)
      }
    }
  }, [lastMessage])

  const { bookingid, speed, time, label } = latestData

  return (
    <div style={styles.container}>
      <h1 style={styles.title}>🚗 Barg Trip</h1>

      <div style={styles.card}>
        <h2 style={styles.sectionTitle}>Trip Info</h2>
        <p>
          🆔 Booking ID: <strong>{bookingid || '—'}</strong>
        </p>
        <p>
          ⏱️ Time: <strong>{time || '—'} s</strong>
        </p>
        <p>
          💨 Speed: <strong>{typeof speed === 'number' ? speed.toFixed(2) : '0.00'} m/s</strong>
        </p>
      </div>

      {label !== null && (
        <div
          style={{
            ...styles.statusBar,
            backgroundColor: label === 0 ? '#2ecc71' : '#e74c3c',
          }}
        >
          {label === 0 ? '✅ Continue driving safely!' : '⚠️ You are driving too fast!'}
        </div>
      )}
    </div>
  )
}

const styles = {
  container: {
    maxWidth: '600px',
    margin: '2rem auto',
    padding: '2rem',
    backgroundColor: '#f9f9f9',
    borderRadius: '16px',
    boxShadow: '0 8px 24px rgba(0,0,0,0.1)',
  },
  title: {
    textAlign: 'center',
    fontSize: '2rem',
    marginBottom: '2rem',
    color: '#27ae60',
  },
  card: {
    backgroundColor: '#fff',
    borderRadius: '12px',
    padding: '1.2rem',
    marginBottom: '1.5rem',
    boxShadow: '0 4px 12px rgba(0,0,0,0.05)',
  },
  sectionTitle: {
    fontSize: '1.2rem',
    marginBottom: '0.8rem',
    color: '#2c3e50',
  },
  statusBar: {
    marginTop: '1.5rem',
    padding: '1rem',
    color: '#fff',
    fontWeight: 'bold',
    borderRadius: '8px',
    textAlign: 'center',
  },
}

export default Trip
