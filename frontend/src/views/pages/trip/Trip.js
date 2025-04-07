import React from 'react'
import { useWebSocket } from './useWebsocket'
import { styles } from './styles'

const Trip = () => {
  const websocketUrl = 'ws://localhost:8000/ws/live-data'

  useWebSocket(websocketUrl, (data) => {
    console.log('Received data:', data)
  })

  return (
    <div style={styles.container}>
      <header style={styles.header}>
        <h1 style={styles.title}>Grab Trip</h1>
      </header>
      <main style={styles.main}>
        <section style={styles.tripDetails}>
          <h2 style={styles.sectionTitle}>Trip Details</h2>
          <p>Pickup: 123 Main Street</p>
          <p>Destination: 456 Elm Street</p>
          <p>Fare: $15.00</p>
        </section>
        <section style={styles.driverDetails}>
          <h2 style={styles.sectionTitle}>Driver Details</h2>
          <p>Name: John Doe</p>
          <p>Car: Toyota Prius</p>
          <p>License Plate: ABC-1234</p>
        </section>
        <button style={styles.button}>Contact Driver</button>
      </main>
    </div>
  )
}

export default Trip
