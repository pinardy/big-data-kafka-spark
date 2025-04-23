import React, { useEffect, useState, useCallback } from 'react'
import {
  CRow,
  CCol,
  CCard,
  CCardBody,
  CCardHeader,
  CTable,
  CTableHead,
  CTableBody,
  CTableRow,
  CTableHeaderCell,
  CTableDataCell,
  CSpinner,
} from '@coreui/react'
import { CChartBar, CChartDoughnut, CChartLine, CChartPie } from '@coreui/react-chartjs'
import { getAllTripData } from '../../dao/TripDao'

const Dashboard = () => {
  const [data, setData] = useState([])
  const [tripData, setTripData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [totalSafe, setTotalSafe] = useState(0)
  const [totalDangerous, setTotalDangerous] = useState(0)
  const [totalTripSpeed, setTotalTripSpeed] = useState([])
  const [totalTripSpeedStd, setTotalTripSpeedStd] = useState([])
  const [totalTripSecond, setTotalTripSecond] = useState([])

  const fetchTripData = useCallback(async () => {
    console.log('🔄 Fetching telematics data...')
    const trips = await getAllTripData()
    console.log('📊 Telematics data:', trips)
    setData(trips)
  }, [])

  useEffect(() => {
    fetchTripData()
    setLoading(false)
    // TODO: use trips to display relevant charts & visualisations
  }, [fetchTripData])

  const handleTripLable = () => {
    const safeTrips = data.filter((trip) => trip.label == 0)
    const dangerousTrips = data.filter((trip) => trip.label == 1)
    setTotalSafe(safeTrips.length)
    setTotalDangerous(dangerousTrips.length)
  }

  const handleTripSpeed = () => {
    // Calculate different speed
    const Trips5 = data.filter((trip) => trip.avg_speed > 0 && trip.avg_speed <= 5).length
    const Trips10 = data.filter((trip) => trip.avg_speed > 5 && trip.avg_speed <= 10).length
    const Trips15 = data.filter((trip) => trip.avg_speed > 10 && trip.avg_speed <= 15).length
    const Trips20 = data.filter((trip) => trip.avg_speed > 15 && trip.avg_speed <= 20).length
    const Trips25 = data.filter((trip) => trip.avg_speed > 20 && trip.avg_speed <= 25).length
    setTotalTripSpeed([Trips5, Trips10, Trips15, Trips20, Trips25])

    const Trips5Std = data.filter((trip) => trip.std_speed > 0 && trip.std_speed <= 5).length
    const Trips10Std = data.filter((trip) => trip.std_speed > 5 && trip.std_speed <= 10).length
    const Trips15Std = data.filter((trip) => trip.std_speed > 10 && trip.std_speed <= 15).length
    const Trips20Std = data.filter((trip) => trip.std_speed > 15 && trip.std_speed <= 20).length
    const Trips25Std = data.filter((trip) => trip.std_speed > 20 && trip.std_speed <= 25).length
    setTotalTripSpeedStd([Trips5Std, Trips10Std, Trips15Std, Trips20Std, Trips25Std])
  }

  const handleTripSecond = () => {
    // Calculate different second
    const TripsSed600 = data.filter((trip) => trip.second > 0 && trip.second <= 600).length
    const TripsSed1800 = data.filter((trip) => trip.second > 600 && trip.second <= 1800).length
    const TripsSedMore = data.filter((trip) => trip.second > 1800).length
    setTotalTripSecond([TripsSed600, TripsSed1800, TripsSedMore])
  }

  useEffect(() => {
    // Calculate total safe and dangerous trips
    handleTripLable()
    handleTripSpeed()
    handleTripSecond()
  }, [data])

  return (
    <>
      <h2 className="mb-4">📊 Telematics Dashboard</h2>
      {loading ? (
        <div className="text-center mt-5">
          <CSpinner color="primary" />
          <p>Loading telematics data...</p>
        </div>
      ) : (
        <CCard>
          <div className="d-flex justify-content-between align-items-center">
            <span>🚗 Trip Sensor Summary</span>
            <input
              type="text"
              className="form-control w-25"
              placeholder="Search by Booking ID"
              onChange={(e) => {
                const searchValue = e.target.value.toLowerCase()
                setTripData(data.find((trip) => trip.bookingid == searchValue))
              }}
            />
          </div>

          {tripData && (
            <CCardBody>
              <CTable responsive striped hover>
                <CTableHead>
                  <CTableRow>
                    <CTableHeaderCell>Booking ID</CTableHeaderCell>
                    <CTableHeaderCell>Avg Speed</CTableHeaderCell>
                    <CTableHeaderCell>Std Speed</CTableHeaderCell>
                    <CTableHeaderCell>Avg Accel</CTableHeaderCell>
                    <CTableHeaderCell>Max Accel</CTableHeaderCell>
                    <CTableHeaderCell>Std Accel</CTableHeaderCell>
                    <CTableHeaderCell>Avg Gyro</CTableHeaderCell>
                    <CTableHeaderCell>Std Gyro</CTableHeaderCell>
                  </CTableRow>
                </CTableHead>
                <CTableBody>
                  <CTableRow>
                    <CTableDataCell>{tripData.bookingid}</CTableDataCell>
                    <CTableDataCell>{tripData.avg_speed.toFixed(2)}</CTableDataCell>
                    <CTableDataCell>{tripData.std_speed.toFixed(2)}</CTableDataCell>
                    <CTableDataCell>{tripData.avg_accel_mag.toFixed(2)}</CTableDataCell>
                    <CTableDataCell>{tripData.max_accel_mag.toFixed(2)}</CTableDataCell>
                    <CTableDataCell>{tripData.std_accel_mag.toFixed(2)}</CTableDataCell>
                    <CTableDataCell>{tripData.avg_gyro_mag.toFixed(2)}</CTableDataCell>
                    <CTableDataCell>{tripData.std_gyro_mag.toFixed(2)}</CTableDataCell>
                  </CTableRow>
                </CTableBody>
              </CTable>
            </CCardBody>
          )}

          <div style={{ margin: '20px 0' }}></div>

          <CRow>
            <CCol xs={12}></CCol>
            <CCol xs={6}>
              <CCard className="mb-4">
                <CCardHeader>Bar Chart</CCardHeader>
                <CCardBody>
                  <CChartBar
                    data={{
                      labels: ['Safe', 'Dangerous'],
                      datasets: [
                        {
                          label: 'Trip Label',
                          backgroundColor: '#f87979',
                          data: [totalSafe, totalDangerous],
                        },
                      ],
                    }}
                    labels="months"
                  />
                </CCardBody>
              </CCard>
            </CCol>

            <CCol xs={6}>
              <CCard className="mb-4">
                <CCardHeader>Line Chart</CCardHeader>
                <CCardBody>
                  <CChartLine
                    data={{
                      labels: ['5', '10', '15', '20', '25'],
                      datasets: [
                        {
                          label: 'Average Speed',
                          backgroundColor: 'rgba(220, 220, 220, 0.2)',
                          borderColor: 'rgba(220, 220, 220, 1)',
                          pointBackgroundColor: 'rgba(220, 220, 220, 1)',
                          pointBorderColor: '#fff',
                          data: totalTripSpeed,
                        },
                        {
                          label: 'Standerd Deviation Speed',
                          backgroundColor: 'rgba(151, 187, 205, 0.2)',
                          borderColor: 'rgba(151, 187, 205, 1)',
                          pointBackgroundColor: 'rgba(151, 187, 205, 1)',
                          pointBorderColor: '#fff',
                          data: totalTripSpeedStd,
                        },
                      ],
                    }}
                  />
                </CCardBody>
              </CCard>
            </CCol>

            <CCol xs={6}>
              <CCard className="mb-4">
                <CCardHeader>Doughnut Chart</CCardHeader>
                <CCardBody>
                  <CChartDoughnut
                    data={{
                      labels: [
                        'Short trip (<10min)',
                        'Normal trip (10-30min)',
                        'Long trip (>30min)',
                      ],
                      datasets: [
                        {
                          backgroundColor: ['#41B883', '#E46651', '#00D8FF', '#DD1B16'],
                          data: totalTripSecond,
                        },
                      ],
                    }}
                  />
                </CCardBody>
              </CCard>
            </CCol>

            <CCol xs={6}>
              <CCard className="mb-4">
                <CCardHeader>Pie Chart</CCardHeader>
                <CCardBody>
                  <CChartPie
                    data={{
                      labels: ['Red', 'Green', 'Yellow'],
                      datasets: [
                        {
                          data: [300, 50, 100],
                          backgroundColor: ['#FF6384', '#36A2EB', '#FFCE56'],
                          hoverBackgroundColor: ['#FF6384', '#36A2EB', '#FFCE56'],
                        },
                      ],
                    }}
                  />
                </CCardBody>
              </CCard>
            </CCol>
          </CRow>
        </CCard>
      )}
    </>
  )
}

export default Dashboard
