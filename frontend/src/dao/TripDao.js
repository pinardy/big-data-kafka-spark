import axios from 'axios'

export const getAllTripData = async () => {
  try {
    const trips = await axios
      .get('http://localhost:8000/trip/get_all')
      .then((response) => response.data)
    return trips ? trips : []
  } catch (error) {
    return []
  }
}

export const getTripData = async (tripId) => {
  try {
    const tripData = await axios
      .get(`http://localhost:8000/trip/${tripId}`)
      .then((response) => response.data)
    return tripData ? tripData : []
  } catch (error) {
    return []
  }
}
