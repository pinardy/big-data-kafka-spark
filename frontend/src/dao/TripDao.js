import axios from 'axios'

export const getTripData = async () => {
  try {
    const trips = await axios
      .get('http://localhost:8000/trip/get_all')
      .then((response) => response.data)
    return trips ? trips : []
  } catch (error) {
    return []
  }
}
