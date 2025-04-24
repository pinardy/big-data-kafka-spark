import React from 'react'

const Dashboard = React.lazy(() => import('./views/dashboard/Dashboard'))

const Trip = React.lazy(() => import('./views/pages/trip/Trip'))

const routes = [
  { path: '/', exact: true, name: 'Home' },
  { path: '/dashboard', name: 'Dashboard', element: Dashboard },
  { path: '/trip', name: 'Trip', element: Trip },
]

export default routes
