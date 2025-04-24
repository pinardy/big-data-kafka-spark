import React from 'react'
import CIcon from '@coreui/icons-react'
import { cilSpeedometer, cilLaptop } from '@coreui/icons'
import { CNavItem } from '@coreui/react'

const _nav = [
  {
    component: CNavItem,
    name: 'Dashboard',
    to: '/dashboard',
    icon: <CIcon icon={cilLaptop} customClassName="nav-icon" />,
  },
  {
    component: CNavItem,
    name: 'Trip',
    to: '/trip',
    icon: <CIcon icon={cilSpeedometer} customClassName="nav-icon" />,
  },
]

export default _nav
