import { createRoot } from 'react-dom/client'
import App from './App.jsx'
import './tailwind.css'
import './styles.css'
import './workspace.css'
import './theme-institutional.css'
import './cot-heatmap.css'
import './workstation/styles/instrumentWorkstation.css'

createRoot(document.getElementById('root')).render(<App />)
