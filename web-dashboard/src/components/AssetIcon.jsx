import React from 'react'

// Local SVG artwork stays sharp at any size and requires no image downloads.
const identities = [
  ['natural gas','gas','#51bce8'], ['crude','oil','#171b20'],
  ['gold','bars','#d6a637'], ['silver','bars','#99aab9'],
  ['palladium','Pd','#657f93'], ['platinum','Pt','#91a5ac'],
  ['copper','bars','#b97047'], ['soybean','soy','#469958'],
  ['sugar','sugar','#a0b1c0'], ['coffee','bean','#79513f'],
  ['cocoa','bean','#995d40'], ['cotton','cotton','#7c9e9e'],
  ['corn','grain','#cfa331'], ['wheat','grain','#b38e43'],
  ['bitcoin','₿','#ef941d'], ['euro','€','#315fa7'],
  ['british pound','£','#9b4059'], ['japanese yen','¥','#c45258'],
  ['swiss franc','Fr','#c9474c'], ['australian dollar','A$','#366b9e'],
  ['canadian dollar','C$','#c34b51'], ['nz dollar','NZ$','#3c7795'],
  ['dollar index','$','#467a66'], ['nasdaq','NQ','#365fb2'],
  ['s&p','S&P','#b54754'], ['dow','DJ','#396f9f'],
]
export function AssetIcon({ name, size = 28 }) {
  const [, kind, color] = identities.find(([key]) => String(name).toLowerCase().includes(key)) || ['', String(name || '?').slice(0, 3).toUpperCase(), '#526375']
  let art
  switch (kind) {
    case 'oil': art = <path d="M16 6c-1.4 4.5-7 8.5-7 13a7 7 0 0 0 14 0c0-4.5-5.6-8.5-7-13Z" fill="white"/>; break
    case 'gas': art = <path d="M17 5c1 6-7 8-7 15a6.5 6.5 0 0 0 13 0c0-4-2-7-4-9 0 4-3 5-3 5 2-5 2-8 1-11Z" fill="white"/>; break
    case 'bars': art = <g fill="white" stroke={color} strokeWidth="1.2" strokeLinejoin="round"><path d="M12 8h8l2 7H10Z"/><path d="M7 17h8l2 7H5Z"/><path d="M18 17h7l2 7H16Z"/></g>; break
    case 'soy': art = <g fill="white"><ellipse cx="12" cy="18" rx="4.4" ry="6" transform="rotate(-30 12 18)"/><ellipse cx="21" cy="13" rx="4" ry="5.8" transform="rotate(30 21 13)"/><path d="M13 12C7 12 6 7 6 7c5-1 8 1 9 4Z"/></g>; break
    case 'sugar': art = <g stroke={color} strokeWidth="1" strokeLinejoin="round"><path d="m6 16 6-3 6 3v7l-6 4-6-4Z" fill="white"/><path d="m6 16 6 4 6-4m-6 4v7" fill="none"/><path d="m16 10 6-3 6 3v7l-6 4-6-4Z" fill="#edf3f7"/><path d="m16 10 6 4 6-4m-6 4v7" fill="none"/></g>; break
    case 'bean': art = <g fill="white" stroke={color} strokeWidth="1.4"><ellipse cx="12" cy="18" rx="4.7" ry="7" transform="rotate(-30 12 18)"/><path d="M11 12c-3 4 5 7 2 12" fill="none"/><ellipse cx="21" cy="13" rx="4.5" ry="6.5" transform="rotate(25 21 13)"/><path d="M23 7c-6 4 1 7-4 12" fill="none"/></g>; break
    case 'cotton': art = <g fill="white"><circle cx="11" cy="16" r="5"/><circle cx="16" cy="12" r="5"/><circle cx="21" cy="16" r="5"/><path d="m11 21 5 6 5-6Z"/></g>; break
    case 'grain': art = <g fill="white"><path d="M15 8h2v19h-2Z"/>{[10,15,20].map(y=><g key={y}><ellipse cx="12" cy={y} rx="2.3" ry="4" transform={`rotate(-45 12 ${y})`}/><ellipse cx="20" cy={y} rx="2.3" ry="4" transform={`rotate(45 20 ${y})`}/></g>)}</g>; break
    default: art = <text x="16" y="17" dominantBaseline="middle" textAnchor="middle" fill="white" fontFamily="Arial, sans-serif" fontWeight="700" fontSize={kind.length > 2 ? 10 : 14}>{kind}</text>
  }
  return <svg width={size} height={size} viewBox="0 0 32 32" aria-hidden="true" focusable="false" style={{ display: 'inline-block', flexShrink: 0, verticalAlign: 'middle' }}><circle cx="16" cy="16" r="15.3" fill={color} stroke="white" strokeOpacity=".16" strokeWidth="1"/>{art}</svg>
}
