# 🇱🇧 LB Market Monitor

Lebanon Price Drop Tracker — Tracking real estate and car price drops since March 1, 2026.

## How it works

- **Scrapers** run twice daily via GitHub Actions (8AM & 8PM Beirut time)
- Data is pulled from **OLX Lebanon** listings
- Price changes are tracked against a **March 1, 2026 baseline**
- Dashboard auto-updates on every deploy

## Live Dashboard

**[bdertodream.github.io/lb-market-monitor](https://bdertodream.github.io/lb-market-monitor)**

## Features

- 🏠 Real Estate price drops (apartments, villas, land, chalets, etc.)
- 🚗 Car price drops (all makes and body types)
- 🌗 Dark/light mode with glassmorphism UI
- 🔍 Filter by type, area, make + sort by % drop, $ drop, recency, price
- 📊 Live stats: tracked listings, drop count, avg drop %, biggest drop
