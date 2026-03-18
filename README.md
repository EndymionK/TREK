# NOMAD

**Navigation Organizer for Maps, Activities & Destinations**

A self-hosted travel planner for organizing trips, places, budgets, packing lists, and more.

[![License](https://img.shields.io/github/license/mauriceboe/NOMAD)](LICENSE)
[![Docker Pulls](https://img.shields.io/docker/pulls/mauriceboe/nomad)](https://hub.docker.com/r/mauriceboe/nomad)
[![GitHub Stars](https://img.shields.io/github/stars/mauriceboe/NOMAD)](https://github.com/mauriceboe/NOMAD)
[![Last Commit](https://img.shields.io/github/last-commit/mauriceboe/NOMAD)](https://github.com/mauriceboe/NOMAD/commits)

## Features

- **Drag & Drop Planner** — Organize places into day plans with drag & drop reordering
- **Interactive Map** — Leaflet map with place markers, route visualization, and customizable tile sources
- **Google Places Search** — Search and auto-fill place details including ratings, reviews, and opening hours (requires API key)
- **Weather Forecasts** — Current weather and 5-day forecasts for your destinations (requires API key)
- **Budget Tracking** — Category-based expenses with pie chart, per-person/per-day splitting, and multi-currency support
- **Packing Lists** — Grouped checklists with progress tracking, drag & drop reordering, and smart suggestions
- **Reservations** — Track booking status, confirmation numbers, and reservation details
- **File Storage** — Attach documents, tickets, and PDFs to your trips (up to 50 MB per file)
- **Photo Gallery** — Upload and manage trip photos with lightbox view
- **PDF Export** — Export complete trip plans as PDF
- **Multi-User** — Invite members to collaborate on shared trips
- **Day Notes** — Add notes to individual days
- **Admin Panel** — User management, global categories, API key configuration, and backups
- **Auto-Backups** — Scheduled backups with configurable interval and retention
- **Dark Mode** — Full light and dark theme support
- **Multilingual** — English and German (i18n)
- **Customizable** — Temperature units, time format (12h/24h), map tile sources, default coordinates

## Tech Stack

- **Backend**: Node.js 22 + Express + SQLite (`node:sqlite`)
- **Frontend**: React 18 + Vite + Tailwind CSS
- **State**: Zustand
- **Auth**: JWT
- **Maps**: Leaflet + Google Places API (optional)
- **Weather**: OpenWeatherMap API (optional)
- **Icons**: lucide-react

## Quick Start

### Prerequisites

- Docker & Docker Compose

### 1. Create a `docker-compose.yml`

```yaml
services:
  app:
    image: mauriceboe/nomad:latest
    container_name: nomad
    ports:
      - "3000:3000"
    environment:
      - NODE_ENV=production
      - JWT_SECRET=change-me-to-a-long-random-string
      - PORT=3000
    volumes:
      - ./data:/app/data
      - ./uploads:/app/uploads
    restart: unless-stopped
```

### 2. Start the app

```bash
docker compose up -d
```

Done. Open `http://your-server:3000` in your browser.

### 3. First setup

The **first user to register** automatically becomes the admin. No default credentials.

### Updating

```bash
docker compose pull
docker compose up -d
```

Your data is persisted in the `./data` and `./uploads` volumes.

### Reverse Proxy (recommended)

For production, put NOMAD behind a reverse proxy with HTTPS (e.g. Nginx, Caddy, Traefik).

Example with **Caddy**:

```
nomad.yourdomain.com {
    reverse_proxy localhost:3000
}
```

Optionally restrict CORS by adding to `environment`:

```yaml
- ALLOWED_ORIGINS=https://nomad.yourdomain.com
```

## Optional API Keys

API keys are configured in the **Admin Panel** after login.

### Google Maps (Place Search)

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a project and enable the **Places API (New)**
3. Create an API key under Credentials
4. In NOMAD: Admin Panel → API Keys → Google Maps

### OpenWeatherMap (Weather Forecasts)

1. Sign up at [OpenWeatherMap](https://openweathermap.org/api)
2. Get a free API key
3. In NOMAD: Admin Panel → API Keys → OpenWeatherMap

## Building from Source

```bash
git clone https://github.com/mauriceboe/NOMAD.git
cd NOMAD
docker build -t nomad .
```

## Data & Backups

- **Database**: SQLite, stored in `./data/travel.db`
- **Uploads**: Stored in `./uploads/`
- **Backups**: Create and restore via Admin Panel
- **Auto-Backups**: Configurable schedule and retention in Admin Panel

## License

[MIT](LICENSE)
