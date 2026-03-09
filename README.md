# IMS Group Project Dashboard (Realtime)

A realtime collaborative dashboard for your IMS project.

## Features

- Shared checkbox progress across all users.
- Realtime sync (Socket.IO): updates appear instantly for everyone.
- Per-task assignee field (name input) synced to all users.
- Persisted state in Postgres when `DATABASE_URL` is set.
- File fallback (`data/shared-state.json`) when database is unavailable.

## Run locally

```bash
npm install
npm start
```

Open [http://localhost:10000](http://localhost:10000).

## Deploy on Render

Use a **Web Service** with:

- Build Command: `npm install`
- Start Command: `npm start`
- Env var: `DATABASE_URL` (recommended, for durable shared state)
