# IMS Group Project Dashboard (Realtime)

A realtime collaborative dashboard for your IMS project.

## Features

- Shared progress across all users (status, priority, due date, assignee).
- Realtime updates with Server-Sent Events (SSE).
- Per-task assignee with standardized member list.
- Operation log with one-step undo.
- Filters: by assignee, by current week due date, and unfinished-only.

## Run locally

```bash
npm start
```

Open [http://localhost:10000](http://localhost:10000).

## Persistence

- State is persisted to `data/shared-state.json`.
- If deployed to Render, this persists while the instance remains active.
