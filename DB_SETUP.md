# Database Setup (PostgreSQL)

This project now stores leaderboard data in PostgreSQL.

## 1. Set environment variables

Set `DATABASE_URL` to your Postgres connection string.

Example:

```bash
export DATABASE_URL="postgresql://USER:PASSWORD@HOST:5432/DBNAME"
```

Optional:

```bash
export PORT=8000
export DB_STATEMENT_TIMEOUT_MS=7000
export LEADERBOARD_ADMIN_TOKEN="set-a-strong-secret"
export ALLOWED_ORIGINS="https://<your-user>.github.io"
```

## 2. Start the server

From the project folder:

```bash
cd /Users/yuhan-zsbth015/ZOGO
pip install -r requirements.txt
python3 server.py
```

Server URL: `http://localhost:8000`

## 3. Open the app

Game:

`http://localhost:8000/Zoho-Logo2.html`

Leaderboard:

`http://localhost:8000/leaderboard.html`

## 4. API checks

Latest results:

`http://localhost:8000/api/results`

Leaderboard ordering:

`http://localhost:8000/api/results?limit=all&sort=leaderboard`

From terminal:

```bash
curl "http://localhost:8000/api/results?limit=20&sort=leaderboard"
```

Clear leaderboard (admin protected):

```bash
curl -X DELETE "http://localhost:8000/api/results" \
  -H "X-Admin-Token: $LEADERBOARD_ADMIN_TOKEN"
```

## 5. Render notes

On Render Web Service:

- Build Command: `pip install -r requirements.txt`
- Start Command: `python server.py`
- Required Env Var: `DATABASE_URL` (use your Render Postgres connection string)
- Recommended Env Vars:
  - `LEADERBOARD_ADMIN_TOKEN` (required to clear leaderboard)
  - `ALLOWED_ORIGINS` (comma-separated origins)
  - `DB_POOL_MIN_SIZE` / `DB_POOL_MAX_SIZE`

## 7. GitHub Pages API base

When the frontend is on GitHub Pages, set the API base once in browser console:

```js
localStorage.setItem('zogoApiBase', 'https://zogo.onrender.com');
```

Or set `window.ZOGO_API_BASE` before the game scripts load.

## 6. Render Blueprint (recommended)

This repo includes `/Users/yuhan-zsbth015/ZOGO/render.yaml` so Render can provision both:

- Web service: `zogo-game`
- PostgreSQL: `zogo-db`

Deploy flow:

1. Push this repository to GitHub.
2. In Render, choose **New +** -> **Blueprint**.
3. Select your repository and apply the blueprint.
4. Render creates the Postgres database and wires `DATABASE_URL` automatically.
