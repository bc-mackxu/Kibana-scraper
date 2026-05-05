# Kibana Log Scraper

A tool that scrapes logs from Kibana Discover into a local SQLite database, then lets you browse, filter, AI-classify, and correlate them through a web UI or Claude Desktop (via MCP).

## Features

- **Scrape** Kibana Discover pages into a local SQLite database using Playwright
- **Web UI** — browse logs with full-text search, field-specific filters, date range, pagination, and sorting
- **AI classification** — uses Claude Haiku to mark each log as checkout-relevant/irrelevant, assign severity, and write a one-sentence summary
- **Auto-classify** — propagate classifications from already-analyzed logs to new unanalyzed ones via pattern matching (no API calls needed)
- **Collections** — group multiple Kibana URLs (e.g. Nginx + Syslog) into one logical source and run them together
- **Correlation** — link log records across collections by shared field values (request ID, IP, etc.)
- **Chunked scraping** — split long time ranges into smaller windows to avoid hitting Kibana result limits
- **Scheduling** — optionally auto-run each job on a nightly cron
- **MCP server** — expose log data as tools that Claude Desktop can query and analyze directly

---

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
playwright install chromium
```

### 2. Log in to Kibana

Kibana uses SSO, so you need to save a browser session first. This opens a real Chrome window — complete the SSO login manually:

```bash
python kibana_scraper.py login https://your-kibana-host
```

Your session is saved to `session.json`. Repeat this step whenever the session expires.

### 3. Run the web app

```bash
python app.py
```

The UI opens automatically at **http://localhost:8765**.

---

## Usage

### Web UI

After opening the app:

1. **Add a Job** — paste a Kibana Discover URL and give it a name.  
   The URL can be a full `/discover#...` URL or a `goto/...` short link.

2. **Run the job** — click ▶ to start scraping. A live log stream shows progress.

3. **Browse logs** — click the job to see scraped rows. Use the search box, field filters (`+`/`−` buttons on each cell), date range pickers, and column sort.

4. **AI Classify** — click **Analyze** to send unanalyzed logs to Claude Haiku. Each log gets a relevance label, severity, and one-sentence summary.

5. **Auto-Classify** — once you have some analyzed examples, click **Auto-Classify** to propagate labels to the rest without using any API quota.

### Collections

A *Collection* groups multiple Kibana URLs that represent the same logical system (e.g. an Nginx access log and a PHP error log for the same app):

1. Click **+ Collection** and add sources (label + URL for each).
2. Optionally add *Correlation Keys* — field names (like `request_id`) that appear in both log types so matching records can be linked.
3. Click ▶ to run all sources sequentially.

### Field Filters

Click **+** next to any cell value to add an **include** filter for that exact field/value.  
Click **−** to add an **exclude** filter.  
Filters appear as chips in the toolbar and stack with AND logic.

### Chunked Scraping

Kibana caps results at 10,000 rows per query. For large time ranges, set **Chunk hours** on a job (e.g. `6`) to split the range into 6-hour windows and scrape each one.

---

## API Keys

Keys are stored in `keys.json` (excluded from git) and can be set via the **Settings** panel in the UI:

| Key | Purpose |
|-----|---------|
| `ANTHROPIC_API_KEY` | AI classification via Claude Haiku |
| `GITHUB_TOKEN` | Code search — links log errors to source files in the BigCommerce org |

---

## MCP Server (Claude Desktop)

The MCP server lets Claude Desktop query and analyze your logs directly in a conversation.

**Setup** — add to `~/.claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "kibana-scraper": {
      "command": "python3",
      "args": ["/path/to/kibana-scraper/mcp_server.py"]
    }
  }
}
```

Restart Claude Desktop. Claude can then call these tools:

| Tool | Description |
|------|-------------|
| `list_jobs()` | Show all scrape jobs with row counts |
| `get_stats(job_id?)` | Severity/relevance breakdown |
| `query_logs(job_id, ...)` | Search and filter log rows |
| `fetch_logs_for_analysis(job_id, limit, offset)` | Fetch unanalyzed rows for Claude to classify |
| `save_analysis_results(results)` | Write Claude's classifications back to the DB |
| `get_analysis_summary(job_id)` | List critical/high severity issues with code references |

**Example prompt:**  
> "Fetch 50 unanalyzed logs from job 3, classify them, and save the results."

---

## Project Structure

```
kibana-scraper/
├── app.py              # FastAPI web app + scheduler
├── db.py               # Shared SQLite helpers (qone/qall/exe/init_db)
├── kibana_scraper.py   # Playwright scraper CLI
├── analyzer.py         # Claude Haiku + GitHub code search
├── mcp_server.py       # MCP server for Claude Desktop
├── static/
│   └── index.html      # Single-page web UI
├── requirements.txt
├── .gitignore
└── keys.json           # API keys (not committed)
```

The SQLite database (`kibana_data.db`) is created automatically on first run.

---

## How It Works

1. **Playwright** opens a headless Chromium browser using your saved SSO session and navigates to the Kibana Discover URL.
2. A **network route interceptor** captures every `bsearch` (Elasticsearch bulk search) response, parsing the compressed JSON payload to extract ES hits.
3. Hits are deduplicated by `_id` (`INSERT OR IGNORE`) and stored in **SQLite** with the raw JSON fields preserved.
4. The **web UI** queries SQLite directly — all filtering, sorting, and pagination happens in SQL.
5. **AI classification** sends batches of 10 logs to Claude Haiku in a single API call; results are written back to the same rows.

---

## Requirements

- Python 3.11+
- Chromium (installed by `playwright install chromium`)
- Access to a Kibana instance (SSO login required once)
- Optional: Anthropic API key for AI classification
- Optional: GitHub personal access token for code search
