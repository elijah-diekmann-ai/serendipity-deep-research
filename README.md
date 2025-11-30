```md
```
# Serendipity Deep Research
A deep research platform with a FastAPI backend and Next.js frontend that produces investor‑grade company briefs.
```
```
## 1. Quick Start (Local)

### Prereqs

- Docker installed
- API Keys I provided (OpenAI, OpenRouter, Exa, People Data Labs, etc.)

### Steps

1. **Clone the repo**

   ```bash
   git clone <https://github.com/elijah-diekmann-ai/serendipity-deep-research>
   cd serendipity-deep-research

   ```
2. **Create your backend `.env` file**

   Copy the example .env and fill in the values I shared:

   ```bash
   cp backend/.env.example backend/.env
   ```

   Then open `backend/.env` and:

   * Paste in the API keys
   * Save the file.

3. **Run everything with Docker Compose**

   ```bash
   docker-compose up --build
   ```

   Once it’s up:
   
   * **Frontend UI:** [http://localhost:3000](http://localhost:3000)
   
   To prompt, ideally just add the company name followed by their website in brackets.
   
   **Example:**
   > Stripe (stripe.com)

---

## 2. Connector Matrix by Brief Section

This is how each brief section pulls data today, and which connectors are already implemented but currently disabled (missing API key / unpaid).

| Brief Section             | 🟢 Active Connectors (Currently Enabled) | ⚪ Implemented Connectors (Ready but .env unconfigured/Unpaid) |
| ------------------------- | ---------------------------------------- | ------------------------------------------------------------- |
| `executive_summary`       | OpenAI, GLEIF, People Data Labs, Exa     | PitchBook, Apollo, Companies House, OpenCorporates            |
| `founding_details`        | GLEIF, Exa                               | Companies House, OpenCorporates                               |
| `founders_and_leadership` | People Data Labs                         | Apollo                                                        |
| `fundraising`             | Exa                                      | PitchBook                                                     |
| `product`                 | Exa                                      | (None – strictly Exa)                                         |
| `technology`              | Exa                                      | (None – strictly Exa)                                         |
| `competitors`             | OpenAI                                   | (None – strictly OpenAI)                                      |
| `recent_news`             | Exa                                      | (None – strictly Exa)                                         |

* **Implemented** = code is wired up; enabling them is just a matter of adding API keys (and in PitchBook’s case, a paid subscription).

---

## 3. How it works (architecture / pipeline)

At a high level, one research run looks like this:

```text
Frontend → API → Orchestrator → Planner → Connectors → EntityResolution → Writer → Brief
```

1. **Orchestrator (Celery worker)**

   * Picks up a new `ResearchJob` from the queue.
   * Owns the overall lifecycle: plan → fetch → resolve → write → mark job as completed/failed.

2. **Planner**

   * Given the target company (name, website, context), builds a **deterministic research plan**:

     * Which connectors to call (Exa, GLEIF, People Data Labs, OpenAI web, etc.).
     * What each one should fetch (site crawl, funding, news, competitors, people).

3. **Connectors**

   * Execute the plan in parallel and normalise everything into a common shape:

     * Web + news snippets
     * Registry / LEI data
     * People / leadership data
     * (Optionally) funding rounds, competitors, etc.

4. **EntityResolution**

   * Merges all raw connector outputs into a single **knowledge graph**:

     * One canonical company.
     * A cleaned-up set of people (founders & leadership).
     * Attached evidence snippets and structured competitors/funding where available.

5. **Writer**

   * Pulls from the knowledge graph + stored sources.
   * For each brief section:

     * Selects the right connectors (per the table above).
     * Builds a compact context + source bundle.
     * Calls the LLM to draft that section with `[S<ID>]` citations.
   * Stores the final JSON brief (all sections + citations) back in the database.

The frontend just:

* **Creates** a job (`POST /api/research`),
* **Polls** for status + brief (`GET /api/research/{job_id}`),
* And renders the finished brief once the pipeline above has completed.

---

## 4. Implemented Connectors (Overview)

All connectors live under `backend/app/services/connectors/`.
Some require API keys to be active; others (like GLEIF) work unauthenticated without a key.

* **Exa (`exa.py`)**
  Deep web search and site crawling.

  * Used for: company website crawl, funding/news, technical evidence, recent news.
  * Returns high‑density snippets with highlights + `published_date`.

* **OpenAI Web Search (`openai_web.py`)**
  Reasoning‑heavy web search via OpenAI’s `web_search` tool.

  * Used primarily for: **competitor discovery** as of now.
  * Produces a structured `competitors` list plus supporting snippets.

* **People Data Labs (`pdl.py`)**
  Primary **people / leadership** discovery & enrichment.

  * Used for: `founders_and_leadership` section and richer biographies.
  * Works via Person Search + (optional) Person Enrichment.

* **GLEIF (`gleif.py`)**
  Global LEI registry lookup.

  * Used for: `founding_details` (legal name, LEI, jurisdiction, registration authority, addresses).
  * No API key required; configured via base URL.

* **Companies House (`companies_house.py`)**
  UK company registry.

  * Provides: company profile, officers, and accounting filings.
  * Only used if `COMPANIES_HOUSE_API_KEY` is set (still trying...)

* **OpenCorporates (`opencorporates.py`)**
  Global corporate registry.

  * Provides: company snapshot (identifiers, status, incorporation date, registered address, filings).
  * Used as an additional authority for `founding_details` when `OPENCORPORATES_API_TOKEN` is configured (still trying...).

* **Apollo (`apollo.py`)**
  Optional people & firmographics connector.

  * Provides: leadership discovery and org firmographics for companies on paid Apollo plans.
  * Currently treated as **implemented but not enabled** by default (also they have no free people search tier).

* **PitchBook (`pitchbook.py`)**
  Funding / deal data connector.

  * Skeleton implementation: structure is in place (`funding_rounds` + `snippets`), but the actual API calls are intentionally stubbed.
  * Easy to be enabled once a PitchBook subscription + API key are available.

---

## 5. Repo Layout (Very Short)

* `backend/` – FastAPI app, Celery worker, connectors, planner, writer, models, migrations.
* `frontend/` – Next.js UI (research form, job status, archive views).
* `docker-compose.yml` – Postgres, Redis, backend, worker, frontend.
* `README.md` – this file.

```
