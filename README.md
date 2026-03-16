# 🦀 JobClaw — Job Intelligence & Outreach Pipeline

Drop your resume. Get an Excel report with ranked leads, contacts, salary data, outreach drafts, and everything you need to land your next role.

## Quick Start (Zero Config)

```bash
pip install -r requirements.txt
playwright install chromium      # for Google Careers + LinkedIn scraping
cp .env.example .env
# Recommended: add GEMINI_API_KEY (free at aistudio.google.com) — primary scorer
# Required fallback: add GROQ_API_KEY (free at console.groq.com)
cp ~/Downloads/resume.pdf .local/   # personal files live in .local/ (gitignored)

# One-time LinkedIn login (use a burner account — see LinkedIn Setup below)
python test_linkedin.py          # browser opens → log in → session saved automatically

python run.py                    # → open data/reports/*.xlsx
```

No JSON to edit. No forms. JobClaw reads your resume, infers your level, what titles to search, who hires people like you, and what to look for on Blind. Works for ICs (Staff, Principal) and managers (Director, VP) equally.

---

## The Algorithm

### Phase 0: BOOTSTRAP (automatic, one LLM call)

```
User drops: resume.pdf

JobClaw infers EVERYTHING from the resume:
  • Level: senior / staff / principal / director / VP
  • Track: IC or management
  • Target roles: 5-7 titles at your level + one step up
  • Search keywords and exclusions
  • Who hires you: "Director of Eng, VP Eng" (for Staff/Principal)
                   "VP, SVP, CTO" (for Director)
  • Blind level codes: E5, L6, IC5, Staff, Principal, etc.

Cached at data/parsed_resume.json — edit if you want, but you don't have to.
```

### Phase 1: DISCOVER (10 sources, zero AI cost, fully parallel)

All 10 sources run concurrently (5 workers). Total scout time ~same as the slowest single source.

| # | Source | Method | What It Finds |
|---|--------|--------|---------------|
| 1 | **JobSpy** | Indeed, LinkedIn, Glassdoor, ZipRecruiter | Broadest coverage — 100 results per platform per term |
| 1b | **Google Careers** | Playwright scraper | Direct from careers.google.com — resilient to bot detection |
| 2 | **Greenhouse** | Public JSON API (`boards-api.greenhouse.io`) | Direct career pages, no rate limit |
| 3 | **Lever** | Public JSON API (`api.lever.co`) | Same — fast, direct |
| 3b | **Ashby** | Public GraphQL API (`jobs.ashbyhq.com`) | 30+ hot startups pre-loaded: Linear, Vercel, Retool, Rippling, Notion, OpenAI, Anthropic, Supabase, Ramp, Brex… |
| 4 | **YC Work at a Startup** | Google fallback | YC-backed startups, founders reachable |
| 5 | **HN Who's Hiring** | Algolia API + Firebase | Founder posts with **direct emails** extracted |
| 6 | **Wellfound** | Google (`site:wellfound.com`) | Startup jobs with equity + funding data |
| 7 | **LinkedIn Hiring Posts** ⭐ | Playwright (real scraping, saved session) | People above your level saying "I'm hiring" — **named contacts** |
| 8 | **Blind Offer Feed** ⭐ | Google (`site:teamblind.com`) | "X vs Y vs Z" posts → companies **confirmed closing** candidates |
| 9 | **Levels.fyi** | Public job listings | Level-aware roles with salary data attached |

**Source 7 is special:** It searches for posts by people who HIRE at your level. If you're Staff, it finds Directors/VPs posting "building my team." Each post = a lead with the person's name, even if no job listing exists.

**Source 8 is special:** "Airbnb vs Google vs Netflix E5 MLE" on Blind means all 3 companies gave offers this week. That's stronger proof of active hiring than any job listing.

**Source 9 is special:** Levels.fyi listings are tagged by level (E5, L6, Staff, etc.) and come with real compensation data — base, equity, bonus — scraped from the same page as the job.

### Phase 2: SCORE (LLM scoring — Gemini first, Groq fallback)

```
For each unscored job in CSV:
  • Quick keyword exclude filter (zero cost)
  • LLM scores JD vs resume → fit_score + outreach draft
  • Keep only matches ≥ 0.75
  • Track scored IDs to never re-score

Model fallback chain (automatic, if GEMINI_API_KEY is set):
  1. Gemini 2.0 Flash               (1500 req/day, 1M tokens/min — recommended first)
  2. Groq llama-3.3-70b-versatile  (100k tokens/day)
  3. Groq llama-3.1-8b-instant      (500k tokens/day)

Without GEMINI_API_KEY: starts at step 2.
```

### Phase 3a: SIGNALS (web intelligence, zero cost)

```
Step A — bulk pre-fetch (done once per run):
  • LinkedIn hiring posts → Playwright scrapes #hiring posts (6 searches, ~3 min)
  • Blind offer pool      → scrape recent offer posts, index by company name
  • Layoffs pool          → scrape layoffs.fyi + tech news, build company set

Step B — per-company enrichment (3 parallel workers):
  • LinkedIn hiring posts   pool lookup (instant — from Playwright scrape above)
  • Blind offer data        pool lookup (instant, no extra search)
  • Blind sentiment / PIP   2 targeted searches
  • Layoff check            pool lookup (instant)
  • Levels.fyi salary       structured endpoint + DDG search fallback
  • Levels.fyi offers       recent offer submissions (proxy for active interviewing)
  • Funding signals         1 targeted search

Results cached in signals_cache.json — re-runs skip already-enriched companies.
~5-6 min for 85 companies (vs ~30 min sequential).
```

### Phase 3b: CONTACTS & RANKING

```
Setup (parallel):
  • Apollo pre-fetched for all companies at once (3 workers)
  • Field leads fetched in parallel: GitHub + dev.to + Stack Overflow + HN (4 workers)
    - GitHub: contributors to repos in your stack (profiles enriched concurrently)
    - dev.to: popular authors writing about your technologies
    - Stack Overflow: top answerers in your tags
    - Hacker News: active users discussing your domain

For each match:
  • Apollo People Search (FREE) → HM name + LinkedIn URL  (pre-fetched, instant)
  • Connections CSV → people you know at the company       (in-memory, instant)
  • Field leads → same-field engineers who could refer     (cached after first run)
  • Cross-reference all sources

Best contact priority:
  your_connection > hiring_post_author > apollo_contact > founder_email > field_lead

action_score = fit_score
  + 0.25 if founder email available (HN/YC)
  + 0.20 if you have a connection
  + 0.20 if source IS a hiring post (named person)
  + 0.15 if Blind confirms company closing candidates
  + 0.15 if hiring post found for company
  + 0.12 if Levels.fyi offer submissions found
  + 0.10 if hiring post author is your connection (GOLDEN)
  + 0.10 if Apollo found HM
  + 0.08 if field leads found (engineers in same field)
  + 0.05 if recently funded
  + 0.05 if no layoffs
  - 0.10 if Blind red flags
  - 0.15 if recent layoffs

Track A (warm): connection OR hiring post OR Apollo OR founder email
Track B (cold): no warm path — tailored resume required
```

### Phase 5: EXCEL REPORT

5 sheets: **Job Matches** (ranked) · **Hiring Posts** · **Salary Data** · **Pipeline Tracker** · **🔔 Alerts**

The **Alerts** tab surfaces only the jobs worth acting on immediately — high action score, active hiring signal, recent funding, or a warm connection — sorted by urgency.

**Salary Data tab** shows per company: TC range, base range (from levels.fyi structured endpoint + search fallback), **Recent Submissions** (people who recently submitted offers to levels.fyi — proof the company is actively closing candidates), Blind offers, and funding news.

---

## Lead Quality Ranking

```
GOLDEN:  Connection at company + hiring post found + Blind confirms hiring
TIER 1:  LinkedIn hiring post from manager + connection
TIER 2:  HN post with founder email
TIER 3:  Blind confirmed hiring + Apollo HM
TIER 4:  Job listing + connection
TIER 5:  Job listing + Apollo HM
TIER 6:  Job listing alone (Track B)
```

## Level-Aware Behavior

| Your Level | Searches for HM posts by | Blind level codes | Platforms |
|---|---|---|---|
| Senior Engineer | Managers, Directors | E4, L5, SDE2 | All 6 platforms |
| Staff / Principal | Directors, VPs | E5, E6, L6, L7, Staff | All 6 platforms + HN |
| Director | VPs, SVPs, CTOs | Director, D1, D2 | LinkedIn-heavy |
| VP+ | CEOs, board posts | VP, SVP | LinkedIn-heavy |

### Cross-Company Level Mapping (via Levels.fyi)

Your title at one company rarely maps 1:1 to another. JobClaw uses Levels.fyi's level equivalency data to search Blind correctly regardless of where you work:

| Your title | Your company | Equivalent levels searched on Blind/Levels.fyi |
|---|---|---|
| PMTS | Oracle | E5, E6, L6, Staff, Principal |
| Staff Engineer | Google | E6, L6, Staff |
| Principal Engineer | Amazon | L7, E7, Principal |
| Senior Engineer II | Microsoft | E5, L5, Senior2 |
| Staff Engineer | Meta | E6, Staff |

Your `blind_level_terms` are inferred from your resume on first run. You can review and edit them in `data/parsed_resume.json → blind_level_terms`. The more accurate these are, the better the Blind offer signal ("PMTS vs E6 vs L7 offer comparison" confirms Oracle, Google, and Amazon are all closing candidates at your level this week).

---

## Setup

### 1. Install
```bash
git clone https://github.com/yourname/jobclaw.git && cd jobclaw
pip install -r requirements.txt
playwright install chromium  # For Google Careers scraping
```

### 2. API Keys
```bash
cp .env.example .env
# GROQ_API_KEY=       ← required (free at console.groq.com)
# GEMINI_API_KEY=     ← recommended (free at aistudio.google.com — primary scorer, 1M tokens/min)
# APOLLO_API_KEY=     ← optional (free at app.apollo.io)
# GITHUB_TOKEN=       ← optional (raises GitHub rate limit: 60 → 5000 req/hr)
```

### 3. Personal files → `.local/` (gitignored)

Your resume and connections never get committed. Put them in `.local/`:

```bash
mkdir -p .local
cp ~/resume.pdf .local/          # auto-parsed via Groq on first run
# OR: cp ~/resume.docx .local/
# OR: cp ~/resume.json .local/   # full manual control

# Optional: override API keys per-machine
cp .env.example .local/.env      # .local/.env takes priority over .env
```

JobClaw checks `.local/` first, then falls back to the project root.

### 4. LinkedIn Setup (one-time login)

JobClaw scrapes LinkedIn `#hiring` posts using a real browser session — this is what finds named hiring managers posting "I'm building my team."

**Use a burner LinkedIn account** (strongly recommended):
- LinkedIn's ToS prohibits scraping. A suspended burner costs nothing; your main account has your professional reputation on it.
- Create a free LinkedIn account with a different email. Add a profile photo and a few connections to make it look real.

Then log in once from the terminal (run from **cmd.exe**, not Git Bash):
```
python test_linkedin.py
```
A browser opens → log in with the burner account → the script detects the feed automatically and saves the session to `data/linkedin_session.json`. Close the browser. Done.

The session persists across runs (headless from now on). Re-run `test_linkedin.py` only if you see "LinkedIn session expired" in the logs.

### 5. Connections (optional)
LinkedIn → Settings → Data Privacy → Get a copy of your data → Connections → save as `.local/connections.csv`

### 6. Run
```bash
python run.py                   # Full pipeline
python run.py --scout-only      # Just find new jobs
python run.py --score-only      # Just score
python run.py --report-only     # Regenerate report
```

### 7. Continuous (optional)
```bash
nohup ./exec_loop.sh > data/loop.log 2>&1 &   # Linux/Mac
# Windows: Task Scheduler → python run.py hourly
```

## Incremental Runs

Run it daily or every few days — it only does new work:

| Phase | Day 1 | Day 3 |
|---|---|---|
| **Scout** | Scrapes all sources | JobSpy fetches only jobs posted in last `hours_old` (default 72h). Greenhouse/Lever/HN re-scrape but dedup by ID — already-seen jobs are skipped. |
| **Scorer** | Scores all new jobs | Only scores jobs not in `scored_ids.txt`. Day 1 jobs never re-scored. |
| **Signals** | Fetches web intel for all companies | Skips companies already in `signals_cache.json`. Only new companies from day 3 are fetched. |
| **Contacts** | Finds contacts for all matches | Re-runs Apollo/connections for all (fast). Field leads reused from `field_leads.json`. |
| **Report** | Generated | Regenerated from all accumulated data. |

**On `date_posted`:** JobSpy passes through the posting date from Indeed/LinkedIn/Glassdoor. Most other sources (Greenhouse, Lever, HN posts, LinkedIn hiring posts, Blind) don't expose a reliable post date — those fields are left blank. The `hours_old` filter in `data/parsed_resume.json → scout.hours_old` controls how far back JobSpy looks (default: 72h).

**To force a full re-run** of a phase, delete the relevant cache file:
```bash
rm data/scored_ids.txt      # re-score everything
rm data/signals_cache.json  # re-fetch all signals
rm data/field_leads.json    # re-fetch GitHub/dev.to/SO leads
```

## After First Run

Review `data/parsed_resume.json` and optionally edit:
- `target_roles` — add/remove titles
- `scout.greenhouse_boards` — e.g. `["databricks", "stripe", "cloudflare"]`
- `scout.ashby_boards` — extra Ashby slugs beyond the 30 pre-loaded defaults
- `scout.max_results` — increase up to 1000 (Indeed unlimited, LinkedIn rate-limited)
- `scout.hours_old` — 168 (7 days) or 336 (2 weeks)
- `hm_titles_above_me` — titles of people who hire at your level

## Cost: $0

| Service | Cost |
|---------|------|
| Gemini 2.0 Flash (scoring — primary if key set) | Free (1500 req/day, 1M tokens/min via Google AI Studio) |
| Groq llama-3.3-70b (scoring — primary without Gemini) | Free (100k tokens/day) |
| Groq llama-3.1-8b-instant (auto fallback) | Free (500k tokens/day) |
| Apollo People Search | Free (no credits consumed) |
| JobSpy, Greenhouse, Lever, HN API | Free (open source / public) |
| Levels.fyi (jobs + salary + offers) | Free (public endpoints) |
| Google Search, Blind | Free |
| Playwright (Google Careers + LinkedIn scraping) | Free (open source) |

## Resilience

Every source in try/except. If one fails, log and continue. Pipeline never crashes.
Check `logs/` for details.

## License

MIT

---

*Inspired in part by [job_market_intelligence_bot](https://github.com/MariyaSha/job_market_intelligence_bot) by MariyaSha.*
