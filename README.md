# 🦀 JobClaw — Job Intelligence & Outreach Pipeline

Drop your resume. Get an Excel report with ranked leads, contacts, salary data, outreach drafts, and everything you need to land your next role.

## Quick Start (Zero Config)

```bash
pip install -r requirements.txt
playwright install chromium    # for Google Careers scraping
cp .env.example .env           # add GROQ_API_KEY (free at console.groq.com)
cp ~/Downloads/resume.pdf .    # drop your resume (PDF or DOCX)
python run.py                  # → open data/reports/*.xlsx
```

No JSON to edit. No forms. JobClaw reads your resume, infers your level, what titles to search, who hires people like you, and what to look for on Blind. Works for ICs (Staff, Principal) and managers (Director, VP) equally.

---

## The Algorithm

### Phase 0: BOOTSTRAP (automatic, one Groq call)

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

### Phase 1: DISCOVER (10 sources, zero AI cost)

Inspired by Andrej Karpathy's job scraping methodology: resilient Playwright-based scraping with local HTML caching for offline iteration and LLM analysis.

| # | Source | Method | What It Finds |
|---|--------|--------|---------------|
| 1 | **JobSpy** | Indeed, LinkedIn, Glassdoor, ZipRecruiter | Broadest coverage — 100 results per platform per term |
| 1b | **Google Careers** | Playwright scraper (Karpathy-inspired) | Direct from careers.google.com — resilient to bot detection |
| 2 | **Greenhouse** | Public JSON API (`boards-api.greenhouse.io`) | Direct career pages, no rate limit |
| 3 | **Lever** | Public JSON API (`api.lever.co`) | Same — fast, direct |
| 4 | **YC Work at a Startup** | Google fallback | YC-backed startups, founders reachable |
| 5 | **HN Who's Hiring** | Algolia API + Firebase | Founder posts with **direct emails** extracted |
| 6 | **Wellfound** | Google (`site:wellfound.com`) | Startup jobs with equity + funding data |
| 7 | **LinkedIn Hiring Posts** ⭐ | Google (`site:linkedin.com/posts`) | People above your level saying "I'm hiring" — **named contacts** |
| 8 | **Blind Offer Feed** ⭐ | Google (`site:teamblind.com`) | "X vs Y vs Z" posts → companies **confirmed closing** candidates |
| 9 | **Levels.fyi** | Public job listings | Level-aware roles with salary data attached |

**Source 7 is special:** It searches for posts by people who HIRE at your level. If you're Staff, it finds Directors/VPs posting "building my team." Each post = a lead with the person's name, even if no job listing exists.

**Source 8 is special:** "Airbnb vs Google vs Netflix E5 MLE" on Blind means all 3 companies gave offers this week. That's stronger proof of active hiring than any job listing.

**Source 9 is special:** Levels.fyi listings are tagged by level (E5, L6, Staff, etc.) and come with real compensation data — base, equity, bonus — scraped from the same page as the job.

### Phase 2: SCORE (Groq — only AI cost)

```
For each unscored job in CSV:
  • Quick keyword exclude filter (zero cost)
  • Groq scores JD vs resume → fit_score + outreach draft
  • Keep only matches ≥ 0.75
  • Track scored IDs to never re-score
```

### Phase 3a: SIGNALS (web intelligence, zero cost)

```
For each scored company:
  • LinkedIn hiring posts       (Google search)
  • Blind offer data            (Google search)
  • Blind sentiment / PIP       (Google search)
  • Layoff check                (layoffs.fyi via Google)
  • Levels.fyi salary + offers  (public .md endpoint + Google)
  • Funding signals             (Google search)

Results cached in signals_cache.json — re-runs skip already-enriched companies.
```

### Phase 3b: CONTACTS & RANKING

```
For each match:
  • Apollo People Search (FREE) → HM name + LinkedIn URL
  • Connections CSV → people you know at the company
  • Field leads (resume-driven) → engineers in your tech field who could refer:
      - GitHub contributors to repos in your stack
      - dev.to authors writing about your technologies
      - Stack Overflow top answerers in your tags
      - Hacker News users active in your domain
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

4 sheets: **Job Matches** (ranked) · **Hiring Posts** · **Salary Data** · **Pipeline Tracker**

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

Groq infers your `blind_level_terms` from your resume on first run. You can review and edit them in `data/parsed_resume.json → blind_level_terms`. The more accurate these are, the better the Blind offer signal ("PMTS vs E6 vs L7 offer comparison" confirms Oracle, Google, and Amazon are all closing candidates at your level this week).

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
# APOLLO_API_KEY=     ← optional (free at app.apollo.io)
# GITHUB_TOKEN=       ← optional (raises GitHub rate limit: 60 → 5000 req/hr)
```

### 3. Resume (pick one)
```bash
cp ~/resume.pdf .       # Just drop it — auto-parsed
# OR: cp ~/resume.docx .
# OR: edit resume.json for full control
```

### 4. Connections (optional)
LinkedIn → Settings → Data Privacy → Get a copy of your data → Connections → save as `connections.csv`

### 5. Run
```bash
python run.py                   # Full pipeline
python run.py --scout-only      # Just find new jobs
python run.py --score-only      # Just score
python run.py --report-only     # Regenerate report
```

### 6. Continuous (optional)
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
- `scout.max_results` — increase up to 1000 (Indeed unlimited, LinkedIn rate-limited)
- `scout.hours_old` — 168 (7 days) or 336 (2 weeks)
- `hm_titles_above_me` — titles of people who hire at your level

## Cost: $0

| Service | Cost |
|---------|------|
| Groq (scoring + resume parse) | Free (30 req/min) |
| Apollo People Search | Free (no credits consumed) |
| JobSpy, Greenhouse, Lever, HN API | Free (open source / public) |
| Levels.fyi (jobs + salary + offers) | Free (public endpoints) |
| Google Search, Blind | Free |
| Playwright (Google Careers) | Free (open source) |

## Resilience

Every source in try/except. If one fails, log and continue. Pipeline never crashes.
Check `data/jobclaw.log` for details.

## License

MIT
