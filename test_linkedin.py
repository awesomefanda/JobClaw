"""
Run from YOUR terminal: venv\Scripts\python test_linkedin.py

1. Browser opens to LinkedIn
2. You log in manually
3. Press Enter in the terminal
4. Script scrapes hiring posts and prints results
"""
import time, random, re
from playwright.sync_api import sync_playwright

STEALTH_JS = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3]});
Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
window.chrome = {runtime: {}};
"""

ENG_KEYWORDS = ["engineer", "staff", "principal", "software", "backend",
                 "platform", "infrastructure", "data engineer", "sde", "swe"]

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    ctx = browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        viewport={"width": 1280, "height": 800},
        locale="en-US",
    )
    ctx.add_init_script(STEALTH_JS)
    page = ctx.new_page()

    print("Opening LinkedIn...")
    page.goto("https://www.linkedin.com/login", timeout=30000)
    print(">>> Log in to LinkedIn in the browser. Waiting up to 2 minutes...")
    deadline = time.time() + 120
    while time.time() < deadline:
        if "/feed" in page.url or "/mynetwork" in page.url:
            print("Logged in detected!")
            break
        time.sleep(1)
    else:
        print("Warning: login not detected, continuing anyway...")

    # Save session for future runs
    ctx.storage_state(path="data/linkedin_session.json")
    print("Session saved.")

    url = (
        "https://www.linkedin.com/search/results/content/"
        "?keywords=%23hiring&origin=CLUSTER_EXPANSION"
        "&datePosted=%5B%22past-week%22%5D"
    )
    print("Loading hiring posts from past week...")
    page.goto(url, timeout=30000)
    time.sleep(4)

    for _ in range(4):
        page.evaluate("window.scrollBy(0, 800)")
        time.sleep(random.uniform(1.5, 2.5))

    raw = page.inner_text("body")
    blocks = raw.split("Feed post\n")
    print(f"\nTotal posts on page: {len(blocks) - 1}")
    print("=" * 60)

    shown = 0
    for block in blocks[1:]:
        tl = block.lower()
        if not any(kw in tl for kw in ENG_KEYWORDS):
            continue
        urls = re.findall(r'https://www\.linkedin\.com/posts/[^\s\n]+', block)
        post_url = urls[0].split("?")[0] if urls else "—"
        print(f"\n--- Match {shown+1} ---")
        print(block[:600].strip())
        print(f"URL: {post_url}")
        shown += 1
        if shown >= 5:
            break

    print(f"\nFound {shown} engineering hiring posts.")
    print("\nDone. Closing browser in 5 seconds...")
    time.sleep(5)
    browser.close()
