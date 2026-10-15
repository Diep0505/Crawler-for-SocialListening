import json
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright

output = Path("output")
output.mkdir(exist_ok=True)

with sync_playwright() as p:
    # Launch browser in headed mode for observation
    browser = p.chromium.launch(headless=False)
    page = browser.new_page()

    try:
        page.goto(
            "https://playwright.dev/python/docs/intro",
            wait_until="domcontentloaded",
            timeout=60000,
        )

        # Wait for heading before reading
        heading = page.get_by_role(
            "heading", name="Installation", exact=True
        )
        heading.wait_for(state="visible")

        data = {
            "url": page.url,
            "title": page.title(),
            "heading": heading.inner_text(),
            "collected_at": datetime.now(timezone.utc).isoformat(),
        }

        (output / "data.json").write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        page.screenshot(
            path=str(output / "screenshot.png"),
            full_page=True,
        )

        print(json.dumps(data, ensure_ascii=False, indent=2))
        print("Data saved in output folder.")

        input("Press Enter in Terminal to close browser...")

    finally:
        browser.close()