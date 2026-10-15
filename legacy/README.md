# Legacy & Prototype Scripts

This directory preserves the initial prototype scripts and foundational experiments created at the beginning of the project. They serve as historical milestones for how the Playwright setup and Facebook data extraction were verified before the production architecture was built.

---

### File Inventory & Milestones

| Script | Purpose & Description | Replaced By |
| :--- | :--- | :--- |
| [`demo.py`](file:///Users/dipedetran/Documents/GW/GW_ARL/SocialListening/playwright-demo/legacy/demo.py) | Initial Playwright verification test verifying headless/headed Chromium launch against `playwright.dev`. | Core Playwright & CloakBrowser setup |
| [`cloak_smoke_test.py`](file:///Users/dipedetran/Documents/GW/GW_ARL/SocialListening/playwright-demo/legacy/cloak_smoke_test.py) | Verification test for the CloakBrowser stealth browser engine connecting to Facebook. | [`cloak_facebook_login.py`](file:///Users/dipedetran/Documents/GW/GW_ARL/SocialListening/playwright-demo/cloak_facebook_login.py) |
| [`facebook_capture-draft.py`](file:///Users/dipedetran/Documents/GW/GW_ARL/SocialListening/playwright-demo/legacy/facebook_capture-draft.py) | Early draft for extracting text elements using Playwright's `storage_state` (`facebook.json`). | [`cloak_facebook_posts.py`](file:///Users/dipedetran/Documents/GW/GW_ARL/SocialListening/playwright-demo/cloak_facebook_posts.py) |
| [`facebook_capture.py`](file:///Users/dipedetran/Documents/GW/GW_ARL/SocialListening/playwright-demo/legacy/facebook_capture.py) | Prototype for capturing full-page Facebook text and generating screenshots. | [`facebook_dataset.py`](file:///Users/dipedetran/Documents/GW/GW_ARL/SocialListening/playwright-demo/facebook_dataset.py) |
| [`facebook_posts.py`](file:///Users/dipedetran/Documents/GW/GW_ARL/SocialListening/playwright-demo/legacy/facebook_posts.py) | First test script attempting post-specific extraction with hardcoded anchor text using standard Playwright. | [`cloak_facebook_posts.py`](file:///Users/dipedetran/Documents/GW/GW_ARL/SocialListening/playwright-demo/cloak_facebook_posts.py) |
| [`facebook_posts_cloak.py`](file:///Users/dipedetran/Documents/GW/GW_ARL/SocialListening/playwright-demo/legacy/facebook_posts_cloak.py) | Early experiment porting post extraction to CloakBrowser before moving to persistent profiles. | [`cloak_facebook_posts_auto.py`](file:///Users/dipedetran/Documents/GW/GW_ARL/SocialListening/playwright-demo/cloak_facebook_posts_auto.py) |

---

### Active Architecture (Project Root)

For active crawling, dataset generation, and maintenance, use the root-level scripts:
1. **Interactive Login / Session Refresh**: [`cloak_facebook_login.py`](file:///Users/dipedetran/Documents/GW/GW_ARL/SocialListening/playwright-demo/cloak_facebook_login.py)
2. **URL Discovery Tool**: [`cloak_facebook_discover.py`](file:///Users/dipedetran/Documents/GW/GW_ARL/SocialListening/playwright-demo/cloak_facebook_discover.py)
3. **Core Dataset & Extraction Engine**: [`facebook_dataset.py`](file:///Users/dipedetran/Documents/GW/GW_ARL/SocialListening/playwright-demo/facebook_dataset.py)
4. **Semi-Automated Crawler**: [`cloak_facebook_posts.py`](file:///Users/dipedetran/Documents/GW/GW_ARL/SocialListening/playwright-demo/cloak_facebook_posts.py)
5. **Fully Automated Crawler**: [`cloak_facebook_posts_auto.py`](file:///Users/dipedetran/Documents/GW/GW_ARL/SocialListening/playwright-demo/cloak_facebook_posts_auto.py)
