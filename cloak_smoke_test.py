from cloakbrowser import launch


def main():
    browser = launch(headless=False)

    try:
        page = browser.new_page()
        page.goto(
            "https://www.facebook.com/",
            wait_until="domcontentloaded",
            timeout=60_000,
        )

        print("Page Title:", page.title())
        input("Press Enter to close browser...")

    finally:
        browser.close()


if __name__ == "__main__":
    main()