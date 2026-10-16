from pathlib import Path

from cloakbrowser import launch_persistent_context
import facebook_dataset as fd

BASE_DIR = Path(__file__).resolve().parent
PROFILE_DIR = BASE_DIR / "profiles" / "facebook"


def main():
    PROFILE_DIR.parent.mkdir(parents=True, exist_ok=True)
    fd.clean_profile_locks(PROFILE_DIR)

    context = launch_persistent_context(
        str(PROFILE_DIR),
        headless=False,
        args=["--fingerprint=12345"],
    )

    try:
        page = context.new_page()
        page.goto(
            "https://www.facebook.com/",
            wait_until="domcontentloaded",
            timeout=60_000,
        )

        print("Please log into Facebook directly in the browser window.")
        input("Once login is complete, press Enter to save and close...")

    finally:
        context.close()
        fd.clean_profile_locks(PROFILE_DIR)


if __name__ == "__main__":
    main()