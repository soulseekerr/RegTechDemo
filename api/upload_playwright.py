# from pathlib import Path
# from datetime import datetime
# from playwright.sync_api import sync_playwright
# import os, json

# BASE_URL = os.getenv("PORTAL_URL", "http://localhost:8000/portal")
# ART = Path("evidence"); ART.mkdir(exist_ok=True)

# def rpa_submit(csv_path: str) -> dict:
#     user, pw = os.getenv("PORTAL_USER","user"), os.getenv("PORTAL_PASS","pass")
#     ts = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
#     shots = []
#     with sync_playwright() as p:
#         b = p.chromium.launch(headless=True); page = b.new_page()
#         page.goto(f"{BASE_URL}/login")
#         page.fill("#user", user); page.fill("#pass", pw); page.click("text=Sign in")
#         page.get_by_text("Upload Filing").wait_for()
#         s1 = ART / f"evidence_{ts}_login.png"; page.screenshot(path=s1); shots.append(str(s1))
#         page.set_input_files("input[type=file]", csv_path)
#         page.click("#submit-btn")
#         page.get_by_text("Submission Success").wait_for()
#         s2 = ART / f"evidence_{ts}_submitted.png"; page.screenshot(path=s2); shots.append(str(s2))
#         b.close()
#     manifest = ART / f"run_manifest_{ts}.json"
#     manifest.write_text(json.dumps({"ts": ts, "csv": csv_path, "evidence": shots}, indent=2))
#     return {"manifest": str(manifest), "evidence": shots}

# rpa/upload_playwright.py
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any
from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout
import os, json, time

DEFAULT_BASE_URL = os.getenv("PORTAL_URL", "http://localhost:8000/portal")
DEFAULT_USER     = os.getenv("PORTAL_USER", "user")
DEFAULT_PASS     = os.getenv("PORTAL_PASS", "pass")
EVID_DIR         = Path(os.getenv("EVIDENCE_DIR", "data/evidence_runs"))
EVID_DIR.mkdir(parents=True, exist_ok=True)

# Optional: map container evidence dir to a host-visible dir for convenience
HOST_EVIDENCE_DIR = os.getenv("HOST_EVIDENCE_DIR")  # e.g. set to ./evidence

def _host_mirror(path: Path) -> Optional[str]:
    """Return a host-visible path if HOST_EVIDENCE_DIR is set and EVID_DIR is mounted."""
    if not HOST_EVIDENCE_DIR:
        return None
    try:
        rel = path.relative_to(EVID_DIR)  # path under evidence dir?
        return str(Path(HOST_EVIDENCE_DIR) / rel)
    except ValueError:
        return None

def rpa_submit(
    file_path: str,
    base_url: str = DEFAULT_BASE_URL,
    username: str = DEFAULT_USER,
    password: str = DEFAULT_PASS,
    # CSS/text selectors (adjust if your portal differs)
    sel_user: str = "#user",
    sel_pass: str = "#pass",
    sel_signin: str = "text=Sign in",
    sel_upload_input: str = "input[type=file]",
    sel_submit_btn: str = "#submit-btn",
    sel_success_text: str = "text=Submission Success",
    headless: bool = True,
    timeout_ms: int = 15_000,
    retries: int = 2,
) -> Dict[str, Any]:
    """
    Automates portal upload with screenshots and a JSON manifest.

    Returns:
        {"manifest": str, "evidence": [str, ...], "ok": bool}
    """
    ts = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
    run_dir = EVID_DIR / f"rpa_{ts}"
    run_dir.mkdir(parents=True, exist_ok=True)
    man_path = run_dir / "rpa_manifest.json"
    screenshots: list[str] = []
    host_screenshots: list[str] = []  # optional mirror

    def shot(name: str):
        p = run_dir / f"{name}.png"
        page.screenshot(path=p)
        screenshots.append(str(p))
        host = _host_mirror(p)
        if host:
            host_screenshots.append(host)

    last_err = None

    for attempt in range(1, retries + 1):
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=headless,
                    args=["--no-sandbox", "--disable-dev-shm-usage"]
                )
                ctx = browser.new_context(accept_downloads=True)
                global page
                page = ctx.new_page()

                # Login
                page.goto(f"{base_url}/login", timeout=timeout_ms)
                page.fill(sel_user, username, timeout=timeout_ms)
                page.fill(sel_pass, password, timeout=timeout_ms)
                page.click(sel_signin, timeout=timeout_ms)
                page.wait_for_selector("text=Upload", timeout=timeout_ms)
                shot("01_after_login")

                # Upload
                page.set_input_files(sel_upload_input, file_path)
                shot("02_after_attach")
                page.click(sel_submit_btn, timeout=timeout_ms)

                # 3) Wait for success and capture
                page.wait_for_selector(sel_success_text, timeout=timeout_ms)
                shot("03_after_submit_success")

                browser.close()

            # Write manifest (success)
            man = {
                "timestamp": ts,
                "base_url": base_url,
                "file": file_path,
                "screenshots": screenshots,               # absolute container paths
                "host_screenshots": host_screenshots or None,  # optional, host paths
                "status": "succeeded",
                "attempts": attempt,
            }
            man_path.write_text(json.dumps(man, indent=2))
            return {"manifest": str(man_path), "evidence": screenshots, "ok": True}

        except (PwTimeout, Exception) as e:
            last_err = str(e)
            # Best-effort screenshot if page exists
            try:
                shot(f"err_attempt_{attempt}")
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass
            if attempt < retries:
                time.sleep(1.0 * attempt)  # simple backoff

    # Write manifest (failure)
    man = {
        "timestamp": ts,
        "base_url": base_url,
        "file": file_path,
        "screenshots": screenshots,
        "host_screenshots": host_screenshots or None,
        "status": "failed",
        "error": last_err,
        "attempts": retries,
    }
    man_path.write_text(json.dumps(man, indent=2))
    return {"manifest": str(man_path), "evidence": screenshots, "ok": False}
