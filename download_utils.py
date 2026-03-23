"""
Download Helper — Robust Euronext Excel retrieval with validation.

Strategy:
  1. Try direct HTTP download (multiple endpoint variants)
  2. Validate response is a real .xlsx (ZIP magic bytes + content-type check)
  3. If direct fails → automatic Playwright browser-based export fallback
  4. Validate Playwright-downloaded file the same way
  5. Only return a filepath if the file is a verified .xlsx
"""

import os
import shutil
import time
import glob
import datetime
import logging
import tempfile

import requests

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════════════════

DATA_DIR = "data"
LATEST_FILE = os.path.join(DATA_DIR, "latest_equities.xlsx")
ARCHIVE_DIR = os.path.join(DATA_DIR, "archive")

# ZIP magic bytes — every .xlsx starts with these (it's a ZIP container)
ZIP_MAGIC = b"PK"

# Minimum file size for a plausible Euronext export (~50KB minimum)
MIN_FILE_SIZE = 20_000

# Direct download endpoints (Euronext internal data API)
DIRECT_URLS = [
    (
        "https://live.euronext.com/en/pd/data/stocks"
        "?mics=XAMS,XBRU,XDUB,XLIS,XPAR,XMIL,XOSL,ALXB,ALXL,ALXP,ENXB,ENXL,ENXM,VPXB,MLXB,TNLA,TNLB,EXGM,BGEM,MTAH,ETLX"
        "&op=&tp=&export=true"
    ),
    (
        "https://live.euronext.com/en/pd/data/stocks"
        "?mics=XAMS,XBRU,XDUB,XLIS,XPAR,XMIL,XOSL,ALXB,ALXL,ALXP,ENXB,ENXL,ENXM,VPXB"
        "&op=&tp=&export=true"
    ),
    # CSV variant that some endpoints return — we only want xlsx
    (
        "https://live.euronext.com/en/pd/data/stocks"
        "?mics=XAMS,XBRU,XDUB,XLIS,XPAR,XMIL,XOSL"
        "&op=&tp=&export=true"
    ),
]

# The page users browse (used by Playwright)
EURONEXT_PAGE_URL = "https://live.euronext.com/en/products/equities/list"

# HTTP headers mimicking a real browser
HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet, "
        "application/vnd.ms-excel, "
        "application/octet-stream, */*"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://live.euronext.com/en/products/equities/list",
}


# ═══════════════════════════════════════════════════════════════════════════
# Validation
# ═══════════════════════════════════════════════════════════════════════════

def is_valid_xlsx(filepath: str) -> bool:
    """Check that a file is a genuine .xlsx (ZIP container).

    Checks:
      1. File exists and is non-empty
      2. File size exceeds minimum threshold
      3. First 2 bytes are ZIP magic bytes (PK = 0x50 0x4B)
    """
    try:
        if not os.path.isfile(filepath):
            logger.warning("Validation: file does not exist: %s", filepath)
            return False

        size = os.path.getsize(filepath)
        if size < MIN_FILE_SIZE:
            logger.warning("Validation: file too small (%d bytes): %s", size, filepath)
            return False

        with open(filepath, "rb") as f:
            magic = f.read(4)

        if not magic.startswith(ZIP_MAGIC):
            # Log what we actually got for diagnostics
            preview = magic[:20] if len(magic) >= 20 else magic
            logger.warning(
                "Validation: bad magic bytes %r (expected PK): %s",
                preview, filepath,
            )
            return False

        return True

    except Exception as e:
        logger.warning("Validation error: %s", e)
        return False


def _check_response_is_excel(resp: requests.Response) -> bool:
    """Check HTTP response headers to see if the body is likely an Excel file."""
    ct = resp.headers.get("Content-Type", "").lower()

    # Reject obvious non-excel content types
    reject_types = ["text/html", "text/plain", "application/json", "text/xml"]
    for rt in reject_types:
        if rt in ct:
            logger.info("Direct download: rejected content-type '%s'", ct)
            return False

    # Accept known excel types or octet-stream
    accept_types = [
        "application/vnd.openxmlformats",
        "application/vnd.ms-excel",
        "application/octet-stream",
        "application/zip",
    ]
    if any(at in ct for at in accept_types):
        return True

    # If content-type is ambiguous, check body magic bytes
    body_start = resp.content[:4] if len(resp.content) >= 4 else b""
    if body_start.startswith(ZIP_MAGIC):
        return True

    logger.info("Direct download: ambiguous content-type '%s', body starts with %r", ct, body_start[:10])
    return False


# ═══════════════════════════════════════════════════════════════════════════
# Method 1: Direct HTTP download
# ═══════════════════════════════════════════════════════════════════════════

def try_direct_download(progress_callback=None) -> str | None:
    """Try direct HTTP download from Euronext endpoints.

    Returns filepath to a validated .xlsx, or None.
    """
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp_path = os.path.join(DATA_DIR, "_download_tmp.xlsx")

    for i, url in enumerate(DIRECT_URLS):
        if progress_callback:
            progress_callback(f"Trying direct endpoint {i + 1}/{len(DIRECT_URLS)}…")

        try:
            # Use a session for cookie handling
            session = requests.Session()
            # First hit the page to get cookies
            session.get(
                "https://live.euronext.com/en/products/equities/list",
                headers={"User-Agent": HTTP_HEADERS["User-Agent"]},
                timeout=15,
            )

            resp = session.get(url, headers=HTTP_HEADERS, timeout=60)

            if resp.status_code != 200:
                logger.info("Direct download: HTTP %d from %s", resp.status_code, url[:80])
                continue

            if len(resp.content) < MIN_FILE_SIZE:
                logger.info("Direct download: response too small (%d bytes)", len(resp.content))
                continue

            # Check content-type / body before saving
            if not _check_response_is_excel(resp):
                continue

            # Write to temp, then validate
            with open(tmp_path, "wb") as f:
                f.write(resp.content)

            if is_valid_xlsx(tmp_path):
                # Promote to latest
                shutil.move(tmp_path, LATEST_FILE)
                _archive_copy()
                logger.info("Direct download: success from %s", url[:80])
                return LATEST_FILE
            else:
                logger.info("Direct download: file failed validation")
                _safe_remove(tmp_path)

        except requests.RequestException as e:
            logger.info("Direct download: request error: %s", e)
            continue
        except Exception as e:
            logger.warning("Direct download: unexpected error: %s", e)
            continue

    return None


# ═══════════════════════════════════════════════════════════════════════════
# Method 2: Playwright browser-based export
# ═══════════════════════════════════════════════════════════════════════════

def try_playwright_download(progress_callback=None) -> str | None:
    """Use Playwright to open the Euronext page and trigger the Excel export.

    Returns filepath to a validated .xlsx, or None.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.warning("Playwright not installed — cannot use browser fallback.")
        return None

    os.makedirs(DATA_DIR, exist_ok=True)

    if progress_callback:
        progress_callback("Launching browser…")

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
            )
            page = context.new_page()

            if progress_callback:
                progress_callback("Loading Euronext page…")

            # Navigate to the equities list
            page.goto(EURONEXT_PAGE_URL, wait_until="domcontentloaded", timeout=45_000)
            page.wait_for_timeout(3000)  # let JS render

            # Accept cookies if banner appears
            try:
                cookie_btn = page.locator("button:has-text('Accept'), #onetrust-accept-btn-handler")
                if cookie_btn.count() > 0:
                    cookie_btn.first.click(timeout=3000)
                    page.wait_for_timeout(1000)
            except Exception:
                pass

            if progress_callback:
                progress_callback("Triggering Excel export…")

            # Strategy A: Look for export/download button on the page
            downloaded_path = _playwright_try_export_button(page, context)

            # Strategy B: Hit the data endpoint directly in the browser context
            if downloaded_path is None:
                downloaded_path = _playwright_try_direct_fetch(page, context)

            browser.close()

            if downloaded_path and is_valid_xlsx(downloaded_path):
                shutil.move(downloaded_path, LATEST_FILE)
                _archive_copy()
                logger.info("Playwright download: success")
                return LATEST_FILE
            else:
                if downloaded_path:
                    logger.info("Playwright download: file failed validation")
                    _safe_remove(downloaded_path)
                return None

    except Exception as e:
        logger.warning("Playwright download: error: %s", e)
        return None


def _playwright_try_export_button(page, context) -> str | None:
    """Try to find and click an export/download button on the page."""
    # Common selectors for Euronext's export functionality
    export_selectors = [
        "a[href*='export']",
        "button:has-text('Export')",
        "a:has-text('Export')",
        "button:has-text('Download')",
        "a:has-text('Download')",
        ".export-btn",
        "[data-export]",
        "a[href*='.xlsx']",
        "a[href*='excel']",
        # Euronext-specific: the Excel icon / link in their data tables
        ".table-export a",
        ".dataTables_wrapper a[href*='export']",
    ]

    with tempfile.TemporaryDirectory() as tmp_dir:
        for selector in export_selectors:
            try:
                el = page.locator(selector)
                if el.count() == 0:
                    continue

                # Set up download handler
                with page.expect_download(timeout=30_000) as dl_info:
                    el.first.click()
                download = dl_info.value
                dest = os.path.join(tmp_dir, "export.xlsx")
                download.save_as(dest)

                if os.path.isfile(dest) and os.path.getsize(dest) > MIN_FILE_SIZE:
                    # Move out of temp before it's cleaned up
                    final = os.path.join(DATA_DIR, "_pw_download.xlsx")
                    shutil.copy2(dest, final)
                    return final

            except Exception:
                continue

    return None


def _playwright_try_direct_fetch(page, context) -> str | None:
    """Use the browser's session/cookies to fetch the data endpoint directly."""
    tmp_path = os.path.join(DATA_DIR, "_pw_fetch.xlsx")

    for url in DIRECT_URLS:
        try:
            # Use page.evaluate to do an XHR with the browser's cookies
            resp = page.request.get(url, timeout=30_000)

            if resp.status != 200:
                continue

            body = resp.body()
            if len(body) < MIN_FILE_SIZE:
                continue

            # Check magic bytes
            if not body[:2] == ZIP_MAGIC:
                continue

            with open(tmp_path, "wb") as f:
                f.write(body)

            if is_valid_xlsx(tmp_path):
                return tmp_path

            _safe_remove(tmp_path)

        except Exception:
            continue

    return None


# ═══════════════════════════════════════════════════════════════════════════
# Orchestrator
# ═══════════════════════════════════════════════════════════════════════════

class DownloadResult:
    """Container for download outcome."""
    __slots__ = ("filepath", "method", "error")

    def __init__(self, filepath=None, method=None, error=None):
        self.filepath = filepath
        self.method = method      # "direct" | "playwright" | "cache"
        self.error = error

    @property
    def ok(self):
        return self.filepath is not None


def download_latest_snapshot(progress_callback=None) -> DownloadResult:
    """Full download pipeline with automatic fallback.

    1. Try direct HTTP
    2. If that fails → try Playwright
    3. If that fails → try last cached file
    4. If nothing → return error

    Parameters
    ----------
    progress_callback : callable(str) or None
        Called with status messages for UI feedback.

    Returns
    -------
    DownloadResult
    """
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(ARCHIVE_DIR, exist_ok=True)

    # ── Step 1: Direct HTTP ──
    if progress_callback:
        progress_callback("Trying direct download…")

    filepath = try_direct_download(progress_callback)
    if filepath and is_valid_xlsx(filepath):
        return DownloadResult(filepath=filepath, method="direct")

    # ── Step 2: Playwright fallback ──
    if progress_callback:
        progress_callback("Direct download failed — launching browser fallback…")

    filepath = try_playwright_download(progress_callback)
    if filepath and is_valid_xlsx(filepath):
        return DownloadResult(filepath=filepath, method="playwright")

    # ── Step 3: Fall back to cached file ──
    if os.path.isfile(LATEST_FILE) and is_valid_xlsx(LATEST_FILE):
        return DownloadResult(
            filepath=LATEST_FILE,
            method="cache",
            error="Both download methods failed. Using last valid cached file.",
        )

    # ── Step 4: Total failure ──
    return DownloadResult(
        error=(
            "Failed to retrieve a valid Excel export from Euronext Live. "
            "The downloaded response was not a real .xlsx file. "
            "Please check your network connection and try again."
        ),
    )


# ═══════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════

def _archive_copy():
    """Copy LATEST_FILE to the archive directory with timestamp."""
    try:
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M")
        dest = os.path.join(ARCHIVE_DIR, f"equities_{ts}.xlsx")
        shutil.copy2(LATEST_FILE, dest)

        # Prune old archives (keep last 200)
        archives = sorted(glob.glob(os.path.join(ARCHIVE_DIR, "equities_*.xlsx")))
        if len(archives) > 200:
            for old in archives[:-200]:
                _safe_remove(old)
    except Exception as e:
        logger.warning("Archive copy failed: %s", e)


def _safe_remove(path: str):
    """Remove a file, ignoring errors."""
    try:
        if os.path.isfile(path):
            os.remove(path)
    except Exception:
        pass
