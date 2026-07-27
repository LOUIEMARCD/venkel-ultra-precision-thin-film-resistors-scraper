#!/usr/bin/env python3
"""Resumable Venkel Ultra Precision Thin Film Resistors scraper for GitHub Actions.

The browser opens only:
https://www.venkel.com/category/resistors/ultra-precision-thin-film-resistors?q=*

The page's own Expertrec JSON listing request is captured, then crawled directly.
Large result sets are recursively divided using Expertrec's live ``sfacets`` values
so the scraper does not hit the API's finite result window.
"""

from __future__ import annotations

import asyncio
import csv
import gzip
import hashlib
import json
import logging
import math
import os
import re
import shutil
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

from playwright.async_api import (
    BrowserContext,
    Page,
    Playwright,
    Response,
    TimeoutError as PlaywrightTimeoutError,
    async_playwright,
)

BASE_URL = "https://www.venkel.com/category/resistors/ultra-precision-thin-film-resistors?q=*"
SCRIPT_VERSION = "2026-07-27-venkel-ultra-precision-v1"
# Keeps the user's existing workflow validation compatible.
WORKFLOW_COMPAT_MARKER = "2026-07-27-venkel-ultra-precision-v1"
STATE_SCHEMA = 1

DATA_DIR = Path("data")
CHUNKS_DIR = DATA_DIR / "chunks"
STATE_FILE = DATA_DIR / "state.json"
PENDING_FILE = DATA_DIR / "pending.jsonl.gz"
FINAL_CSV = DATA_DIR / "venkel_ultra_precision_thin_film_resistors.csv"
FINAL_CSV_GZ = DATA_DIR / "venkel_ultra_precision_thin_film_resistors.csv.gz"
LOG_FILE = DATA_DIR / "venkel_ultra_precision_thin_film_resistors.log"
API_TEMPLATE_FILE = DATA_DIR / "captured_ultra_precision_api_template.json"
API_SAMPLE_FILE = DATA_DIR / "captured_ultra_precision_api_response_sample.json"
DIAGNOSTIC_FILE = DATA_DIR / "ultra_precision_partition_diagnostic.json"

MAX_RUN_MINUTES = int(os.getenv("MAX_RUN_MINUTES", "315"))
REQUESTED_PAGE_SIZE = min(100, max(24, int(os.getenv("RESULTS_PER_PAGE", "100"))))
# Earlier runs showed the unfiltered Expertrec response repeating around 2,448 rows.
# Split well before that boundary.
# Expertrec repeats/overlaps results once an individual filtered query crosses roughly 1,000 rows.
# Force every leaf partition below that boundary even when the workflow still passes 1,800.
SAFE_RESULT_WINDOW = min(900, max(300, int(os.getenv("SAFE_RESULT_WINDOW", "900"))))
CHUNK_SIZE = max(500, int(os.getenv("CHUNK_SIZE", "5000")))
REQUEST_DELAY_SECONDS = max(0.20, float(os.getenv("REQUEST_DELAY_SECONDS", "0.45")))
HEADLESS = os.getenv("HEADLESS", "1") != "0"

PART_RE = re.compile(r"\bUPTF[A-Z0-9._+\-/]*", re.IGNORECASE)
PREFERRED_FACETS = (
    "size",
    "package",
    "case",
    "tolerance",
    "temperature coefficient",
    "temp coefficient",
    "tcr",
    "power rating",
    "power",
    "termination",
    "packaging",
    "reel",
    "resistance",
    "operating temperature",
    "voltage",
    "life cycle",
    "status",
)


def configure_logging() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
        ],
        force=True,
    )


def utc_now() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


def clean(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def default_state() -> dict[str, Any]:
    return {
        "schema": STATE_SCHEMA,
        "source_url": BASE_URL,
        "complete": False,
        "root_total": None,
        "rows_saved": 0,
        "chunk_index": 0,
        "queue": [],
        "partitions_completed": 0,
        "last_run_utc": None,
    }


def load_state() -> dict[str, Any]:
    state = default_state()
    if STATE_FILE.exists():
        try:
            saved = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            if int(saved.get("schema", 0)) == STATE_SCHEMA:
                state.update(saved)
            else:
                logging.warning(
                    "Resetting obsolete checkpoint schema %s to schema %s; existing product rows are preserved.",
                    saved.get("schema", "unknown"),
                    STATE_SCHEMA,
                )
        except Exception as exc:
            backup = STATE_FILE.with_suffix(f".corrupt-{int(time.time())}.json")
            shutil.copy2(STATE_FILE, backup)
            logging.warning("Unreadable state.json (%s); backed up to %s.", exc, backup)
    state["schema"] = STATE_SCHEMA
    state["source_url"] = BASE_URL
    return state


def flatten(value: Any, prefix: str = "") -> dict[str, str]:
    out: dict[str, str] = {}
    if isinstance(value, dict):
        for key, child in value.items():
            name = f"{prefix}.{key}" if prefix else str(key)
            out.update(flatten(child, name))
    elif isinstance(value, list):
        out[prefix or "Value"] = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    else:
        out[prefix or "Value"] = clean(value)
    return out


def find_part_number(record: dict[str, str]) -> str:
    preferred: list[str] = []
    fallback: list[str] = []
    for key, value in record.items():
        if not value:
            continue
        match = PART_RE.search(value)
        if not match:
            continue
        pn = match.group(0).rstrip(".,;:)").upper()
        key_norm = re.sub(r"[^a-z0-9]", "", key.lower())
        if any(token in key_norm for token in ("partnumber", "partno", "sku", "mpn", "productcode", "uid")):
            preferred.append(pn)
        else:
            fallback.append(pn)
    return (preferred or fallback or [""])[0]


def find_product_url(record: dict[str, str], part_number: str) -> str:
    for key, value in record.items():
        if not value:
            continue
        if any(token in key.lower() for token in ("url", "href", "link")) and "/part/" in value:
            match = re.search(r"https?://[^\s\"']+|/part/[^\s\"']+", value)
            if match:
                return urljoin("https://www.venkel.com", match.group(0))
    for value in record.values():
        if "/part/" in value:
            match = re.search(r"https?://[^\s\"']+|/part/[^\s\"']+", value)
            if match:
                return urljoin("https://www.venkel.com", match.group(0))
    return f"https://www.venkel.com/part/{part_number}" if part_number else ""


def normalize_record(raw: dict[str, Any]) -> dict[str, str]:
    record = {clean(k): clean(v) for k, v in raw.items() if clean(k)}
    pn = record.get("Part Number") or find_part_number(record)
    url = record.get("Product URL") or find_product_url(record, pn)
    if pn:
        record["Part Number"] = pn
    if url:
        record["Product URL"] = url
    return record


def record_key(record: dict[str, str]) -> str:
    identity = record.get("Part Number") or record.get("Product URL")
    if identity:
        return identity.casefold().strip()
    payload = json.dumps(record, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def iter_jsonl_gz(path: Path) -> Iterable[dict[str, str]]:
    if not path.exists():
        return
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line)


class Store:
    def __init__(self, state: dict[str, Any]) -> None:
        CHUNKS_DIR.mkdir(parents=True, exist_ok=True)
        self.state = state
        self.seen: set[str] = set()
        self.pending: list[dict[str, str]] = []
        max_index = 0
        for path in sorted(CHUNKS_DIR.glob("chunk_*.jsonl.gz")):
            match = re.search(r"chunk_(\d+)\.jsonl\.gz$", path.name)
            if match:
                max_index = max(max_index, int(match.group(1)))
            for row in iter_jsonl_gz(path):
                self.seen.add(record_key(row))
        if PENDING_FILE.exists():
            for row in iter_jsonl_gz(PENDING_FILE):
                key = record_key(row)
                if key not in self.seen:
                    self.seen.add(key)
                    self.pending.append(row)
        self.state["chunk_index"] = max(max_index, int(self.state.get("chunk_index", 0)))
        self.state["rows_saved"] = len(self.seen)
        logging.info("Loaded %s existing unique product rows.", f"{len(self.seen):,}")

    def add(self, records: Iterable[dict[str, Any]]) -> int:
        added = 0
        for raw in records:
            row = normalize_record(raw)
            if not row:
                continue
            key = record_key(row)
            if key in self.seen:
                continue
            self.seen.add(key)
            self.pending.append(row)
            added += 1
            while len(self.pending) >= CHUNK_SIZE:
                self.flush_chunk()
        self.state["rows_saved"] = len(self.seen)
        return added

    def flush_chunk(self) -> None:
        if len(self.pending) < CHUNK_SIZE:
            return
        batch = self.pending[:CHUNK_SIZE]
        self.pending = self.pending[CHUNK_SIZE:]
        index = int(self.state.get("chunk_index", 0)) + 1
        target = CHUNKS_DIR / f"chunk_{index:06d}.jsonl.gz"
        tmp = target.with_suffix(target.suffix + ".tmp")
        with gzip.open(tmp, "wt", encoding="utf-8", newline="") as fh:
            for row in batch:
                fh.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
        tmp.replace(target)
        self.state["chunk_index"] = index
        logging.info("Saved checkpoint chunk %s (%s rows).", target.name, f"{len(batch):,}")
        self.save_pending()

    def save_pending(self) -> None:
        if not self.pending:
            PENDING_FILE.unlink(missing_ok=True)
            return
        tmp = PENDING_FILE.with_suffix(PENDING_FILE.suffix + ".tmp")
        with gzip.open(tmp, "wt", encoding="utf-8", newline="") as fh:
            for row in self.pending:
                fh.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
        tmp.replace(PENDING_FILE)

    def all_rows(self) -> Iterable[dict[str, str]]:
        yielded: set[str] = set()
        for path in sorted(CHUNKS_DIR.glob("chunk_*.jsonl.gz")):
            for row in iter_jsonl_gz(path):
                key = record_key(row)
                if key not in yielded:
                    yielded.add(key)
                    yield row
        for row in self.pending:
            key = record_key(row)
            if key not in yielded:
                yielded.add(key)
                yield row

    def export_csv(self) -> None:
        fields: set[str] = set()
        for row in self.all_rows():
            fields.update(row)
        preferred = ["Part Number", "Product URL"]
        columns = [name for name in preferred if name in fields]
        columns.extend(sorted(fields.difference(columns), key=str.casefold))
        tmp = FINAL_CSV.with_suffix(".csv.tmp")
        with tmp.open("w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            for row in self.all_rows():
                writer.writerow(row)
        tmp.replace(FINAL_CSV)
        tmp_gz = FINAL_CSV_GZ.with_suffix(".gz.tmp")
        with FINAL_CSV.open("rb") as src, gzip.open(tmp_gz, "wb", compresslevel=6) as dst:
            shutil.copyfileobj(src, dst)
        tmp_gz.replace(FINAL_CSV_GZ)
        logging.info("CSV exported with %s unique rows.", f"{len(self.seen):,}")


def parse_count(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)) and value >= 0:
        return int(value)
    if isinstance(value, str):
        stripped = value.replace(",", "").strip()
        if stripped.isdigit():
            return int(stripped)
    return None


def extract_total(payload: Any) -> int | None:
    # Expertrec v6 places the total count here.
    if isinstance(payload, dict):
        res = payload.get("res")
        if isinstance(res, dict):
            count = parse_count(res.get("count"))
            if count is not None:
                return count
        for key in ("total", "totalResults", "resultCount", "totalCount"):
            count = parse_count(payload.get(key))
            if count is not None:
                return count
    return None


def extract_records(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    results = payload.get("results")
    if not isinstance(results, list):
        return []
    return [item for item in results if isinstance(item, dict)]


def extract_facets(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    sfacets = payload.get("sfacets")
    if not isinstance(sfacets, dict):
        return []

    output: list[dict[str, Any]] = []
    for field, raw_values in sfacets.items():
        values: list[dict[str, Any]] = []
        if isinstance(raw_values, dict):
            raw_values = [{"name": name, "count": count} for name, count in raw_values.items()]
        if not isinstance(raw_values, list):
            continue
        for item in raw_values:
            if not isinstance(item, dict):
                continue
            value = item.get("name")
            if value is None:
                value = item.get("value") or item.get("label")
            count = parse_count(item.get("count"))
            value_text = clean(value)
            if value_text and count and count > 0:
                values.append({"value": value_text, "count": count})
        if len(values) >= 2:
            output.append({"field": clean(field), "label": clean(field), "values": values})
    return output


@dataclass
class Candidate:
    url: str
    headers: dict[str, str]
    payload: dict[str, Any]
    records: list[dict[str, Any]]
    total: int
    captured_at: float


class Collector:
    def __init__(self, page: Page) -> None:
        self.candidates: list[Candidate] = []
        self.tasks: set[asyncio.Task[Any]] = set()
        page.on("response", self._on_response)

    def _on_response(self, response: Response) -> None:
        task = asyncio.create_task(self._process(response))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def drain(self) -> None:
        if self.tasks:
            await asyncio.gather(*list(self.tasks), return_exceptions=True)

    async def _process(self, response: Response) -> None:
        req = response.request
        parsed = urlparse(req.url)
        if req.method.upper() != "GET":
            return
        if parsed.netloc.lower() != "searchv7.expertrec.com" or "/v6/search/" not in parsed.path:
            return
        try:
            payload = await response.json()
        except Exception:
            return
        records = extract_records(payload)
        total = extract_total(payload)
        if not records or total is None:
            return
        try:
            headers = await req.all_headers()
        except Exception:
            headers = {}
        candidate = Candidate(
            url=req.url,
            headers=headers,
            payload=payload,
            records=records,
            total=total,
            captured_at=time.monotonic(),
        )
        self.candidates.append(candidate)
        logging.info(
            "Captured Expertrec listing API: records=%s | total=%s | facets=%s",
            len(records),
            f"{total:,}",
            len(extract_facets(payload)),
        )

    def best(self) -> Candidate | None:
        if not self.candidates:
            return None
        return max(self.candidates, key=lambda item: (item.total, len(item.records), item.captured_at))


def safe_headers(headers: dict[str, str]) -> dict[str, str]:
    blocked = {
        "host", "content-length", "connection", "accept-encoding", "cookie",
        "sec-fetch-dest", "sec-fetch-mode", "sec-fetch-site",
    }
    return {key: value for key, value in headers.items() if key.lower() not in blocked}


def build_url(candidate: Candidate, page_index: int, partition: dict[str, Any]) -> str:
    parsed = urlparse(candidate.url)
    base_pairs = parse_qsl(parsed.query, keep_blank_values=True)
    pairs = [(k, v) for k, v in base_pairs if k not in {"page", "size"}]
    pairs.append(("page", str(page_index)))  # Expertrec pages are zero-based.
    pairs.append(("size", str(REQUESTED_PAGE_SIZE)))

    for item in partition.get("filters", []):
        kind = item.get("kind", "list")
        field = clean(item.get("field"))
        if not field:
            continue
        if kind == "list":
            pairs.append(("fq", f"{field}:{clean(item.get('value'))}"))
        elif kind == "range":
            pairs.append(("nf", f"{field}:{item.get('low', '')}-{item.get('high', '')}"))

    return urlunparse(parsed._replace(query=urlencode(pairs, doseq=True)))


async def fetch_payload(
    context: BrowserContext,
    candidate: Candidate,
    page_index: int,
    partition: dict[str, Any],
) -> tuple[list[dict[str, Any]], int, dict[str, Any]]:
    url = build_url(candidate, page_index, partition)
    expected = urlparse(candidate.url)
    actual = urlparse(url)
    if (actual.scheme, actual.netloc, actual.path) != (expected.scheme, expected.netloc, expected.path):
        raise RuntimeError("Generated request escaped the exact Expertrec endpoint captured from Venkel.")

    headers = safe_headers(candidate.headers)
    last_error: Exception | None = None
    for attempt in range(1, 7):
        try:
            response = await context.request.get(url, headers=headers, timeout=120_000)
            if response.status == 429:
                wait = min(120, 15 * attempt)
                logging.warning("Expertrec returned HTTP 429; waiting %s seconds (attempt %s/6).", wait, attempt)
                await asyncio.sleep(wait)
                continue
            if response.status >= 500:
                wait = min(60, 10 * attempt)
                logging.warning("Expertrec returned HTTP %s; retrying in %s seconds.", response.status, wait)
                await asyncio.sleep(wait)
                continue
            if not response.ok:
                raise RuntimeError(f"Expertrec returned HTTP {response.status} for page index {page_index}.")
            payload = await response.json()
            records = extract_records(payload)
            total = extract_total(payload)
            if total is None:
                raise RuntimeError("Expertrec response did not contain res.count.")
            return records, total, payload
        except Exception as exc:
            last_error = exc
            if attempt < 6:
                await asyncio.sleep(min(30, 3 * attempt))
    raise RuntimeError(f"Expertrec request failed after retries: {last_error}")


def facet_rank(facet: dict[str, Any], parent_total: int) -> tuple[Any, ...]:
    name = f"{facet.get('label', '')} {facet.get('field', '')}".lower()
    preferred = len(PREFERRED_FACETS)
    for index, token in enumerate(PREFERRED_FACETS):
        if token in name:
            preferred = index
            break
    values = facet.get("values", [])
    largest = max((int(item["count"]) for item in values), default=parent_total)
    return (preferred, largest, len(values))


def partition_label(partition: dict[str, Any]) -> str:
    bits = [f"{item.get('field')}={item.get('value')}" for item in partition.get("filters", [])]
    return " | ".join(bits) or "root"


def page_signature(records: list[dict[str, Any]]) -> str:
    keys = sorted(record_key(normalize_record(flatten(item))) for item in records)
    return hashlib.sha256("\n".join(keys).encode("utf-8")).hexdigest()


async def choose_split(
    context: BrowserContext,
    candidate: Candidate,
    parent: dict[str, Any],
    parent_total: int,
    payload: dict[str, Any],
) -> list[dict[str, Any]] | None:
    used_fields = set(parent.get("used_fields", []))
    facets = [facet for facet in extract_facets(payload) if facet["field"] not in used_fields]
    facets.sort(key=lambda item: facet_rank(item, parent_total))
    rejected: list[dict[str, Any]] = []

    for facet in facets:
        field = facet["field"]
        values = facet["values"]
        if len(values) < 2 or len(values) > 5000:
            rejected.append({"field": field, "reason": "value count outside 2..5000", "values": len(values)})
            continue

        listed_sum = sum(int(item["count"]) for item in values)
        coverage = listed_sum / max(parent_total, 1)
        # Facet values must account for effectively all parent rows. A small overlap is acceptable.
        if coverage < 0.995 or coverage > 1.25:
            rejected.append({
                "field": field,
                "reason": "facet coverage unsuitable",
                "listed_sum": listed_sum,
                "parent_total": parent_total,
                "coverage": coverage,
            })
            continue

        probes: list[dict[str, Any]] = []
        valid = True
        for value_info in sorted(values, key=lambda x: int(x["count"]), reverse=True)[:3]:
            child = {
                "filters": list(parent.get("filters", [])) + [{
                    "kind": "list", "field": field, "value": value_info["value"]
                }],
                "used_fields": sorted(used_fields | {field}),
                "next_page": 0,
                "last_signature": "",
            }
            _, live_total, _ = await fetch_payload(context, candidate, 0, child)
            probes.append({"value": value_info["value"], "listed": value_info["count"], "live": live_total})
            if live_total <= 0 or live_total >= parent_total:
                valid = False
                break

        if not valid:
            rejected.append({"field": field, "reason": "live filter validation failed", "probes": probes})
            continue

        children = []
        for value_info in values:
            children.append({
                "filters": list(parent.get("filters", [])) + [{
                    "kind": "list", "field": field, "value": value_info["value"]
                }],
                "used_fields": sorted(used_fields | {field}),
                "next_page": 0,
                "last_signature": "",
                "total_hint": int(value_info["count"]),
            })
        children.sort(key=lambda child: int(child.get("total_hint", 0)), reverse=True)
        logging.info(
            "Validated Expertrec facet '%s': %s children, coverage %.4f, probes=%s.",
            field, len(children), coverage, probes,
        )
        return children

    atomic_json(DIAGNOSTIC_FILE, {
        "script_version": SCRIPT_VERSION,
        "parent": parent,
        "parent_total": parent_total,
        "available_facets": extract_facets(payload),
        "rejected": rejected,
        "raw_sfacets": payload.get("sfacets") if isinstance(payload, dict) else None,
    })
    return None


async def open_and_capture(page: Page, collector: Collector) -> Candidate:
    logging.info("Opening exact Venkel listing page in Chromium...")
    response = await page.goto(BASE_URL, wait_until="commit", timeout=120_000)
    logging.info("Initial Venkel page response: HTTP %s", response.status if response else "unknown")
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=30_000)
    except PlaywrightTimeoutError:
        logging.warning("DOMContentLoaded was delayed; waiting for product links directly.")
    try:
        await page.wait_for_function(
            "document.querySelectorAll('a[href*=\"/part/\"]').length > 0",
            timeout=120_000,
        )
    except PlaywrightTimeoutError:
        await page.screenshot(path=str(DATA_DIR / "navigation_failure.png"), full_page=False)
        (DATA_DIR / "navigation_failure.html").write_text(await page.content(), encoding="utf-8")
        raise RuntimeError("The Venkel listing did not render product links in Chromium.")

    await page.wait_for_timeout(7000)
    await collector.drain()
    candidate = collector.best()
    if candidate is None:
        raise RuntimeError("The Venkel page rendered, but its Expertrec listing request was not captured.")

    atomic_json(API_TEMPLATE_FILE, {
        "script_version": SCRIPT_VERSION,
        "url": candidate.url,
        "records_on_first_response": len(candidate.records),
        "total": candidate.total,
        "facets": extract_facets(candidate.payload),
    })
    # Save one real response so any future schema change can be diagnosed without another user round-trip.
    atomic_json(API_SAMPLE_FILE, candidate.payload)
    return candidate


async def crawl(
    context: BrowserContext,
    candidate: Candidate,
    state: dict[str, Any],
    store: Store,
    deadline: float,
) -> None:
    root = {"filters": [], "used_fields": [], "next_page": 0, "last_signature": ""}
    root_records, root_total, root_payload = await fetch_payload(context, candidate, 0, root)
    if root_total < 1000:
        raise RuntimeError(f"Captured Ultra Precision listing total is unexpectedly small ({root_total}); refusing a wrong-category crawl.")
    state["root_total"] = root_total

    root_added = store.add(flatten(item) for item in root_records)
    effective_page_size = len(root_records)
    if effective_page_size <= 0:
        raise RuntimeError("Expertrec returned an empty first page.")
    state["effective_page_size"] = effective_page_size

    logging.info(
        "Verified Venkel Precision Thin Film listing: %s results; API page size=%s; page 0 added %s.",
        f"{root_total:,}", effective_page_size, root_added,
    )

    if not state.get("queue"):
        state["queue"] = [root]
        state["complete"] = False
        logging.info(
            "Initialized fresh Ultra Precision Thin Film Resistors facet crawl; "
            "%s existing rows are preserved.",
            f"{len(store.seen):,}",
        )

    queue: list[dict[str, Any]] = state["queue"]
    requests_since_save = 0

    while queue and time.monotonic() < deadline:
        partition = queue[0]
        label = partition_label(partition)
        page0_records, total, page0_payload = await fetch_payload(context, candidate, 0, partition)
        page_size = len(page0_records)
        if page_size <= 0 and total > 0:
            raise RuntimeError(f"Partition '{label}' returned no rows on page 0.")
        partition["total_hint"] = total

        if total <= 0:
            logging.info("Discarding empty partition: %s", label)
            queue.pop(0)
            continue

        if total > SAFE_RESULT_WINDOW or partition.get("force_split"):
            # Save the first page before splitting. This also preserves any item that
            # Expertrec omits from the selected facet's value list.
            head_added = store.add(flatten(item) for item in page0_records)
            if head_added:
                logging.info(
                    "Partition %s | first page added %s | total saved %s / %s",
                    label, head_added, f"{len(store.seen):,}", f"{root_total:,}",
                )
            children = await choose_split(context, candidate, partition, total, page0_payload)
            if not children:
                raise RuntimeError(
                    f"Could not split partition '{label}' containing {total:,} results. "
                    f"Exact diagnostics were saved to {DIAGNOSTIC_FILE}."
                )
            queue.pop(0)
            queue[0:0] = children
            logging.info(
                "Split partition %s (%s results) into %s children; largest listed child=%s.",
                label,
                f"{total:,}",
                len(children),
                f"{max(int(child.get('total_hint', 0)) for child in children):,}",
            )
            store.save_pending()
            atomic_json(STATE_FILE, state)
            continue

        pages = max(1, math.ceil(total / page_size))
        page_index = max(0, int(partition.get("next_page", 0)))
        seen_signatures = {
            clean(value)
            for value in partition.get("seen_signatures", [])
            if clean(value)
        }
        seen_page_keys = {
            clean(value)
            for value in partition.get("seen_page_keys", [])
            if clean(value)
        }

        logging.info(
            "Crawling partition %s | total=%s | page size=%s | pages=%s | resume index=%s.",
            label, f"{total:,}", page_size, f"{pages:,}", f"{page_index:,}",
        )

        split_triggered = False
        while page_index < pages and time.monotonic() < deadline:
            if page_index == 0:
                records = page0_records
                current_total = total
            else:
                records, current_total, _ = await fetch_payload(context, candidate, page_index, partition)

            if current_total != total:
                total = current_total
                partition["total_hint"] = total
                pages = max(1, math.ceil(total / max(page_size, 1)))

            if not records:
                raise RuntimeError(
                    f"Partition '{label}' returned an empty page index {page_index} before expected index {pages - 1}."
                )

            signature = page_signature(records)
            page_keys = {
                record_key(normalize_record(flatten(item)))
                for item in records
            }
            exact_repeat = signature in seen_signatures
            overlap_count = len(page_keys.intersection(seen_page_keys))
            material_overlap = page_index > 0 and overlap_count > 0

            if exact_repeat or material_overlap:
                logging.warning(
                    "Partition %s repeated/overlapped data at page index %s "
                    "(exact_repeat=%s, overlap=%s); splitting it more finely.",
                    label, page_index, exact_repeat, overlap_count,
                )
                partition["next_page"] = 0
                partition["last_signature"] = ""
                partition["seen_signatures"] = []
                partition["seen_page_keys"] = []
                partition["force_split"] = True
                children = await choose_split(context, candidate, partition, total, page0_payload)
                if not children:
                    raise RuntimeError(
                        f"Partition '{label}' hit the Expertrec result window and could not be split further. "
                        f"Exact diagnostics were saved to {DIAGNOSTIC_FILE}."
                    )
                queue.pop(0)
                queue[0:0] = children
                store.save_pending()
                atomic_json(STATE_FILE, state)
                split_triggered = True
                break

            added = store.add(flatten(item) for item in records)
            seen_signatures.add(signature)
            seen_page_keys.update(page_keys)
            partition["last_signature"] = signature
            partition["seen_signatures"] = sorted(seen_signatures)
            partition["seen_page_keys"] = sorted(seen_page_keys)
            partition["next_page"] = page_index + 1
            state["last_run_utc"] = utc_now()
            requests_since_save += 1

            if page_index <= 1 or page_index % 10 == 0 or added == 0 or page_index == pages - 1:
                logging.info(
                    "Partition %s | page index %s/%s | received %s | added %s | total saved %s / %s",
                    label,
                    f"{page_index:,}",
                    f"{pages - 1:,}",
                    len(records),
                    added,
                    f"{len(store.seen):,}",
                    f"{root_total:,}",
                )

            if requests_since_save >= 5:
                store.save_pending()
                atomic_json(STATE_FILE, state)
                requests_since_save = 0

            page_index += 1
            await asyncio.sleep(REQUEST_DELAY_SECONDS)

        if split_triggered:
            continue

        if page_index >= pages:
            partition.pop("seen_signatures", None)
            partition.pop("seen_page_keys", None)
            queue.pop(0)
            state["partitions_completed"] = int(state.get("partitions_completed", 0)) + 1
            logging.info("Finished partition %s. Remaining partitions: %s.", label, f"{len(queue):,}")
            store.save_pending()
            atomic_json(STATE_FILE, state)

    state["rows_saved"] = len(store.seen)
    state["last_run_utc"] = utc_now()

    if not queue:
        difference = root_total - len(store.seen)
        if difference > 1:
            raise RuntimeError(
                f"Ultra Precision Thin Film Resistors crawl finished with {len(store.seen):,} stored rows "
                f"against {root_total:,} site-reported results."
            )
        state.update({
            "complete": True,
            "completion_status": (
                "exact" if difference == 0 else "complete_with_one_index_count_discrepancy"
            ),
            "rows_saved": len(store.seen),
            "site_reported_results": root_total,
            "index_count_difference": max(0, difference),
        })
        logging.info(
            "FINALIZED: preserved %s stored rows against %s site-reported results "
            "(index-count difference: %s).",
            f"{len(store.seen):,}",
            f"{root_total:,}",
            max(0, difference),
        )
    else:
        state["complete"] = False
        logging.info(
            "Run time limit reached safely: %s / %s rows saved; %s partitions remain.",
            f"{len(store.seen):,}", f"{root_total:,}", f"{len(queue):,}",
        )


async def run(playwright: Playwright) -> None:
    state = load_state()
    store = Store(state)

    root_total = int(state.get("root_total") or 0)

    # On a resumed completed crawl, preserve the stored output. Expertrec may
    # occasionally report one more index result than the number of unique parts.
    if root_total and len(store.seen) >= root_total - 1:
        difference = max(0, root_total - len(store.seen))
        state.update({
            "complete": True,
            "completion_status": (
                "exact" if difference == 0 else "complete_with_one_index_count_discrepancy"
            ),
            "rows_saved": len(store.seen),
            "site_reported_results": root_total,
            "index_count_difference": difference,
            "last_run_utc": utc_now(),
        })
        store.save_pending()
        atomic_json(STATE_FILE, state)
        store.export_csv()
        logging.info(
            "FINALIZED: preserved %s stored rows against %s site-reported results "
            "(index-count difference: %s). No rows were removed.",
            f"{len(store.seen):,}",
            f"{root_total:,}",
            difference,
        )
        return

    if state.get("complete"):
        state["complete"] = False

    deadline = time.monotonic() + MAX_RUN_MINUTES * 60
    browser = await playwright.chromium.launch(
        headless=HEADLESS,
        args=[
            "--disable-dev-shm-usage",
            "--no-sandbox",
            "--disable-http2",
            "--disable-blink-features=AutomationControlled",
        ],
    )

    context = await browser.new_context(
        viewport={"width": 1600, "height": 1000},
        locale="en-US",
        user_agent=(
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36"
        ),
    )

    page = await context.new_page()
    collector = Collector(page)

    async def route_handler(route: Any) -> None:
        if route.request.resource_type in {"image", "media", "font"}:
            await route.abort()
        else:
            await route.continue_()

    await page.route("**/*", route_handler)

    try:
        candidate = await open_and_capture(page, collector)
        await crawl(context, candidate, state, store, deadline)
    finally:
        store.save_pending()
        atomic_json(STATE_FILE, state)
        store.export_csv()
        await context.close()
        await browser.close()


async def main_async() -> None:
    async with async_playwright() as playwright:
        await run(playwright)


def main() -> int:
    configure_logging()
    logging.info("Starting Venkel Ultra Precision Thin Film Resistors scraper (%s).", SCRIPT_VERSION)
    logging.info("Source URL: %s", BASE_URL)
    logging.info(
        "Fresh/resumable Expertrec facet crawl with a maximum 900-result leaf partition.",
    )
    try:
        asyncio.run(main_async())
        return 0
    except KeyboardInterrupt:
        logging.warning("Interrupted; checkpoint files were preserved.")
        return 130
    except Exception as exc:
        logging.exception("Scraper stopped with an error: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
