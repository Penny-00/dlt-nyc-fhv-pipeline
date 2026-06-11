from __future__ import annotations

import csv
import gzip
import io
import json
import logging
import os
import threading
from dataclasses import dataclass
from queue import Queue
from typing import Iterable, Iterator

import dlt
from dlt.destinations.adapters import bigquery_adapter
from requests.adapters import HTTPAdapter
import requests
from urllib3.util.retry import Retry

# Configuration Constants
SOURCE_BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv"
PIPELINE_NAME = "fhv_2019_pipeline"
DATASET_NAME = "fhv_2019"
# Generates all months from Jan (1) to Dec (12) for 2019
MONTHS_2019 = tuple((2019, m) for m in range(1, 13))
CHUNK_SIZE = 50_000
MAX_WORKERS = 4
REQUEST_TIMEOUT_SECONDS = 60
GCP_KEY_PATH = "/home/penny_dev/.gcp/gcp-key.json"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("dlt_bigquery_pipeline")


@dataclass(frozen=True)
class MonthTask:
    year: int
    month: int

    @property
    def key(self) -> str:
        return f"fhv_tripdata_{self.year}-{self.month:02d}"

    @property
    def url(self) -> str:
        return f"{SOURCE_BASE_URL}/{self.key}.csv.gz"


def create_retry_session() -> requests.Session:
    """Creates a requests session with robust retry settings for network resiliency."""
    session = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def iter_month_tasks() -> Iterator[MonthTask]:
    for year, month in MONTHS_2019:
        yield MonthTask(year=year, month=month)


def stream_month_rows(task: MonthTask) -> Iterator[list[dict[str, object]]]:
    """Streams data from GitHub, parses gzip, cleans fields, and yields chunks."""
    logger.info("Extracting %s from %s", task.key, task.url)
    session = create_retry_session()
    
    with session.get(task.url, stream=True, timeout=REQUEST_TIMEOUT_SECONDS) as response:
        response.raise_for_status()
        response.raw.decode_content = True
        
        with gzip.GzipFile(fileobj=response.raw) as gzip_stream:
            text_stream = io.TextIOWrapper(gzip_stream, encoding="utf-8")
            reader = csv.DictReader(text_stream)
            
            batch: list[dict[str, object]] = []
            row_count = 0
            
            for row in reader:
                clean_row: dict[str, object] = {}
                for key, value in row.items():
                    if key is None:
                        continue
                    # Force keys to lowercase and strip whitespace to match the DLT schema contract
                    cleaned_key = key.strip().lower()
                    # Remap source column names to match schema contract
                    if cleaned_key == "dropoff_datetime":
                        cleaned_key = "drop_off_datetime"
                    elif cleaned_key == "pulocationid":
                        cleaned_key = "p_ulocation_id"
                    elif cleaned_key == "dolocationid":
                        cleaned_key = "d_olocation_id"

                    # Clean values: strip whitespace and map empty strings to None (database NULL)
                    if value is not None:
                        cleaned_val = value.strip()
                        if cleaned_val == "":
                            clean_row[cleaned_key] = None
                        else:
                            clean_row[cleaned_key] = cleaned_val
                    else:
                        clean_row[cleaned_key] = None
                
                batch.append(clean_row)
                row_count += 1
                
                if len(batch) >= CHUNK_SIZE:
                    yield batch
                    batch = []
            
            if batch:
                yield batch
                
            logger.info("Finished extraction of %s with %s rows", task.key, f"{row_count:,}")


def load_months_concurrently(
    tasks: Iterable[MonthTask],
    loaded_months: list[str],
) -> Iterator[list[dict[str, object]]]:
    """Downloads multiple months in parallel and streams batches back to the main thread."""
    queue: Queue[tuple[str, object]] = Queue()
    stop_event = threading.Event()
    active_workers = 0
    concurrency_limit = threading.BoundedSemaphore(MAX_WORKERS)

    def worker(task: MonthTask) -> None:
        nonlocal active_workers
        with concurrency_limit:
            if stop_event.is_set():
                queue.put(("done", task.key))
                return

            if task.key in loaded_months:
                logger.info("Skipping %s (already loaded in DLT state)", task.key)
                queue.put(("done", task.key))
                return

            try:
                for batch in stream_month_rows(task):
                    if stop_event.is_set():
                        return
                    queue.put(("batch", batch))
                queue.put(("done", task.key))
            except Exception as exc:
                logger.error("Error downloading %s: %s", task.key, str(exc))
                queue.put(("error", (task.key, exc)))

    threads: list[threading.Thread] = []
    for task in tasks:
        # Check if month is already loaded to avoid spawning threads unnecessarily
        if task.key in loaded_months:
            logger.info("Skipping thread spawn for %s (already loaded)", task.key)
            continue
            
        thread = threading.Thread(target=worker, args=(task,), daemon=True)
        threads.append(thread)
        thread.start()
        active_workers += 1

    if active_workers == 0:
        logger.info("All months are already successfully loaded.")
        return

    completed_months: set[str] = set()
    succeeded = False
    try:
        while active_workers > 0:
            kind, payload = queue.get()
            if kind == "batch":
                yield payload  # type: ignore[misc]
            elif kind == "done":
                completed_months.add(payload)  # type: ignore[arg-type]
                active_workers -= 1
            elif kind == "error":
                task_key, exc = payload  # type: ignore[misc]
                stop_event.set()
                raise RuntimeError(f"Pipeline extraction failed at month: {task_key}") from exc
        succeeded = True
    finally:
        # signal and clean up threads
        stop_event.set()
        for thread in threads:
            thread.join(timeout=2)
            
    if succeeded:
        for month_key in completed_months:
            if month_key not in loaded_months:
                loaded_months.append(month_key)


# DLT Resource with Schema Contract, Partitioning & Clustering hints
@dlt.resource(
    name="fhv_trips",
    write_disposition="append",
    columns={
        "pickup_datetime": {"data_type": "timestamp", "partition": True},
        "drop_off_datetime": {"data_type": "timestamp"},
        "dispatching_base_num": {"data_type": "text"},
        "p_ulocation_id": {"data_type": "bigint"},
        "d_olocation_id": {"data_type": "bigint"},
        "sr_flag": {"data_type": "text"},
        "affiliated_base_number": {"data_type": "text"},
    }
)
def fhv_trips_resource() -> Iterator[list[dict[str, object]]]:
    state = dlt.current.resource_state()
    loaded_months = state.setdefault("loaded_months", [])
    yield from load_months_concurrently(iter_month_tasks(), loaded_months)


def run() -> None:
    # 1. Load credentials dynamically if available
    credentials = None
    if os.path.exists(GCP_KEY_PATH):
        logger.info("Loading GCP service account credentials from %s", GCP_KEY_PATH)
        try:
            with open(GCP_KEY_PATH, "r") as f:
                credentials = json.load(f)
        except Exception as e:
            logger.error("Failed to load GCP credentials file: %s", str(e))
    else:
        logger.warning("GCP credentials file not found at %s. Falling back to local credentials / config.", GCP_KEY_PATH)

    # 2. Build and run the pipeline
    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination=dlt.destinations.bigquery(credentials=credentials),
        dataset_name=DATASET_NAME,
    )

    logger.info("Starting DLT Pipeline run: %s", PIPELINE_NAME)
    resource = fhv_trips_resource()
    # Apply clustering dynamically using bigquery_adapter to avoid deprecation warnings
    resource = bigquery_adapter(
        resource,
        cluster=["dispatching_base_num", "p_ulocation_id"]
    )
    load_info = pipeline.run(resource)
    logger.info("Pipeline run finished. Load Info details:")
    print(load_info)


if __name__ == "__main__":
    run()
