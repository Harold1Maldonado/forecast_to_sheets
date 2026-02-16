import os
import time
import logging
from datetime import datetime, timedelta
from collections import defaultdict, Counter
from typing import Any, Dict, List, Tuple, Optional

import requests
from dotenv import load_dotenv

import gspread
from google.oauth2.service_account import Credentials


# -----------------------
# CONFIG
# -----------------------
SHIPSTATION_BASE = "https://ssapi.shipstation.com"
FULFILLMENTS_URL = f"{SHIPSTATION_BASE}/fulfillments"
ORDER_URL = f"{SHIPSTATION_BASE}/orders"

SHEET_ID = "1W5SooGZjqZ83cTLdDmOW6UXoEi9_FLIUKMsRxaIs9oU"
TAB_RAW = "RAW_LINES"
TAB_AGG = "MONTHLY_SKU"
TAB_MATRIX = "FORECAST_MATRIX"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("forecast")


class ConfigError(RuntimeError):
    pass


def require_env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise ConfigError(f"Missing required env var: {name}")
    return v


def iso_date(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def parse_ss_dt(s: str) -> Optional[datetime]:
    if not s:
        return None
    s = s.strip()
    if "." in s:
        left, right = s.split(".", 1)
        digits = "".join(ch for ch in right if ch.isdigit())
        digits = (digits + "000000")[:6]
        s = f"{left}.{digits}"
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def month_key(dt: datetime) -> str:
    return dt.strftime("%Y-%m")


def month_label(dt: datetime) -> str:
    # Formato como en el ejemplo: "February 2026" o corto "Feb 2026"
    fmt = os.environ.get("FORECAST_MONTH_LABEL", "short").strip().lower()
    return dt.strftime("%b %Y") if fmt == "short" else dt.strftime("%B %Y")


def iter_month_starts(start_date: str, end_date: str) -> List[datetime]:
    """
    Devuelve una lista de fechas (1er día del mes) desde start_date hasta end_date inclusive.
    """
    sdt = datetime.fromisoformat(start_date)
    edt = datetime.fromisoformat(end_date)

    cur = sdt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end = edt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    out = []
    while cur <= end:
        out.append(cur)
        # sumar 1 mes sin librerías extra
        y = cur.year + (cur.month // 12)
        m = (cur.month % 12) + 1
        cur = cur.replace(year=y, month=m)
    return out


def shipstation_get(url: str, params: Dict[str, Any], auth: Tuple[str, str], retries: int = 4) -> Dict[str, Any]:
    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, params=params, auth=auth, timeout=(10, 60))
            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                sleep_s = int(retry_after) if retry_after and retry_after.isdigit() else min(
                    30, 2 ** attempt)
                logger.warning(
                    f"429 rate limit. Sleep {sleep_s}s (attempt {attempt}/{retries})")
                time.sleep(sleep_s)
                continue
            r.raise_for_status()
            return r.json() if r.content else {}
        except Exception as e:
            last_err = e
            if attempt < retries:
                sleep_s = min(30, 2 ** attempt)
                logger.warning(
                    f"Request failed: {e} (attempt {attempt}/{retries}) sleep {sleep_s}s")
                time.sleep(sleep_s)
            else:
                raise
    raise last_err  # type: ignore


def list_fulfillments(ship_start: str, ship_end: str, auth: Tuple[str, str], page_size: int = 500) -> List[Dict[str, Any]]:
    page = 1
    out: List[Dict[str, Any]] = []

    while True:
        params = {
            "shipDateStart": ship_start,
            "shipDateEnd": ship_end,
            "sortBy": "ShipDate",
            "sortDir": "ASC",
            "page": page,
            "pageSize": page_size,
        }
        logger.info(
            f"Fetching fulfillments page={page} {ship_start}..{ship_end}")
        data = shipstation_get(FULFILLMENTS_URL, params=params, auth=auth)
        batch = data.get("fulfillments", []) or []
        out.extend(batch)

        if len(batch) < page_size:
            break
        page += 1

    logger.info(f"Fulfillments fetched: {len(out)}")
    return out


def get_order(order_id: Any, auth: Tuple[str, str], cache: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    oid = str(order_id)
    if oid in cache:
        return cache[oid]

    url = f"{ORDER_URL}/{oid}"
    data = shipstation_get(url, params={}, auth=auth)
    cache[oid] = data
    return data


def connect_sheet(service_account_json_path: str):
    creds = Credentials.from_service_account_file(
        service_account_json_path, scopes=SCOPES)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SHEET_ID)


def ensure_tab(sh, title: str, rows: int = 1000, cols: int = 20):
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=rows, cols=cols)


def overwrite_worksheet(ws, values: List[List[Any]]):
    ws.clear()
    if values:
        ws.update(values, value_input_option="RAW")


def best_mpn_for_item(item: Dict[str, Any]) -> str:
    # “MPN” según vuestro criterio previo: warehouseLocation o fulfillmentSku
    return (item.get("warehouseLocation") or item.get("fulfillmentSku") or "").strip()


def main():
    load_dotenv()

    ss_key = require_env("SHIPSTATION_API_KEY")
    ss_secret = require_env("SHIPSTATION_API_SECRET")
    auth = (ss_key, ss_secret)

    sa_path = require_env("GOOGLE_SERVICE_ACCOUNT_JSON")

    months_back = int(os.environ.get("FORECAST_MONTHS_BACK", "12"))
    progress_every = int(os.environ.get("FORECAST_PROGRESS_EVERY", "200"))
    max_orders = int(os.environ.get("FORECAST_MAX_ORDERS", "0"))

    today = datetime.now()
    start_dt = (today - timedelta(days=months_back * 31)
                ).replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    ship_start = os.environ.get("FORECAST_SHIP_START", iso_date(start_dt))
    ship_end = os.environ.get("FORECAST_SHIP_END", iso_date(today))

    logger.info(f"Forecast range shipDate: {ship_start} .. {ship_end}")

    t0 = time.time()
    fulfillments = list_fulfillments(ship_start, ship_end, auth=auth)
    logger.info(f"Phase 1 done (fulfillments). elapsed={time.time()-t0:.1f}s")

    # Agrupar por orderId y tomar shipDate más reciente por orderId
    t1 = time.time()
    order_shipdate: Dict[str, datetime] = {}
    for f in fulfillments:
        oid = f.get("orderId")
        sd = parse_ss_dt(f.get("shipDate") or "")
        if not oid or not sd:
            continue
        oid_s = str(oid)
        prev = order_shipdate.get(oid_s)
        if (prev is None) or (sd > prev):
            order_shipdate[oid_s] = sd

    unique_orders = list(order_shipdate.items())
    logger.info(f"Unique orders from fulfillments: {len(unique_orders)}")
    logger.info(
        f"Phase 2 done (group by orderId). elapsed={time.time()-t1:.1f}s")

    if max_orders and len(unique_orders) > max_orders:
        unique_orders = unique_orders[:max_orders]
        logger.warning(
            f"FORECAST_MAX_ORDERS active -> processing only first {max_orders} orders")

    # Cache de orders
    order_cache: Dict[str, Dict[str, Any]] = {}

    # RAW + AGG
    raw_rows: List[List[Any]] = [["ShipDate", "Month",
                                  "OrderNumber", "OrderId", "SKU", "Qty", "MPN"]]
    agg: Dict[Tuple[str, str], int] = defaultdict(int)

    # Para matriz: SKU x Month -> Qty
    pivot: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))

    # Para MPN por SKU: elegimos el más frecuente observado
    mpn_counter_by_sku: Dict[str, Counter] = defaultdict(Counter)

    # -------------------------
    # Descarga orders (fase lenta) + progreso
    # -------------------------
    t2 = time.time()
    for i, (order_id, ship_dt) in enumerate(unique_orders, start=1):
        order = get_order(order_id, auth=auth, cache=order_cache)
        order_number = order.get("orderNumber") or ""
        mkey = month_key(ship_dt)

        items = order.get("items") or []
        for it in items:
            sku = (it.get("sku") or "").strip()
            if not sku:
                continue

            raw_qty = it.get("quantity", 0)
            try:
                qty = int(float(raw_qty))
            except (TypeError, ValueError):
                qty = 0

            mpn = best_mpn_for_item(it)

            raw_rows.append([
                ship_dt.strftime("%Y-%m-%d"),
                mkey,
                order_number,
                str(order_id),
                sku,
                qty,
                mpn,
            ])

            agg[(mkey, sku)] += qty
            pivot[sku][mkey] += qty

            if mpn:
                mpn_counter_by_sku[sku][mpn] += 1

        if progress_every and (i % progress_every == 0):
            logger.info(
                f"Processed {i}/{len(unique_orders)} orders | cached={len(order_cache)} | raw_lines={len(raw_rows)-1}"
            )

    logger.info(f"Phase 3 done (orders->lines). elapsed={time.time()-t2:.1f}s")

    # AGG rows (long)
    agg_rows: List[List[Any]] = [["Month", "SKU", "QtyShipped"]]
    for (m, sku), qty in sorted(agg.items(), key=lambda x: (x[0][0], x[0][1])):
        agg_rows.append([m, sku, qty])

    # MATRIX rows (wide)
    month_starts = iter_month_starts(ship_start, ship_end)
    month_keys = [dt.strftime("%Y-%m") for dt in month_starts]
    month_labels = [month_label(dt) for dt in month_starts]

    matrix_rows: List[List[Any]] = []
    header = ["SKU", "MPN"] + month_labels
    matrix_rows.append(header)

    for sku in sorted(pivot.keys()):
        # MPN “mejor” (más frecuente)
        mpn = ""
        if sku in mpn_counter_by_sku and mpn_counter_by_sku[sku]:
            mpn = mpn_counter_by_sku[sku].most_common(1)[0][0]

        row = [sku, mpn]
        sku_months = pivot.get(sku, {})
        for mk in month_keys:
            row.append(int(sku_months.get(mk, 0)))
        matrix_rows.append(row)

    # -------------------------
    # Write to Google Sheets
    # -------------------------
    t3 = time.time()
    sh = connect_sheet(sa_path)

    ws_raw = ensure_tab(sh, TAB_RAW, rows=max(
        1000, len(raw_rows) + 100), cols=12)
    ws_agg = ensure_tab(sh, TAB_AGG, rows=max(
        1000, len(agg_rows) + 100), cols=10)
    ws_matrix = ensure_tab(sh, TAB_MATRIX, rows=max(
        1000, len(matrix_rows) + 100), cols=max(10, len(header) + 2))

    logger.info(f"Writing {TAB_RAW} rows={len(raw_rows)-1}")
    overwrite_worksheet(ws_raw, raw_rows)

    logger.info(f"Writing {TAB_AGG} rows={len(agg_rows)-1}")
    overwrite_worksheet(ws_agg, agg_rows)

    logger.info(
        f"Writing {TAB_MATRIX} rows={len(matrix_rows)-1} cols={len(header)}")
    overwrite_worksheet(ws_matrix, matrix_rows)

    logger.info(
        f"Done. RAW_LINES rows={len(raw_rows)-1}, MONTHLY_SKU rows={len(agg_rows)-1}, "
        f"FORECAST_MATRIX rows={len(matrix_rows)-1} | write_elapsed={time.time()-t3:.1f}s"
    )
    logger.info(f"TOTAL elapsed={time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
