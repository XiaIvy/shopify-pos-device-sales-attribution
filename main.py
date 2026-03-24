#!/usr/bin/env python3
"""
Pull POS sales for a specific staff member and aggregate totals by POS device.

This script uses Shopify Admin GraphQL Orders API and attributes each matching
POS order to device IDs observed on successful payment transactions.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

from zoneinfo import ZoneInfo


ORDERS_QUERY = """
query FetchOrders($first: Int!, $after: String, $query: String!) {
  orders(first: $first, after: $after, query: $query, sortKey: PROCESSED_AT) {
    pageInfo {
      hasNextPage
      endCursor
    }
    nodes {
      id
      name
      processedAt
      sourceName
      sourceIdentifier
      retailLocation {
        id
        name
      }
      currentTotalPriceSet {
        shopMoney {
          amount
          currencyCode
        }
      }
      transactions {
        id
        kind
        status
        processedAt
        amountSet {
          shopMoney {
            amount
            currencyCode
          }
        }
        device {
          id
        }
      }
      events(first: 40, reverse: true) {
        nodes {
          createdAt
          appTitle
          message
        }
      }
    }
  }
}
"""

POSITIVE_PRIMARY_KINDS = {"SALE", "CAPTURE"}
NEGATIVE_KINDS = {"REFUND", "VOID", "CHANGE"}


POS_PROCESSED_EVENT_RE = re.compile(
  r"^(?P<name>.+?) processed this order(?: for .+?)? on Shopify POS\.$",
  flags=re.IGNORECASE,
)


@dataclass
class DeviceTotals:
  device_id: str
  location_name: str
  register_hint: str
  total_sales: float = 0.0
  orders_count: int = 0
  transactions_count: int = 0


def parse_args() -> argparse.Namespace:
  parser = argparse.ArgumentParser(
    description="Aggregate POS sales by device for one staff member."
  )
  parser.add_argument("--staff", required=True, help="Exact staff full name (e.g. 'Alex Johnson').")
  parser.add_argument("--shop-domain", help="Shop domain, e.g. your-shop.myshopify.com")
  parser.add_argument(
    "--access-token",
    help="Optional direct Admin API token override. If omitted, token exchange uses SHOPIFY_CLIENT_ID + SHOPIFY_CLIENT_SECRET.",
  )
  parser.add_argument("--api-version", default=None, help="Shopify API version (default from env or 2026-01)")
  parser.add_argument("--timezone", default="America/Chicago", help="IANA timezone for --day or naive times")

  date_group = parser.add_mutually_exclusive_group(required=True)
  date_group.add_argument("--day", help="Single local day in YYYY-MM-DD")
  date_group.add_argument(
    "--start",
    help="Start datetime (ISO-8601). If no offset is provided, --timezone is used.",
  )

  parser.add_argument(
    "--end",
    help="End datetime exclusive (ISO-8601). Required when --start is used.",
  )
  parser.add_argument(
    "--match-mode",
    choices=["either", "order", "transaction"],
    default="either",
    help="Staff match mode (timeline-event based in this script build).",
  )
  parser.add_argument(
    "--output",
    default="staff_device_totals.csv",
    help="CSV output path (default: staff_device_totals.csv)",
  )
  parser.add_argument(
    "--include-unknown",
    action="store_true",
    help="Include orders where no device ID was returned.",
  )
  parser.add_argument(
    "--limit",
    type=int,
    default=0,
    help="Optional cap on orders processed (0 = no cap)",
  )
  return parser.parse_args()


def load_dotenv(path: Optional[str] = None) -> None:
  dotenv_path = path or os.path.join(os.getcwd(), ".env")
  if not os.path.exists(dotenv_path):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    dotenv_path = os.path.join(script_dir, ".env")

  if not os.path.exists(dotenv_path):
    return

  with open(dotenv_path, "r", encoding="utf-8") as f:
    for raw in f:
      line = raw.strip()
      if not line or line.startswith("#") or "=" not in line:
        continue
      key, value = line.split("=", 1)
      key = key.strip()
      value = value.strip().strip('"').strip("'")
      os.environ.setdefault(key, value)


def normalize_shop_domain(raw_shop: str) -> str:
  shop = raw_shop.strip()
  if not shop:
    return ""
  if "://" in shop:
    shop = shop.split("://", 1)[1]
  shop = shop.strip().strip("/")
  if not shop.endswith(".myshopify.com"):
    shop = f"{shop}.myshopify.com"
  return shop


def exchange_client_credentials_for_access_token(
  shop_domain: str,
  client_id: str,
  client_secret: str,
) -> str:
  endpoint = f"https://{shop_domain}/admin/oauth/access_token"
  form = urllib.parse.urlencode(
    {
      "grant_type": "client_credentials",
      "client_id": client_id,
      "client_secret": client_secret,
    }
  ).encode("utf-8")

  request = urllib.request.Request(
    endpoint,
    data=form,
    headers={
      "Content-Type": "application/x-www-form-urlencoded",
      "Accept": "application/json",
    },
    method="POST",
  )

  try:
    with urllib.request.urlopen(request, timeout=60) as response:
      body = response.read().decode("utf-8")
      payload = json.loads(body)
  except urllib.error.HTTPError as exc:
    detail = exc.read().decode("utf-8", errors="replace")
    raise RuntimeError(f"Token exchange failed (HTTP {exc.code}): {detail}") from exc
  except urllib.error.URLError as exc:
    raise RuntimeError(f"Token exchange network error: {exc}") from exc

  access_token = (payload.get("access_token") or "").strip()
  if not access_token:
    raise RuntimeError(f"Token exchange response missing access_token: {payload}")

  return access_token


def env_config(args: argparse.Namespace) -> Tuple[str, str, str]:
  load_dotenv()

  shop_domain = normalize_shop_domain(
    args.shop_domain
    or os.getenv("SHOPIFY_SHOP_DOMAIN", "")
    or os.getenv("SHOPIFY_SHOP", "")
  )

  direct_token = (args.access_token or "").strip()
  if not direct_token:
    direct_token = os.getenv("SHOPIFY_ADMIN_ACCESS_TOKEN", "").strip()
  if not direct_token:
    direct_token = os.getenv("SHOPIFY_ACCESS_TOKEN", "").strip()

  client_id = os.getenv("SHOPIFY_CLIENT_ID", "").strip()
  client_secret = os.getenv("SHOPIFY_CLIENT_SECRET", "").strip()

  api_version = (
    args.api_version
    or os.getenv("SHOPIFY_API_VERSION", "").strip()
    or "2026-01"
  )

  missing = []
  if not shop_domain:
    missing.append("SHOPIFY_SHOP_DOMAIN")
  if not direct_token and not (client_id and client_secret):
    missing.append("either an access token or SHOPIFY_CLIENT_ID + SHOPIFY_CLIENT_SECRET")
  if missing:
    raise ValueError(f"Missing config: {', '.join(missing)}")

  if direct_token:
    token = direct_token
  else:
    token = exchange_client_credentials_for_access_token(
      shop_domain=shop_domain,
      client_id=client_id,
      client_secret=client_secret,
    )
    print("Auth: exchanged SHOPIFY_CLIENT_ID + SHOPIFY_CLIENT_SECRET for Admin API access token.")

  return shop_domain, token, api_version


def full_name(person: Optional[dict]) -> str:
  if not person:
    return ""
  first = (person.get("firstName") or "").strip()
  last = (person.get("lastName") or "").strip()
  return " ".join(part for part in [first, last] if part).strip()


def normalize_name(name: str) -> str:
  return re.sub(r"\s+", " ", name.strip().lower())


def parse_time_window(args: argparse.Namespace) -> Tuple[dt.datetime, dt.datetime, str]:
  tz = ZoneInfo(args.timezone)

  if args.day:
    start_local = dt.datetime.strptime(args.day, "%Y-%m-%d").replace(tzinfo=tz)
    end_local = start_local + dt.timedelta(days=1)
  else:
    if not args.end:
      raise ValueError("--end is required when --start is used")
    start_local = parse_datetime(args.start, tz)
    end_local = parse_datetime(args.end, tz)

  if end_local <= start_local:
    raise ValueError("End must be after start")

  start_utc = start_local.astimezone(dt.timezone.utc)
  end_utc = end_local.astimezone(dt.timezone.utc)

  start_utc_str = start_utc.isoformat().replace('+00:00', 'Z')
  end_utc_str = end_utc.isoformat().replace('+00:00', 'Z')

  # Quote timestamp literals so Shopify search parser doesn't misread the ':' segments.
  search = (
    f"source_name:pos "
    f"processed_at:>=\"{start_utc_str}\" "
    f"processed_at:<\"{end_utc_str}\""
  )
  return start_utc, end_utc, search


def parse_datetime(value: str, fallback_tz: ZoneInfo) -> dt.datetime:
  raw = value.strip().replace(" ", "T")
  if raw.endswith("Z"):
    raw = raw[:-1] + "+00:00"
  parsed = dt.datetime.fromisoformat(raw)
  if parsed.tzinfo is None:
    parsed = parsed.replace(tzinfo=fallback_tz)
  return parsed


def gql_request(shop_domain: str, token: str, api_version: str, query: str, variables: dict) -> dict:
  endpoint = f"https://{shop_domain}/admin/api/{api_version}/graphql.json"
  payload = json.dumps({"query": query, "variables": variables}).encode("utf-8")
  request = urllib.request.Request(
    endpoint,
    data=payload,
    headers={
      "Content-Type": "application/json",
      "X-Shopify-Access-Token": token,
    },
    method="POST",
  )

  try:
    with urllib.request.urlopen(request, timeout=60) as response:
      body = response.read().decode("utf-8")
      data = json.loads(body)
  except urllib.error.HTTPError as exc:
    detail = exc.read().decode("utf-8", errors="replace")
    raise RuntimeError(f"Shopify API HTTP {exc.code}: {detail}") from exc

  if data.get("errors"):
    raise RuntimeError(f"Shopify GraphQL errors: {data['errors']}")

  return data.get("data", {})


def fetch_orders(
  shop_domain: str,
  token: str,
  api_version: str,
  search_query: str,
  limit: int = 0,
) -> List[dict]:
  all_orders: List[dict] = []
  cursor: Optional[str] = None

  while True:
    page = gql_request(
      shop_domain,
      token,
      api_version,
      ORDERS_QUERY,
      {"first": 100, "after": cursor, "query": search_query},
    )
    orders_node = page.get("orders") or {}
    nodes = orders_node.get("nodes") or []

    all_orders.extend(nodes)
    if limit and len(all_orders) >= limit:
      return all_orders[:limit]

    page_info = orders_node.get("pageInfo") or {}
    if not page_info.get("hasNextPage"):
      break

    cursor = page_info.get("endCursor")
    time.sleep(0.15)

  return all_orders


def extract_pos_processor_names(order: dict) -> List[str]:
  names: List[str] = []
  events = (order.get("events") or {}).get("nodes") or []
  for event in events:
    message = (event.get("message") or "").strip()
    if not message:
      continue
    match = POS_PROCESSED_EVENT_RE.match(message)
    if match:
      names.append(match.group("name").strip())
  return names


def match_staff(order: dict, target_normalized: str, mode: str) -> bool:
  processor_names = {normalize_name(name) for name in extract_pos_processor_names(order)}

  # In this scope configuration, staff identity comes from POS timeline events.
  timeline_match = target_normalized in processor_names

  if mode in {"order", "transaction", "either"}:
    return timeline_match
  return timeline_match


def choose_sale_transactions(order: dict) -> List[dict]:
  txs = order.get("transactions") or []
  successful = [tx for tx in txs if tx.get("status") == "SUCCESS"]

  primary = [
    tx
    for tx in successful
    if tx.get("kind") in POSITIVE_PRIMARY_KINDS and txn_amount(tx) > 0
  ]
  if primary:
    return primary

  fallback = [
    tx
    for tx in successful
    if tx.get("kind") not in NEGATIVE_KINDS and txn_amount(tx) > 0
  ]
  return fallback


def txn_amount(tx: dict) -> float:
  money = ((tx.get("amountSet") or {}).get("shopMoney") or {})
  try:
    return float(money.get("amount") or 0.0)
  except (TypeError, ValueError):
    return 0.0


def register_hint_from_source_identifier(source_identifier: str) -> str:
  if not source_identifier:
    return ""
  parts = source_identifier.split("-")
  numeric_parts = [p for p in parts if p.isdigit()]
  if not numeric_parts:
    return ""
  if len(parts) >= 3 and parts[-2].isdigit():
    return parts[-2]
  if parts[0].isdigit():
    return parts[0]
  return numeric_parts[0]


def attribute_order_to_devices(order: dict, include_unknown: bool) -> List[Tuple[str, float, int]]:
  """
  Returns a list of tuples: (device_id, attributed_amount, transaction_count)
  """
  total_money = ((order.get("currentTotalPriceSet") or {}).get("shopMoney") or {})
  try:
    order_total = float(total_money.get("amount") or 0.0)
  except (TypeError, ValueError):
    order_total = 0.0

  sale_txs = choose_sale_transactions(order)
  by_device: Dict[str, float] = {}
  by_device_txn_count: Dict[str, int] = {}

  for tx in sale_txs:
    amount = txn_amount(tx)
    device = tx.get("device") or {}
    device_id = device.get("id")
    if not device_id:
      if include_unknown:
        device_id = "UNKNOWN_DEVICE"
      else:
        continue

    by_device[device_id] = by_device.get(device_id, 0.0) + amount
    by_device_txn_count[device_id] = by_device_txn_count.get(device_id, 0) + 1

  if not by_device and include_unknown:
    return [("UNKNOWN_DEVICE", order_total, 0)]
  if not by_device:
    return []

  summed_tx_amount = sum(by_device.values())
  if summed_tx_amount <= 0:
    weight = order_total / len(by_device) if by_device else 0.0
    return [(device_id, weight, by_device_txn_count.get(device_id, 0)) for device_id in by_device]

  result = []
  for device_id, tx_amount in by_device.items():
    attributed = order_total * (tx_amount / summed_tx_amount) if order_total > 0 else tx_amount
    result.append((device_id, attributed, by_device_txn_count.get(device_id, 0)))
  return result


def aggregate(
  orders: Iterable[dict],
  staff_name: str,
  mode: str,
  include_unknown: bool,
) -> Tuple[List[DeviceTotals], int, int]:
  target = normalize_name(staff_name)
  totals: Dict[Tuple[str, str], DeviceTotals] = {}

  matched_orders = 0
  skipped_unattributed = 0

  for order in orders:
    if not match_staff(order, target, mode):
      continue

    matched_orders += 1
    location_name = ((order.get("retailLocation") or {}).get("name") or "Unknown location").strip()
    register_hint = register_hint_from_source_identifier(order.get("sourceIdentifier") or "")

    attributions = attribute_order_to_devices(order, include_unknown=include_unknown)
    if not attributions:
      skipped_unattributed += 1
      continue

    for device_id, attributed_amount, txn_count in attributions:
      key = (device_id, location_name)
      if key not in totals:
        totals[key] = DeviceTotals(
          device_id=device_id,
          location_name=location_name,
          register_hint=register_hint,
        )

      row = totals[key]
      row.total_sales += attributed_amount
      row.orders_count += 1
      row.transactions_count += txn_count
      if not row.register_hint and register_hint:
        row.register_hint = register_hint

  rows = sorted(
    totals.values(),
    key=lambda r: r.total_sales,
    reverse=True,
  )
  return rows, matched_orders, skipped_unattributed


def print_summary(rows: List[DeviceTotals], matched_orders: int, skipped_unattributed: int, staff_name: str) -> None:
  print()
  print(f"Staff: {staff_name}")
  print(f"Matched POS orders: {matched_orders}")
  if skipped_unattributed:
    print(f"Skipped (no device on payment tx): {skipped_unattributed}")

  if not rows:
    print("No device-attributed sales found for this filter.")
    return

  print()
  print("Device totals:")
  print(
    f"{'device_id':<34} {'location':<28} {'register_hint':<14} {'orders':>8} {'txns':>8} {'total_sales':>14}"
  )
  for row in rows:
    print(
      f"{row.device_id:<34} {row.location_name[:28]:<28} {row.register_hint:<14} "
      f"{row.orders_count:>8} {row.transactions_count:>8} {row.total_sales:>14.2f}"
    )


def write_csv(path: str, rows: List[DeviceTotals]) -> None:
  with open(path, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(
      [
        "device_id",
        "location_name",
        "register_hint",
        "orders_count",
        "transactions_count",
        "total_sales",
      ]
    )
    for row in rows:
      writer.writerow(
        [
          row.device_id,
          row.location_name,
          row.register_hint,
          row.orders_count,
          row.transactions_count,
          f"{row.total_sales:.2f}",
        ]
      )


def main() -> int:
  args = parse_args()

  try:
    shop_domain, token, api_version = env_config(args)
    _, _, search_query = parse_time_window(args)
  except Exception as exc:
    print(f"Configuration error: {exc}", file=sys.stderr)
    return 2

  print(f"Query: {search_query}")
  print("Fetching POS orders from Shopify...")

  try:
    orders = fetch_orders(
      shop_domain=shop_domain,
      token=token,
      api_version=api_version,
      search_query=search_query,
      limit=args.limit,
    )
  except Exception as exc:
    print(f"API error: {exc}", file=sys.stderr)
    return 1

  rows, matched_orders, skipped_unattributed = aggregate(
    orders=orders,
    staff_name=args.staff,
    mode=args.match_mode,
    include_unknown=args.include_unknown,
  )

  print_summary(rows, matched_orders, skipped_unattributed, args.staff)
  write_csv(args.output, rows)
  print()
  print(f"Wrote CSV: {args.output}")
  return 0


if __name__ == "__main__":
  raise SystemExit(main())
