#!/usr/bin/env python3
"""Clean a Squarespace order export and optionally import it into Shopify.

Default behavior writes a cleaned UTF-8 CSV that is easier to inspect and use
for migration tooling. With `--import-to-shopify`, the same script can also
build `orderCreate` payloads for Shopify's Admin GraphQL API and optionally
submit them.

The importer is intentionally conservative:
- it always keeps inventory behavior at `BYPASS`
- it never sends order or fulfillment receipts
- it keeps a local state file so reruns can skip already imported orders
- it can dry-run payload generation before any API write happens

Examples:
    python3 scripts/squarespace_orders_to_shopify.py \
      /path/to/orders.csv \
      -o /path/to/orders.shopify-history.csv

    python3 scripts/squarespace_orders_to_shopify.py \
      /path/to/orders.csv \
      --import-to-shopify \
      --payload-output /tmp/shopify-order-payloads.json \
      --max-orders 5

    SHOPIFY_STORE_DOMAIN=example-dev.myshopify.com \
    SHOPIFY_ADMIN_ACCESS_TOKEN=shpat_xxx \
    python3 scripts/squarespace_orders_to_shopify.py \
      /path/to/orders.csv \
      --import-to-shopify \
      --apply \
      --max-orders 5
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib import error, request


KNOWN_LINEITEM_FIELDS = {
    "Lineitem quantity",
    "Lineitem name",
    "Lineitem price",
    "Lineitem sku",
    "Lineitem variant",
    "Lineitem requires shipping",
    "Lineitem taxable",
    "Lineitem fulfillment status",
}

DATE_FIELDS = {
    "Paid at",
    "Fulfilled at",
    "Created at",
    "Cancelled at",
}

MONEY_FIELDS = {
    "Subtotal",
    "Shipping",
    "Taxes",
    "Amount Refunded",
    "Total",
    "Discount Amount",
    "Lineitem price",
}

BOOLEAN_FIELDS = {
    "Lineitem requires shipping",
    "Lineitem taxable",
}

STATUS_VALUE_MAP = {
    "paid": "paid",
    "pending": "pending",
    "authorized": "authorized",
    "refunded": "refunded",
    "partially_refunded": "partially_refunded",
    "partially_paid": "partially_paid",
    "voided": "voided",
    "fulfilled": "fulfilled",
    "unfulfilled": "unfulfilled",
    "partial": "partial",
    "partially_fulfilled": "partial",
    "restocked": "restocked",
    "cancelled": "cancelled",
    "canceled": "cancelled",
    "on_hold": "on_hold",
    "scheduled": "scheduled",
    "in_progress": "in_progress",
    "open": "open",
}

COUNTRY_ALIASES = {
    "united states": "US",
    "usa": "US",
    "us": "US",
    "canada": "CA",
    "ca": "CA",
    "australia": "AU",
    "au": "AU",
    "united kingdom": "GB",
    "uk": "GB",
    "great britain": "GB",
    "england": "GB",
    "germany": "DE",
    "de": "DE",
    "switzerland": "CH",
    "ch": "CH",
    "france": "FR",
    "fr": "FR",
    "belgium": "BE",
    "be": "BE",
    "norway": "NO",
    "no": "NO",
    "spain": "ES",
    "es": "ES",
    "ireland": "IE",
    "ie": "IE",
    "netherlands": "NL",
    "nl": "NL",
    "new zealand": "NZ",
    "nz": "NZ",
    "japan": "JP",
    "jp": "JP",
    "mexico": "MX",
    "mx": "MX",
}

MONEY_QUANTUM = Decimal("0.01")
DEFAULT_SHOPIFY_API_VERSION = "2026-01"
DEFAULT_IMPORT_INTERVAL_SECONDS = 12.5

PRODUCT_VARIANTS_QUERY = """
query ProductVariantsForSkuMap($first: Int!, $after: String) {
  productVariants(first: $first, after: $after) {
    edges {
      cursor
      node {
        id
        sku
        title
        product {
          id
          title
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

ORDER_CREATE_MUTATION = """
mutation OrderCreate($order: OrderCreateOrderInput!, $options: OrderCreateOptionsInput) {
  orderCreate(order: $order, options: $options) {
    order {
      id
      name
      legacyResourceId
    }
    userErrors {
      field
      message
    }
  }
}
"""

ORDER_LOOKUP_QUERY = """
query OrderLookup($id: ID!) {
  order(id: $id) {
    id
    name
    test
    closed
    closedAt
    cancelledAt
  }
}
"""

ORDER_CLOSE_MUTATION = """
mutation OrderClose($input: OrderCloseInput!) {
  orderClose(input: $input) {
    order {
      id
      name
      closed
      closedAt
    }
    userErrors {
      field
      message
    }
  }
}
"""

ORDER_DELETE_MUTATION = """
mutation OrderDelete($orderId: ID!) {
  orderDelete(orderId: $orderId) {
    deletedId
    userErrors {
      field
      message
      code
    }
  }
}
"""


@dataclass
class Summary:
    rows_read: int = 0
    rows_written: int = 0
    orders_seen: int = 0
    fill_down_rows: int = 0
    fill_down_fields: int = 0
    dropped_tax_columns: int = 0
    blank_email_orders: int = 0
    blank_shipping_name_orders: int = 0
    blank_sku_rows: int = 0


@dataclass
class OrderGroup:
    order_id: str
    rows: list[dict[str, str]]


@dataclass
class VariantRecord:
    id: str
    sku: str
    product_id: str
    product_title: str
    variant_title: str


@dataclass
class ShopifyCredentials:
    shop_domain: str
    api_version: str
    access_token: str = ""
    client_id: str = ""
    client_secret: str = ""


@dataclass
class OrderBuildResult:
    order_input: dict[str, Any]
    variant_backed_line_items: int = 0
    custom_line_items: int = 0
    unmatched_skus: set[str] = field(default_factory=set)


@dataclass
class ImportSummary:
    selected_orders: int = 0
    skipped_state_orders: int = 0
    dry_run_orders: int = 0
    attempted_orders: int = 0
    imported_orders: int = 0
    failed_orders: int = 0
    variant_backed_line_items: int = 0
    custom_line_items: int = 0
    customer_retry_without_upsert: int = 0
    deleted_test_orders_before_import: int = 0
    skipped_non_test_cleanup_orders: int = 0
    unmatched_skus: set[str] = field(default_factory=set)
    failures: list[str] = field(default_factory=list)


class ShopifyGraphQLError(RuntimeError):
    """Raised when Shopify returns a transport or GraphQL error."""


class ShopifyGraphQLClient:
    def __init__(self, credentials: ShopifyCredentials):
        self.credentials = credentials
        self.endpoint = (
            f"https://{credentials.shop_domain}/admin/api/"
            f"{credentials.api_version}/graphql.json"
        )
        self._access_token = credentials.access_token

    def get_access_token(self) -> str:
        if self._access_token:
            return self._access_token

        if not self.credentials.client_id or not self.credentials.client_secret:
            raise ShopifyGraphQLError(
                "Missing Shopify credentials. Provide an access token or a client ID/secret pair."
            )

        form_body = urlencode(
            {
                "grant_type": "client_credentials",
                "client_id": self.credentials.client_id,
                "client_secret": self.credentials.client_secret,
            }
        ).encode("utf-8")
        token_endpoint = f"https://{self.credentials.shop_domain}/admin/oauth/access_token"
        req = request.Request(
            token_endpoint,
            data=form_body,
            method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

        try:
            with request.urlopen(req) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except error.HTTPError as exc:
            details = exc.read().decode("utf-8", errors="replace")
            raise ShopifyGraphQLError(
                f"Shopify token request failed with HTTP {exc.code}: {details or exc.reason}"
            ) from exc
        except error.URLError as exc:
            raise ShopifyGraphQLError(f"Shopify token connection error: {exc.reason}") from exc

        token = (payload.get("access_token") or "").strip()
        if not token:
            raise ShopifyGraphQLError("Shopify token response did not include access_token.")
        self._access_token = token
        return token

    def execute(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        body = json.dumps({"query": query, "variables": variables}).encode("utf-8")
        req = request.Request(
            self.endpoint,
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "X-Shopify-Access-Token": self.get_access_token(),
            },
        )

        try:
            with request.urlopen(req) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except error.HTTPError as exc:
            details = exc.read().decode("utf-8", errors="replace")
            raise ShopifyGraphQLError(
                f"Shopify HTTP {exc.code}: {details or exc.reason}"
            ) from exc
        except error.URLError as exc:
            raise ShopifyGraphQLError(f"Shopify connection error: {exc.reason}") from exc

        graphql_errors = payload.get("errors") or []
        if graphql_errors:
            messages = ", ".join(err.get("message", "Unknown GraphQL error") for err in graphql_errors)
            raise ShopifyGraphQLError(messages)

        return payload.get("data") or {}

    def load_variant_lookup(self, warnings: list[str]) -> dict[str, VariantRecord]:
        lookup: dict[str, VariantRecord] = {}
        cursor: str | None = None
        duplicate_skus: set[str] = set()

        while True:
            data = self.execute(PRODUCT_VARIANTS_QUERY, {"first": 250, "after": cursor})
            connection = (data.get("productVariants") or {})
            edges = connection.get("edges") or []
            for edge in edges:
                node = edge.get("node") or {}
                sku = (node.get("sku") or "").strip()
                if not sku:
                    continue
                record = VariantRecord(
                    id=node.get("id", ""),
                    sku=sku,
                    product_id=((node.get("product") or {}).get("id") or "").strip(),
                    product_title=((node.get("product") or {}).get("title") or "").strip(),
                    variant_title=(node.get("title") or "").strip(),
                )
                if sku in lookup:
                    duplicate_skus.add(sku)
                    continue
                lookup[sku] = record

            page_info = connection.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
            if not cursor:
                break

        if duplicate_skus:
            warnings.append(
                "Shopify variant lookup found duplicate SKUs; using the first variant for: "
                + ", ".join(sorted(duplicate_skus))
            )

        return lookup


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Clean a Squarespace orders CSV and optionally import it into Shopify."
    )
    parser.add_argument("input_csv", type=Path, help="Path to the Squarespace orders CSV.")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="Cleaned CSV output path. Defaults to '<input>.shopify-history.csv'.",
    )
    parser.add_argument(
        "--keep-tax-breakdown",
        action="store_true",
        help="Retain the granular city/county tax columns instead of dropping them.",
    )
    parser.add_argument(
        "--keep-status-case",
        action="store_true",
        help="Do not normalize financial and fulfillment statuses.",
    )
    parser.add_argument(
        "--keep-date-format",
        action="store_true",
        help="Do not rewrite date fields as ISO 8601 timestamps.",
    )
    parser.add_argument(
        "--import-to-shopify",
        action="store_true",
        help="Build Shopify orderCreate payloads after cleaning the CSV.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually submit orderCreate mutations. Without this flag, import mode is dry-run only.",
    )
    parser.add_argument(
        "--shop-domain",
        default="",
        help="Shopify shop domain. Falls back to SHOPIFY_STORE_DOMAIN or SHOPIFY_SHOP_DOMAIN.",
    )
    parser.add_argument(
        "--access-token",
        default="",
        help=(
            "Shopify Admin API access token. Falls back to SHOPIFY_ADMIN_ACCESS_TOKEN "
            "or SHOPIFY_ACCESS_TOKEN."
        ),
    )
    parser.add_argument(
        "--api-version",
        default="",
        help=f"Shopify Admin API version. Defaults to {DEFAULT_SHOPIFY_API_VERSION}.",
    )
    parser.add_argument(
        "--payload-output",
        type=Path,
        help="Optional JSON file for dry-run payload inspection.",
    )
    parser.add_argument(
        "--state-file",
        type=Path,
        help="JSON file used to remember successfully imported order IDs.",
    )
    parser.add_argument(
        "--ignore-state",
        action="store_true",
        help="Ignore the local state file and attempt all orders again.",
    )
    parser.add_argument(
        "--delete-tracked-test-orders-before-import",
        action="store_true",
        help=(
            "Before import selection, close and delete previously imported Shopify test orders "
            "recorded in the local state file."
        ),
    )
    parser.add_argument(
        "--max-orders",
        type=int,
        help="Limit import mode to the first N not-yet-imported orders.",
    )
    parser.add_argument(
        "--min-interval-seconds",
        type=float,
        default=DEFAULT_IMPORT_INTERVAL_SECONDS,
        help=(
            "Delay between submitted orderCreate mutations. Defaults to 12.5s, which stays "
            "under Shopify's documented dev-store `orderCreate` limit of five new orders per minute."
        ),
    )
    parser.add_argument(
        "--skip-variant-lookup",
        action="store_true",
        help="Do not query Shopify variants by SKU. All line items will import as custom line items.",
    )
    parser.add_argument(
        "--customer-mode",
        choices=("auto", "upsert", "email-only", "none"),
        default="auto",
        help=(
            "How to attach customers to imported orders. `auto` tries customer upsert first and "
            "retries email-only if Shopify rejects the customer payload."
        ),
    )
    parser.add_argument(
        "--order-tag-prefix",
        default="squarespace-import",
        help="Prefix for tags added to imported Shopify orders.",
    )
    parser.add_argument(
        "--order-name-prefix",
        default="",
        help="Optional prefix for the Shopify order name, for example 'SSQ-'.",
    )
    parser.add_argument(
        "--test-orders",
        action="store_true",
        help="Create imported orders as Shopify test orders.",
    )
    parser.add_argument(
        "--stop-on-error",
        action="store_true",
        help="Stop immediately when one Shopify order import fails.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    input_csv = args.input_csv.expanduser().resolve()
    output_csv = (
        args.output.expanduser().resolve()
        if args.output
        else input_csv.with_name(f"{input_csv.stem}.shopify-history.csv")
    )

    if not input_csv.exists():
        print(f"Input file not found: {input_csv}", file=sys.stderr)
        return 1

    with input_csv.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            print(f"Input CSV has no header row: {input_csv}", file=sys.stderr)
            return 1
        source_headers = reader.fieldnames
        source_rows = list(reader)

    if "Order ID" not in source_headers:
        print("Input CSV is missing required column: Order ID", file=sys.stderr)
        return 1

    tax_breakdown_headers = [
        header for header in source_headers if header.endswith(" Tax") and header != "Taxes"
    ]
    output_headers = (
        list(source_headers)
        if args.keep_tax_breakdown
        else [header for header in source_headers if header not in tax_breakdown_headers]
    )
    order_level_headers = [
        header
        for header in source_headers
        if header not in KNOWN_LINEITEM_FIELDS and header not in tax_breakdown_headers
    ]

    warnings: list[str] = []
    summary = Summary(
        rows_read=len(source_rows),
        dropped_tax_columns=0 if args.keep_tax_breakdown else len(tax_breakdown_headers),
    )
    cleaned_rows = clean_rows(
        source_rows=source_rows,
        source_headers=source_headers,
        order_level_headers=order_level_headers,
        keep_tax_breakdown=args.keep_tax_breakdown,
        keep_status_case=args.keep_status_case,
        keep_date_format=args.keep_date_format,
        warnings=warnings,
        summary=summary,
    )

    with output_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=output_headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(cleaned_rows)

    print(
        f"Wrote {summary.rows_written} cleaned rows for {summary.orders_seen} orders to {output_csv}"
    )
    print(
        "Note: Shopify admin does not support native historical order CSV imports; "
        "the importer mode below uses the Admin GraphQL API instead."
    )
    if summary.fill_down_rows:
        print(
            f"- Filled down {summary.fill_down_fields} order-level fields across "
            f"{summary.fill_down_rows} multi-line order rows."
        )
    if summary.dropped_tax_columns:
        print(
            f"- Dropped {summary.dropped_tax_columns} tax breakdown columns "
            "while preserving total Taxes."
        )
    if summary.blank_email_orders:
        print(f"- {summary.blank_email_orders} orders still have a blank Email after cleanup.")
    if summary.blank_shipping_name_orders:
        print(
            f"- {summary.blank_shipping_name_orders} orders still have a blank Shipping Name "
            "after cleanup."
        )
    if summary.blank_sku_rows:
        print(f"- {summary.blank_sku_rows} line items still have a blank SKU.")

    if args.import_to_shopify:
        import_summary = run_shopify_import(
            cleaned_rows=cleaned_rows,
            input_csv=input_csv,
            args=args,
            warnings=warnings,
        )
        print_shopify_import_summary(import_summary, args.apply)

    if warnings:
        print("Warnings:", file=sys.stderr)
        for warning in warnings:
            print(f"- {warning}", file=sys.stderr)

    return 0


def clean_rows(
    *,
    source_rows: list[dict[str, str]],
    source_headers: list[str],
    order_level_headers: list[str],
    keep_tax_breakdown: bool,
    keep_status_case: bool,
    keep_date_format: bool,
    warnings: list[str],
    summary: Summary,
) -> list[dict[str, str]]:
    cleaned_rows: list[dict[str, str]] = []
    order_cache: dict[str, dict[str, str]] = {}
    seen_orders: set[str] = set()
    blank_email_orders: set[str] = set()
    blank_shipping_name_orders: set[str] = set()

    for line_number, raw_row in enumerate(source_rows, start=2):
        row = {header: (raw_row.get(header, "") or "").strip() for header in source_headers}

        if not any(row.values()):
            continue

        order_id = row.get("Order ID", "")
        if not order_id:
            warnings.append(f"Line {line_number}: missing Order ID; row skipped.")
            continue

        seen_orders.add(order_id)
        row_filled = False

        cached_order = order_cache.get(order_id)
        if cached_order is None:
            cached_order = {header: row.get(header, "") for header in order_level_headers}
            order_cache[order_id] = cached_order
        else:
            for header in order_level_headers:
                value = row.get(header, "")
                cached_value = cached_order.get(header, "")
                if value:
                    if cached_value and cached_value != value:
                        warnings.append(
                            f"Line {line_number}: order {order_id} has conflicting values for "
                            f"{header!r}; keeping the most recent non-blank value."
                        )
                    cached_order[header] = value
                    continue
                if cached_value:
                    row[header] = cached_value
                    summary.fill_down_fields += 1
                    row_filled = True

        if row_filled:
            summary.fill_down_rows += 1

        normalized_row: dict[str, str] = {}
        for header in source_headers:
            if not keep_tax_breakdown and header.endswith(" Tax") and header != "Taxes":
                continue

            value = row.get(header, "")
            if header in BOOLEAN_FIELDS:
                value = normalize_boolean(value)
            elif header in MONEY_FIELDS:
                value = normalize_money(value, line_number=line_number, header=header, warnings=warnings)
            elif header == "Lineitem quantity":
                value = normalize_quantity(value, line_number=line_number, warnings=warnings)
            elif header in DATE_FIELDS and not keep_date_format:
                value = normalize_date(value, line_number=line_number, header=header, warnings=warnings)
            elif (
                header in {"Financial Status", "Fulfillment Status", "Lineitem fulfillment status"}
                and not keep_status_case
            ):
                value = normalize_status(value)

            normalized_row[header] = value

        if not normalized_row.get("Email"):
            blank_email_orders.add(order_id)
        if not normalized_row.get("Shipping Name"):
            blank_shipping_name_orders.add(order_id)
        if normalized_row.get("Lineitem name") and not normalized_row.get("Lineitem sku"):
            summary.blank_sku_rows += 1

        cleaned_rows.append(normalized_row)

    summary.rows_written = len(cleaned_rows)
    summary.orders_seen = len(seen_orders)
    summary.blank_email_orders = len(blank_email_orders)
    summary.blank_shipping_name_orders = len(blank_shipping_name_orders)
    return cleaned_rows


def run_shopify_import(
    *,
    cleaned_rows: list[dict[str, str]],
    input_csv: Path,
    args: argparse.Namespace,
    warnings: list[str],
) -> ImportSummary:
    import_summary = ImportSummary()
    order_groups = group_orders(cleaned_rows)
    state_path = (
        args.state_file.expanduser().resolve()
        if args.state_file
        else input_csv.with_name(f"{input_csv.stem}.shopify-import-state.json")
    )
    state = load_import_state(state_path, warnings)
    credentials = resolve_shopify_credentials(args)
    client = ShopifyGraphQLClient(credentials) if credentials else None

    if args.delete_tracked_test_orders_before_import:
        if client is None:
            raise SystemExit(
                "Shopify credentials are required to delete tracked test orders. "
                "Set SHOPIFY_STORE plus an access token or client ID/secret."
            )
        deleted_count, skipped_non_test = delete_tracked_test_orders_before_import(
            client=client,
            state=state,
            state_path=state_path,
            warnings=warnings,
        )
        import_summary.deleted_test_orders_before_import = deleted_count
        import_summary.skipped_non_test_cleanup_orders = skipped_non_test

    already_imported = set() if args.ignore_state else set((state.get("imports") or {}).keys())
    selected_orders: list[OrderGroup] = []
    for order_group in order_groups:
        if order_group.order_id in already_imported:
            import_summary.skipped_state_orders += 1
            continue
        selected_orders.append(order_group)

    if args.max_orders is not None:
        selected_orders = selected_orders[: max(args.max_orders, 0)]
    import_summary.selected_orders = len(selected_orders)

    if not selected_orders:
        return import_summary

    variant_lookup: dict[str, VariantRecord] = {}

    if not args.skip_variant_lookup:
        if client is None:
            warnings.append(
                "No Shopify credentials were provided, so variant lookup was skipped and "
                "line items will be built as custom items in dry-run mode."
            )
        else:
            try:
                variant_lookup = client.load_variant_lookup(warnings)
                print(f"Loaded {len(variant_lookup)} Shopify variants for SKU matching.")
            except ShopifyGraphQLError as exc:
                warnings.append(
                    "Shopify variant lookup failed; continuing with custom line items only: "
                    f"{exc}"
                )

    payload_preview: list[dict[str, Any]] = []
    for order_group in selected_orders:
        build_result = build_order_input(
            order_group=order_group,
            variant_lookup=variant_lookup,
            customer_mode=args.customer_mode,
            order_tag_prefix=args.order_tag_prefix,
            order_name_prefix=args.order_name_prefix.strip(),
            test_orders=args.test_orders,
            warnings=warnings,
        )
        import_summary.variant_backed_line_items += build_result.variant_backed_line_items
        import_summary.custom_line_items += build_result.custom_line_items
        import_summary.unmatched_skus.update(build_result.unmatched_skus)
        payload_preview.append(
            {
                "order_id": order_group.order_id,
                "order": build_result.order_input,
                "options": build_order_create_options(),
            }
        )

    if args.payload_output:
        payload_path = args.payload_output.expanduser().resolve()
        write_json(payload_path, payload_preview)
        print(f"Wrote Shopify payload preview to {payload_path}")

    if not args.apply:
        import_summary.dry_run_orders = len(payload_preview)
        return import_summary

    if client is None:
        raise SystemExit(
            "Shopify credentials are required with --apply. Set --shop-domain / --access-token "
            "or export SHOPIFY_STORE_DOMAIN and SHOPIFY_ADMIN_ACCESS_TOKEN."
        )

    state.setdefault("imports", {})
    state["shop_domain"] = credentials.shop_domain
    state["api_version"] = credentials.api_version

    for index, payload in enumerate(payload_preview, start=1):
        order_id = payload["order_id"]
        import_summary.attempted_orders += 1
        if index > 1 and args.min_interval_seconds > 0:
            time.sleep(args.min_interval_seconds)

        order_input = payload["order"]
        used_customer_retry = False
        try:
            created_order, used_customer_retry = submit_order_create(
                client=client,
                order_input=order_input,
                customer_mode=args.customer_mode,
            )
        except ShopifyGraphQLError as exc:
            import_summary.failed_orders += 1
            message = f"{order_id}: {exc}"
            import_summary.failures.append(message)
            print(f"Failed Shopify import for order {order_id}: {exc}", file=sys.stderr)
            if args.stop_on_error:
                break
            continue

        if used_customer_retry:
            import_summary.customer_retry_without_upsert += 1

        import_summary.imported_orders += 1
        state["imports"][order_id] = {
            "shopify_order_gid": created_order.get("id", ""),
            "shopify_order_name": created_order.get("name", ""),
            "shopify_legacy_resource_id": created_order.get("legacyResourceId", ""),
            "imported_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        write_json(state_path, state)
        print(
            f"Imported Squarespace order {order_id} -> Shopify {created_order.get('name', created_order.get('id', ''))}"
        )

    return import_summary


def submit_order_create(
    *,
    client: ShopifyGraphQLClient,
    order_input: dict[str, Any],
    customer_mode: str,
) -> tuple[dict[str, Any], bool]:
    variables = {"order": order_input, "options": build_order_create_options()}
    data = client.execute(ORDER_CREATE_MUTATION, variables)
    payload = data.get("orderCreate") or {}
    user_errors = payload.get("userErrors") or []
    order = payload.get("order")

    if not user_errors and order:
        return order, False

    if (
        order_input.get("customer")
        and customer_mode == "auto"
        and user_errors
        and is_customer_related_error(user_errors)
    ):
        fallback_order = dict(order_input)
        fallback_order.pop("customer", None)
        fallback_variables = {"order": fallback_order, "options": build_order_create_options()}
        fallback_data = client.execute(ORDER_CREATE_MUTATION, fallback_variables)
        fallback_payload = fallback_data.get("orderCreate") or {}
        fallback_errors = fallback_payload.get("userErrors") or []
        fallback_order_result = fallback_payload.get("order")
        if not fallback_errors and fallback_order_result:
            return fallback_order_result, True
        raise ShopifyGraphQLError(format_user_errors(fallback_errors or user_errors))

    raise ShopifyGraphQLError(format_user_errors(user_errors))


def delete_tracked_test_orders_before_import(
    *,
    client: ShopifyGraphQLClient,
    state: dict[str, Any],
    state_path: Path,
    warnings: list[str],
) -> tuple[int, int]:
    imports = state.get("imports") or {}
    if not imports:
        return 0, 0

    deleted_count = 0
    skipped_non_test = 0

    for squarespace_order_id, entry in list(imports.items()):
        order_gid = str(entry.get("shopify_order_gid", "")).strip()
        if not order_gid:
            warnings.append(
                f"State entry for Squarespace order {squarespace_order_id} is missing shopify_order_gid; removing it."
            )
            imports.pop(squarespace_order_id, None)
            write_json(state_path, state)
            continue

        order_data = fetch_order_for_cleanup(client, order_gid)
        order = order_data.get("order")
        if order is None:
            warnings.append(
                f"Tracked Shopify order {order_gid} for Squarespace order {squarespace_order_id} no longer exists; removing state entry."
            )
            imports.pop(squarespace_order_id, None)
            write_json(state_path, state)
            continue

        if not order.get("test"):
            skipped_non_test += 1
            warnings.append(
                f"Tracked Shopify order {order.get('name', order_gid)} is not a test order, so it was not deleted."
            )
            continue

        if not order.get("closed"):
            close_order_for_cleanup(client, order_gid)

        delete_order_for_cleanup(client, order_gid)
        imports.pop(squarespace_order_id, None)
        write_json(state_path, state)
        deleted_count += 1
        print(
            f"Deleted tracked Shopify test order {order.get('name', order_gid)} for Squarespace order {squarespace_order_id}."
        )

    return deleted_count, skipped_non_test


def fetch_order_for_cleanup(client: ShopifyGraphQLClient, order_gid: str) -> dict[str, Any]:
    data = client.execute(ORDER_LOOKUP_QUERY, {"id": order_gid})
    return data


def close_order_for_cleanup(client: ShopifyGraphQLClient, order_gid: str) -> None:
    data = client.execute(ORDER_CLOSE_MUTATION, {"input": {"id": order_gid}})
    payload = data.get("orderClose") or {}
    user_errors = payload.get("userErrors") or []
    if user_errors:
        raise ShopifyGraphQLError(format_user_errors(user_errors))


def delete_order_for_cleanup(client: ShopifyGraphQLClient, order_gid: str) -> None:
    data = client.execute(ORDER_DELETE_MUTATION, {"orderId": order_gid})
    payload = data.get("orderDelete") or {}
    user_errors = payload.get("userErrors") or []
    if user_errors:
        raise ShopifyGraphQLError(format_user_errors(user_errors))


def build_order_input(
    *,
    order_group: OrderGroup,
    variant_lookup: dict[str, VariantRecord],
    customer_mode: str,
    order_tag_prefix: str,
    order_name_prefix: str,
    test_orders: bool,
    warnings: list[str],
) -> OrderBuildResult:
    first_row = order_group.rows[0]
    currency = (first_row.get("Currency") or "USD").strip() or "USD"

    line_items, variant_count, custom_count, unmatched_skus = build_line_items(
        order_group=order_group,
        variant_lookup=variant_lookup,
        currency=currency,
        warnings=warnings,
    )
    if not line_items:
        raise ShopifyGraphQLError(
            f"Order {order_group.order_id} has no line items after cleanup; cannot import."
        )

    order_input: dict[str, Any] = {
        "currency": currency,
        "email": first_row.get("Email", ""),
        "lineItems": line_items,
        "processedAt": first_row.get("Created at", ""),
        "sourceIdentifier": order_group.order_id,
        "tags": build_order_tags(order_tag_prefix, order_group.order_id),
        "test": test_orders,
    }

    if order_name_prefix:
        order_input["name"] = f"{order_name_prefix}{order_group.order_id}"

    note_value = build_order_note(first_row)
    if note_value:
        order_input["note"] = note_value

    custom_attributes = build_order_custom_attributes(first_row, order_group.order_id)
    if custom_attributes:
        order_input["customAttributes"] = custom_attributes

    phone_value = choose_primary_phone(first_row)
    if phone_value:
        order_input["phone"] = phone_value

    shipping_address = build_order_address(first_row, "Shipping")
    if shipping_address:
        order_input["shippingAddress"] = shipping_address

    billing_address = build_order_address(first_row, "Billing")
    if billing_address:
        order_input["billingAddress"] = billing_address

    shipping_lines = build_shipping_lines(first_row, currency)
    if shipping_lines:
        order_input["shippingLines"] = shipping_lines

    discount_code = build_discount_code(first_row, currency)
    if discount_code:
        order_input["discountCode"] = discount_code

    tax_lines = build_tax_lines(first_row, currency, order_group.rows)
    if tax_lines:
        order_input["taxLines"] = tax_lines

    transactions = build_transactions(first_row, currency)
    if transactions:
        order_input["transactions"] = transactions

    financial_status = map_financial_status(first_row)
    if financial_status:
        order_input["financialStatus"] = financial_status

    fulfillment_status = map_fulfillment_status(first_row)
    if fulfillment_status:
        order_input["fulfillmentStatus"] = fulfillment_status

    closed_at = first_row.get("Cancelled at", "").strip()
    if closed_at:
        order_input["closedAt"] = closed_at

    customer_payload = build_customer_payload(first_row, customer_mode)
    if customer_payload:
        order_input["customer"] = customer_payload

    compact = compact_object(order_input)
    return OrderBuildResult(
        order_input=compact,
        variant_backed_line_items=variant_count,
        custom_line_items=custom_count,
        unmatched_skus=unmatched_skus,
    )


def build_line_items(
    *,
    order_group: OrderGroup,
    variant_lookup: dict[str, VariantRecord],
    currency: str,
    warnings: list[str],
) -> tuple[list[dict[str, Any]], int, int, set[str]]:
    line_items: list[dict[str, Any]] = []
    variant_count = 0
    custom_count = 0
    unmatched_skus: set[str] = set()

    for row in order_group.rows:
        quantity_value = row.get("Lineitem quantity", "").strip()
        try:
            quantity = int(quantity_value or "0")
        except ValueError:
            raise ShopifyGraphQLError(
                f"Order {order_group.order_id} has invalid line item quantity {quantity_value!r}."
            )

        if quantity <= 0:
            warnings.append(
                f"Order {order_group.order_id} has non-positive quantity {quantity_value!r}; "
                "skipping that line item."
            )
            continue

        sku = row.get("Lineitem sku", "").strip()
        variant = variant_lookup.get(sku) if sku else None

        line_item: dict[str, Any] = {
            "quantity": quantity,
            "title": row.get("Lineitem name", "").strip(),
            "priceSet": money_bag(row.get("Lineitem price", "0.00"), currency),
            "requiresShipping": parse_boolean(row.get("Lineitem requires shipping", ""), default=True),
            "taxable": parse_boolean(row.get("Lineitem taxable", ""), default=True),
        }

        if sku:
            line_item["sku"] = sku

        variant_title = row.get("Lineitem variant", "").strip()
        if not variant_title and variant and variant.variant_title and variant.variant_title != "Default Title":
            variant_title = variant.variant_title
        if variant_title:
            line_item["variantTitle"] = variant_title

        if variant:
            line_item["variantId"] = variant.id
            variant_count += 1
        else:
            custom_count += 1
            if sku:
                unmatched_skus.add(sku)

        line_items.append(compact_object(line_item))

    return line_items, variant_count, custom_count, unmatched_skus


def build_shipping_lines(row: dict[str, str], currency: str) -> list[dict[str, Any]]:
    shipping_amount = as_decimal(row.get("Shipping", "0"))
    shipping_method = (row.get("Shipping Method") or "").strip()
    if shipping_amount == 0 and not shipping_method:
        return []

    shipping_line = {
        "title": shipping_method or "Shipping",
        "priceSet": money_bag(format_decimal(shipping_amount), currency),
    }
    return [compact_object(shipping_line)]


def build_discount_code(row: dict[str, str], currency: str) -> dict[str, Any] | None:
    discount_amount = as_decimal(row.get("Discount Amount", "0"))
    discount_code = (row.get("Discount Code") or "").strip()
    shipping_amount = as_decimal(row.get("Shipping", "0"))

    if discount_amount <= 0 and not discount_code:
        return None

    if not discount_code:
        discount_code = "SQUARESPACE-IMPORTED-DISCOUNT"

    if shipping_amount > 0 and discount_amount == shipping_amount:
        return {
            "freeShippingDiscountCode": {
                "code": discount_code,
            },
        }

    return {
        "itemFixedDiscountCode": {
            "code": discount_code,
            "amountSet": money_bag(format_decimal(discount_amount), currency),
        },
    }


def build_tax_lines(
    row: dict[str, str],
    currency: str,
    order_rows: list[dict[str, str]],
) -> list[dict[str, Any]]:
    tax_total = as_decimal(row.get("Taxes", "0"))
    if tax_total <= 0:
        return []

    subtotal = sum((as_decimal(item.get("Lineitem price", "0")) * int(item.get("Lineitem quantity", "0") or 0)) for item in order_rows)
    discount_amount = as_decimal(row.get("Discount Amount", "0"))
    taxable_base = subtotal - discount_amount
    if taxable_base <= 0:
        taxable_base = subtotal
    if taxable_base <= 0:
        shipping_amount = as_decimal(row.get("Shipping", "0"))
        taxable_base = shipping_amount

    rate = Decimal("0")
    if taxable_base > 0:
        rate = (tax_total / taxable_base).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)

    return [
        {
            "title": "Imported Squarespace tax",
            "priceSet": money_bag(format_decimal(tax_total), currency),
            "rate": format(rate, "f"),
        }
    ]


def build_transactions(row: dict[str, str], currency: str) -> list[dict[str, Any]]:
    financial_status = normalize_status(row.get("Financial Status", ""))
    total = as_decimal(row.get("Total", "0"))
    amount_refunded = as_decimal(row.get("Amount Refunded", "0"))
    paid_at = (row.get("Paid at") or row.get("Created at") or "").strip()
    refund_at = (row.get("Cancelled at") or row.get("Paid at") or row.get("Created at") or "").strip()
    gateway = (row.get("Payment Method") or row.get("Channel Name") or "Squarespace").strip()

    transactions: list[dict[str, Any]] = []

    if total > 0 and financial_status in {"paid", "refunded", "partially_refunded", "partially_paid"}:
        transactions.append(
            compact_object(
                {
                    "amountSet": money_bag(format_decimal(total), currency),
                    "gateway": gateway,
                    "kind": "SALE",
                    "processedAt": paid_at,
                    "status": "SUCCESS",
                }
            )
        )
    elif total > 0 and financial_status == "authorized":
        transactions.append(
            compact_object(
                {
                    "amountSet": money_bag(format_decimal(total), currency),
                    "gateway": gateway,
                    "kind": "AUTHORIZATION",
                    "processedAt": paid_at,
                    "status": "SUCCESS",
                }
            )
        )
    elif total > 0 and financial_status == "pending":
        transactions.append(
            compact_object(
                {
                    "amountSet": money_bag(format_decimal(total), currency),
                    "gateway": gateway,
                    "kind": "SALE",
                    "processedAt": paid_at,
                    "status": "PENDING",
                }
            )
        )

    if amount_refunded > 0:
        transactions.append(
            compact_object(
                {
                    "amountSet": money_bag(format_decimal(amount_refunded), currency),
                    "gateway": gateway,
                    "kind": "REFUND",
                    "processedAt": refund_at,
                    "status": "SUCCESS",
                }
            )
        )

    return transactions


def map_financial_status(row: dict[str, str]) -> str | None:
    value = normalize_status(row.get("Financial Status", ""))
    mapping = {
        "authorized": "AUTHORIZED",
        "paid": "PAID",
        "pending": "PENDING",
        "partially_paid": "PARTIALLY_PAID",
        "refunded": "REFUNDED",
        "partially_refunded": "PARTIALLY_REFUNDED",
        "voided": "VOIDED",
    }
    return mapping.get(value)


def map_fulfillment_status(row: dict[str, str]) -> str | None:
    value = normalize_status(row.get("Fulfillment Status", ""))
    if value == "fulfilled":
        return "FULFILLED"
    if value == "partial":
        return "PARTIAL"
    if value in {"cancelled", "restocked"}:
        return "RESTOCKED"
    return None


def build_customer_payload(row: dict[str, str], customer_mode: str) -> dict[str, Any] | None:
    email = (row.get("Email") or "").strip()
    if not email or customer_mode in {"email-only", "none"}:
        return None

    shipping_address = build_customer_address(row, "Shipping")
    billing_address = build_customer_address(row, "Billing")
    addresses: list[dict[str, Any]] = []
    for address in (shipping_address, billing_address):
        if address and address not in addresses:
            addresses.append(address)

    first_name, last_name = split_name(
        row.get("Shipping Name") or row.get("Billing Name") or ""
    )
    customer_input: dict[str, Any] = {
        "toUpsert": {
            "email": email,
            "firstName": first_name,
            "lastName": last_name,
            "addresses": addresses,
        }
    }
    return compact_object(customer_input)


def build_customer_address(row: dict[str, str], prefix: str) -> dict[str, Any] | None:
    name_value = (row.get(f"{prefix} Name") or "").strip()
    address1 = (row.get(f"{prefix} Address1") or "").strip()
    address2 = (row.get(f"{prefix} Address2") or "").strip()
    city = (row.get(f"{prefix} City") or "").strip()
    zip_code = (row.get(f"{prefix} Zip") or "").strip()
    province = (row.get(f"{prefix} Province") or "").strip()
    country = (row.get(f"{prefix} Country") or "").strip()
    phone = normalize_phone((row.get(f"{prefix} Phone") or "").strip(), country)

    if not any((name_value, address1, address2, city, zip_code, province, country, phone)):
        return None

    first_name, last_name = split_name(name_value)
    address = {
        "address1": address1,
        "address2": address2,
        "city": city,
        "country": country,
        "firstName": first_name,
        "lastName": last_name,
        "phone": phone,
        "province": province,
        "zip": zip_code,
    }
    return compact_object(address)


def build_order_address(row: dict[str, str], prefix: str) -> dict[str, Any] | None:
    address = build_customer_address(row, prefix)
    if address is None:
        return None

    country_name = (row.get(f"{prefix} Country") or "").strip()
    country_code = country_code_for_name(country_name)
    if country_code:
        address["countryCode"] = country_code
    return compact_object(address)


def build_order_note(row: dict[str, str]) -> str:
    note_parts: list[str] = []
    private_notes = (row.get("Private Notes") or "").strip()
    if private_notes:
        note_parts.append(private_notes)
    return "\n\n".join(note_parts)


def build_order_custom_attributes(row: dict[str, str], order_id: str) -> list[dict[str, str]]:
    raw_items = [
        ("sqsp_order_id", order_id),
        ("sqsp_channel_order_number", row.get("Channel Order Number", "")),
        ("sqsp_channel_name", row.get("Channel Name", "")),
        ("sqsp_channel_type", row.get("Channel Type", "")),
        ("sqsp_payment_method", row.get("Payment Method", "")),
        ("sqsp_payment_reference", row.get("Payment Reference", "")),
        ("sqsp_paid_at", row.get("Paid at", "")),
        ("sqsp_fulfilled_at", row.get("Fulfilled at", "")),
        ("sqsp_cancelled_at", row.get("Cancelled at", "")),
    ]
    return [{"key": key, "value": str(value).strip()} for key, value in raw_items if str(value).strip()]


def build_order_tags(prefix: str, order_id: str) -> list[str]:
    base = sanitize_tag(prefix) or "squarespace-import"
    return [base, sanitize_tag(f"{base}-order-{order_id}")]


def build_order_create_options() -> dict[str, Any]:
    return {
        "inventoryBehaviour": "BYPASS",
        "sendReceipt": False,
        "sendFulfillmentReceipt": False,
    }


def group_orders(cleaned_rows: list[dict[str, str]]) -> list[OrderGroup]:
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in cleaned_rows:
        order_id = row.get("Order ID", "")
        if not order_id:
            continue
        grouped.setdefault(order_id, []).append(row)
    order_groups = [OrderGroup(order_id=order_id, rows=rows) for order_id, rows in grouped.items()]
    order_groups.sort(
        key=lambda order_group: (
            parse_sortable_datetime(order_group.rows[0].get("Created at", "")),
            order_group.order_id,
        )
    )
    return order_groups


def resolve_shopify_credentials(args: argparse.Namespace) -> ShopifyCredentials | None:
    shop_domain = sanitize_shop_domain(
        args.shop_domain
        or os.environ.get("SHOPIFY_STORE_DOMAIN", "")
        or os.environ.get("SHOPIFY_STORE", "")
        or os.environ.get("SHOPIFY_SHOP_DOMAIN", "")
    )
    access_token = (
        args.access_token
        or os.environ.get("SHOPIFY_ADMIN_ACCESS_TOKEN", "")
        or os.environ.get("SHOPIFY_ACCESS_TOKEN", "")
    ).strip()
    client_id = (
        os.environ.get("SHOPIFY_CLIENT_ID", "")
    ).strip()
    client_secret = (
        os.environ.get("SHOPIFY_CLIENT_SECRET", "")
    ).strip()
    api_version = (
        args.api_version
        or os.environ.get("SHOPIFY_API_VERSION", "")
        or DEFAULT_SHOPIFY_API_VERSION
    ).strip()

    if shop_domain and "." not in shop_domain:
        shop_domain = f"{shop_domain}.myshopify.com"

    if not shop_domain or (not access_token and (not client_id or not client_secret)):
        return None
    return ShopifyCredentials(
        shop_domain=shop_domain,
        api_version=api_version,
        access_token=access_token,
        client_id=client_id,
        client_secret=client_secret,
    )


def load_import_state(state_path: Path, warnings: list[str]) -> dict[str, Any]:
    if not state_path.exists():
        return {"imports": {}}
    try:
        return json.loads(state_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        warnings.append(f"State file {state_path} is invalid JSON; starting fresh: {exc}")
        return {"imports": {}}


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(path.name + ".tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temp_path.replace(path)


def print_shopify_import_summary(summary: ImportSummary, apply_mode: bool) -> None:
    if summary.deleted_test_orders_before_import:
        print(
            f"- Deleted {summary.deleted_test_orders_before_import} tracked Shopify test order(s) before import."
        )
    if summary.skipped_non_test_cleanup_orders:
        print(
            f"- Left {summary.skipped_non_test_cleanup_orders} tracked order(s) in place because they were not marked as test orders."
        )

    if apply_mode:
        print(
            f"Shopify import attempted {summary.attempted_orders} order(s): "
            f"{summary.imported_orders} imported, {summary.failed_orders} failed."
        )
    else:
        print(
            f"Shopify dry run built {summary.dry_run_orders} orderCreate payload(s) "
            f"for {summary.selected_orders} selected order(s)."
        )

    if summary.skipped_state_orders:
        print(f"- Skipped {summary.skipped_state_orders} orders already present in the local state file.")
    if summary.variant_backed_line_items or summary.custom_line_items:
        print(
            f"- Prepared {summary.variant_backed_line_items} SKU-matched variant line item(s) and "
            f"{summary.custom_line_items} custom line item(s)."
        )
    if summary.customer_retry_without_upsert:
        print(
            f"- Retried {summary.customer_retry_without_upsert} order(s) without customer upsert "
            "after Shopify rejected the customer payload."
        )
    if summary.unmatched_skus:
        print(
            "- Unmatched SKUs imported as custom line items: "
            + ", ".join(sorted(summary.unmatched_skus))
        )
    if summary.failures:
        print("Import failures:", file=sys.stderr)
        for failure in summary.failures:
            print(f"- {failure}", file=sys.stderr)


def normalize_boolean(value: str) -> str:
    normalized = value.strip().lower()
    if not normalized:
        return ""
    if normalized in {"true", "1", "yes", "y"}:
        return "true"
    if normalized in {"false", "0", "no", "n"}:
        return "false"
    return normalized


def parse_boolean(value: str, *, default: bool) -> bool:
    normalized = normalize_boolean(value)
    if not normalized:
        return default
    return normalized == "true"


def normalize_status(value: str) -> str:
    stripped = value.strip()
    if not stripped:
        return ""
    lookup = stripped.lower().replace("-", "_").replace(" ", "_")
    return STATUS_VALUE_MAP.get(lookup, stripped.lower())


def normalize_money(value: str, *, line_number: int, header: str, warnings: list[str]) -> str:
    stripped = value.strip()
    if not stripped:
        return ""
    normalized = stripped.replace("$", "").replace(",", "")
    try:
        amount = Decimal(normalized)
    except InvalidOperation:
        warnings.append(f"Line {line_number}: could not parse money value {stripped!r} in {header!r}.")
        return stripped
    return format(amount.quantize(MONEY_QUANTUM, rounding=ROUND_HALF_UP), "f")


def normalize_quantity(value: str, *, line_number: int, warnings: list[str]) -> str:
    stripped = value.strip()
    if not stripped:
        return ""
    try:
        amount = Decimal(stripped)
    except InvalidOperation:
        warnings.append(f"Line {line_number}: could not parse quantity value {stripped!r}.")
        return stripped

    if amount == amount.to_integral_value():
        return str(int(amount))

    warnings.append(f"Line {line_number}: quantity {stripped!r} is not an integer; preserving decimal.")
    return format(amount.normalize(), "f")


def normalize_date(value: str, *, line_number: int, header: str, warnings: list[str]) -> str:
    stripped = value.strip()
    if not stripped:
        return ""

    iso_candidate = stripped.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(iso_candidate)
        return parsed.isoformat(timespec="seconds")
    except ValueError:
        pass

    for date_format in ("%Y-%m-%d %H:%M:%S %z", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(stripped, date_format)
            return parsed.isoformat(timespec="seconds")
        except ValueError:
            continue

    warnings.append(f"Line {line_number}: could not parse date value {stripped!r} in {header!r}.")
    return stripped


def sanitize_shop_domain(value: str) -> str:
    cleaned = value.strip()
    if not cleaned:
        return ""
    for prefix in ("https://", "http://"):
        if cleaned.startswith(prefix):
            cleaned = cleaned[len(prefix) :]
    cleaned = cleaned.split("/", 1)[0].strip()
    return cleaned.rstrip("/")


def sanitize_tag(value: str) -> str:
    return "-".join(part for part in value.strip().replace("_", "-").split() if part)


def split_name(full_name: str) -> tuple[str, str]:
    parts = [part for part in full_name.strip().split() if part]
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return " ".join(parts[:-1]), parts[-1]


def as_decimal(value: str) -> Decimal:
    stripped = (value or "").strip()
    if not stripped:
        return Decimal("0")
    normalized = stripped.replace("$", "").replace(",", "")
    try:
        return Decimal(normalized)
    except InvalidOperation:
        return Decimal("0")


def format_decimal(value: Decimal) -> str:
    return format(value.quantize(MONEY_QUANTUM, rounding=ROUND_HALF_UP), "f")


def money_bag(amount: str, currency: str) -> dict[str, Any]:
    return {
        "shopMoney": {
            "amount": amount,
            "currencyCode": currency,
        }
    }


def country_code_for_name(country_name: str) -> str:
    normalized = country_name.strip().lower()
    if not normalized:
        return ""
    return COUNTRY_ALIASES.get(normalized, normalized.upper() if len(normalized) == 2 else "")


def normalize_phone(phone: str, country_name: str) -> str:
    raw = phone.strip()
    if not raw:
        return ""

    if raw.startswith("+"):
        digits = "+" + "".join(char for char in raw if char.isdigit())
        if 8 <= len(digits.replace("+", "")) <= 15:
            return digits
        return ""

    digits = "".join(char for char in raw if char.isdigit())
    if not digits:
        return ""

    country_code = country_code_for_name(country_name)
    if country_code in {"US", "CA"}:
        if len(digits) == 10:
            return f"+1{digits}"
        if len(digits) == 11 and digits.startswith("1"):
            return f"+{digits}"
    if 8 <= len(digits) <= 15 and not digits.startswith("0"):
        return f"+{digits}"
    return ""


def choose_primary_phone(row: dict[str, str]) -> str:
    shipping_phone = normalize_phone(
        (row.get("Shipping Phone") or "").strip(),
        row.get("Shipping Country", ""),
    )
    if shipping_phone:
        return shipping_phone
    return normalize_phone((row.get("Billing Phone") or "").strip(), row.get("Billing Country", ""))


def compact_object(value: Any) -> Any:
    if isinstance(value, dict):
        compacted = {}
        for key, nested in value.items():
            compacted_value = compact_object(nested)
            if compacted_value in ("", None, [], {}):
                continue
            compacted[key] = compacted_value
        return compacted
    if isinstance(value, list):
        compacted_list = [compact_object(item) for item in value]
        return [item for item in compacted_list if item not in ("", None, [], {})]
    return value


def parse_sortable_datetime(value: str) -> datetime:
    stripped = value.strip()
    if not stripped:
        return datetime.min.replace(tzinfo=timezone.utc)

    iso_candidate = stripped.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(iso_candidate)
    except ValueError:
        for date_format in ("%Y-%m-%d %H:%M:%S %z", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(stripped, date_format)
                break
            except ValueError:
                continue
        else:
            return datetime.min.replace(tzinfo=timezone.utc)

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def format_user_errors(user_errors: list[dict[str, Any]]) -> str:
    if not user_errors:
        return "Unknown Shopify user error."
    formatted: list[str] = []
    for item in user_errors:
        field = item.get("field") or []
        prefix = ".".join(str(part) for part in field) if field else "order"
        message = item.get("message", "Unknown Shopify user error")
        formatted.append(f"{prefix}: {message}")
    return "; ".join(formatted)


def is_customer_related_error(user_errors: list[dict[str, Any]]) -> bool:
    for item in user_errors:
        message = (item.get("message") or "").lower()
        field = [str(part).lower() for part in (item.get("field") or [])]
        if "customer" in message:
            return True
        if any(part == "customer" for part in field):
            return True
        if "protected customer data" in message:
            return True
    return False


if __name__ == "__main__":
    raise SystemExit(main())
