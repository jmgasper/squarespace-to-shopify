#!/usr/bin/env python3
"""Convert a Squarespace profiles export into a Shopify customer CSV.

The converter maps Squarespace contact and customer exports into Shopify's
customer import template while keeping non-importable Squarespace metadata in
the Shopify `Note` field.

Example:
    python3 scripts/squarespace_customers_to_shopify.py \
      /path/to/profiles.csv \
      -o /path/to/shopify-customers.csv
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
import unicodedata
from dataclasses import dataclass
from pathlib import Path


SHOPIFY_HEADERS = [
    "First Name",
    "Last Name",
    "Email",
    "Accepts Email Marketing",
    "Default Address Company",
    "Default Address Address1",
    "Default Address Address2",
    "Default Address City",
    "Default Address Province Code",
    "Default Address Country Code",
    "Default Address Zip",
    "Default Address Phone",
    "Phone",
    "Accepts SMS Marketing",
    "Tags",
    "Note",
    "Tax Exempt",
]

TRUE_VALUES = {"1", "true", "yes", "y"}
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
    "sweden": "SE",
    "se": "SE",
}


@dataclass
class Summary:
    rows_read: int = 0
    rows_written: int = 0
    duplicate_rows_merged: int = 0
    blank_email_rows_skipped: int = 0
    billing_address_used: int = 0
    no_address_rows: int = 0
    blank_name_rows: int = 0


@dataclass
class Address:
    source: str
    name: str
    address1: str
    address2: str
    city: str
    province_code: str
    country_code: str
    zip_code: str
    phone: str

    def has_address_data(self) -> bool:
        return any(
            [
                self.address1,
                self.address2,
                self.city,
                self.province_code,
                self.country_code,
                self.zip_code,
            ]
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Convert a Squarespace profiles CSV into a Shopify customer CSV."
    )
    parser.add_argument("input_csv", type=Path, help="Path to the Squarespace profiles CSV.")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="Output Shopify CSV path. Defaults to '<input>.shopify-customers.csv'.",
    )
    parser.add_argument(
        "--billing-first",
        action="store_true",
        help="Prefer billing fields over shipping fields when building the default address.",
    )
    parser.add_argument(
        "--no-copy-phone",
        action="store_true",
        help="Leave Shopify's customer-level Phone field blank instead of copying the best source phone.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    input_csv = args.input_csv.expanduser().resolve()
    output_csv = (
        args.output.expanduser().resolve()
        if args.output
        else input_csv.with_name(f"{input_csv.stem}.shopify-customers.csv")
    )

    if not input_csv.exists():
        print(f"Input file not found: {input_csv}", file=sys.stderr)
        return 1

    with input_csv.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            print(f"Input CSV has no header row: {input_csv}", file=sys.stderr)
            return 1
        source_rows = list(reader)

    summary = Summary(rows_read=len(source_rows))
    warnings: list[str] = []
    merged_rows = merge_duplicate_emails(source_rows, summary, warnings)

    shopify_rows: list[dict[str, str]] = []
    for row in merged_rows:
        address = choose_address(row, billing_first=args.billing_first)
        if address.source == "billing":
            summary.billing_address_used += 1
        if not address.has_address_data():
            summary.no_address_rows += 1

        first_name, last_name = resolve_name(row, address.name)
        if not first_name or not last_name:
            summary.blank_name_rows += 1

        best_phone = address.phone or normalize_phone(
            row.get("Shipping Phone Number", ""), normalize_country_code(row.get("Shipping Country", ""))
        ) or normalize_phone(
            row.get("Billing Phone Number", ""), normalize_country_code(row.get("Billing Country", ""))
        )

        shopify_row = blank_shopify_row()
        shopify_row["First Name"] = first_name
        shopify_row["Last Name"] = last_name
        shopify_row["Email"] = clean_text(row.get("Email", ""))
        shopify_row["Accepts Email Marketing"] = to_yes_no(row.get("Accepts Marketing", ""))
        shopify_row["Default Address Address1"] = address.address1
        shopify_row["Default Address Address2"] = address.address2
        shopify_row["Default Address City"] = address.city
        shopify_row["Default Address Province Code"] = address.province_code
        shopify_row["Default Address Country Code"] = address.country_code
        shopify_row["Default Address Zip"] = address.zip_code
        shopify_row["Default Address Phone"] = address.phone
        shopify_row["Phone"] = "" if args.no_copy_phone else best_phone
        shopify_row["Accepts SMS Marketing"] = "no"
        shopify_row["Tags"] = ", ".join(build_tags(row))
        shopify_row["Note"] = build_note(row)
        shopify_row["Tax Exempt"] = "no"
        shopify_rows.append(shopify_row)

    with output_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=SHOPIFY_HEADERS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(shopify_rows)

    summary.rows_written = len(shopify_rows)

    print(f"Wrote {summary.rows_written} Shopify customers to {output_csv}")
    if summary.duplicate_rows_merged:
        print(f"- Merged {summary.duplicate_rows_merged} duplicate Squarespace rows by email.")
    if summary.blank_email_rows_skipped:
        print(f"- Skipped {summary.blank_email_rows_skipped} rows with no email address.")
    if summary.billing_address_used:
        print(f"- Used billing details for {summary.billing_address_used} default addresses.")
    if summary.no_address_rows:
        print(f"- {summary.no_address_rows} customers have no default address in the export.")
    if summary.blank_name_rows:
        print(f"- {summary.blank_name_rows} customers still have blank first/last name.")
    if warnings:
        print("Warnings:", file=sys.stderr)
        for warning in warnings:
            print(f"- {warning}", file=sys.stderr)

    return 0


def merge_duplicate_emails(
    rows: list[dict[str, str]], summary: Summary, warnings: list[str]
) -> list[dict[str, str]]:
    merged: dict[str, dict[str, str]] = {}
    order: list[str] = []

    for index, row in enumerate(rows, start=2):
        email = clean_text(row.get("Email", ""))
        if not email:
            summary.blank_email_rows_skipped += 1
            warnings.append(f"Line {index}: skipped row with no email address.")
            continue

        key = email.casefold()
        normalized_row = {key_name: clean_text(value) for key_name, value in row.items()}
        normalized_row["Email"] = email

        if key not in merged:
            merged[key] = normalized_row
            order.append(key)
            continue

        summary.duplicate_rows_merged += 1
        merged[key] = merge_rows(merged[key], normalized_row)

    return [merged[key] for key in order]


def merge_rows(left: dict[str, str], right: dict[str, str]) -> dict[str, str]:
    merged = dict(left)
    for key in set(left) | set(right):
        left_value = clean_text(left.get(key, ""))
        right_value = clean_text(right.get(key, ""))
        if key in {"Accepts Marketing", "Has Account"}:
            merged[key] = "true" if is_truthy(left_value) or is_truthy(right_value) else "false"
        elif left_value:
            merged[key] = left_value
        else:
            merged[key] = right_value
    return merged


def choose_address(row: dict[str, str], billing_first: bool) -> Address:
    if billing_first:
        primary_prefix = "Billing"
        secondary_prefix = "Shipping"
    else:
        primary_prefix = "Shipping"
        secondary_prefix = "Billing"

    primary = build_address(row, primary_prefix, secondary_prefix)
    secondary = build_address(row, secondary_prefix, primary_prefix)

    if primary.has_address_data():
        return primary
    if secondary.has_address_data():
        return secondary
    return primary


def build_address(row: dict[str, str], primary_prefix: str, fallback_prefix: str) -> Address:
    def value(primary_field: str, fallback_field: str) -> str:
        return clean_text(row.get(primary_field, "")) or clean_text(row.get(fallback_field, ""))

    country = normalize_country_code(value(f"{primary_prefix} Country", f"{fallback_prefix} Country"))
    phone = normalize_phone(
        value(f"{primary_prefix} Phone Number", f"{fallback_prefix} Phone Number"),
        country,
    )

    return Address(
        source=primary_prefix.casefold(),
        name=value(f"{primary_prefix} Name", f"{fallback_prefix} Name"),
        address1=value(f"{primary_prefix} Address 1", f"{fallback_prefix} Address 1"),
        address2=value(f"{primary_prefix} Address 2", f"{fallback_prefix} Address 2"),
        city=value(f"{primary_prefix} City", f"{fallback_prefix} City"),
        province_code=normalize_code(
            value(f"{primary_prefix} Province/State", f"{fallback_prefix} Province/State")
        ),
        country_code=country,
        zip_code=value(f"{primary_prefix} Zip", f"{fallback_prefix} Zip"),
        phone=phone,
    )


def resolve_name(row: dict[str, str], fallback_full_name: str) -> tuple[str, str]:
    first_name = clean_text(row.get("First Name", ""))
    last_name = clean_text(row.get("Last Name", ""))
    if first_name and last_name:
        return first_name, last_name

    inferred_first, inferred_last = split_name(fallback_full_name)
    return first_name or inferred_first, last_name or inferred_last


def split_name(full_name: str) -> tuple[str, str]:
    value = re.sub(r"\s+", " ", clean_text(full_name))
    if not value:
        return "", ""

    if "," in value:
        last_name, first_name = [part.strip() for part in value.split(",", 1)]
        return first_name, last_name

    parts = value.split(" ")
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


def build_tags(row: dict[str, str]) -> list[str]:
    tags: list[str] = []
    tags.append("imported-from-squarespace")

    if has_customer_history(row):
        tags.append("squarespace-customer")
    if has_subscriber_history(row):
        tags.append("squarespace-subscriber")
    if is_truthy(row.get("Has Account", "")):
        tags.append("squarespace-account")

    tags.extend(split_source_tags(row.get("Tags", "")))

    for mailing_list in split_source_values(row.get("Mailing Lists", "")):
        tags.append(slugify_tag(f"mailing-list-{mailing_list}"))
    for member_area in split_source_values(row.get("Member Areas", "")):
        tags.append(slugify_tag(f"member-area-{member_area}"))
    if clean_text(row.get("Subscriber Source", "")):
        tags.append(slugify_tag(f"subscriber-source-{row['Subscriber Source']}"))

    deduped: list[str] = []
    seen: set[str] = set()
    for tag in tags:
        cleaned = clean_text(tag)
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        deduped.append(cleaned)
    return deduped


def split_source_tags(value: str) -> list[str]:
    tags: list[str] = []
    for item in split_source_values(value):
        normalized = clean_text(item)
        if normalized:
            tags.append(normalized)
    return tags


def split_source_values(value: str) -> list[str]:
    raw = clean_text(value)
    if not raw:
        return []
    return [part.strip() for part in re.split(r"[;,|]", raw) if part.strip()]


def slugify_tag(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", clean_text(value))
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", ascii_value).strip("-").lower()
    return slug


def build_note(row: dict[str, str]) -> str:
    note_lines: list[str] = []

    def add_line(label: str, field_name: str, *, skip_if_zero: bool = False) -> None:
        value = clean_text(row.get(field_name, ""))
        if not value:
            return
        if skip_if_zero and is_zeroish(value):
            return
        note_lines.append(f"{label}: {value}")

    add_line("Squarespace Created On", "Created On")
    add_line("Squarespace Customer Since", "Customer Since")
    add_line("Squarespace Subscriber Since", "Subscriber Since")
    add_line("Squarespace Last Order Date", "Last Order Date")
    add_line("Squarespace Order Count", "Order Count", skip_if_zero=True)
    add_line("Squarespace Total Spent", "Total Spent", skip_if_zero=True)
    add_line("Squarespace Last Donation Date", "Last Donation Date")
    add_line("Squarespace Donation Count", "Donation Count", skip_if_zero=True)
    add_line("Squarespace Total Donation Amount", "Total Donation Amount", skip_if_zero=True)
    add_line("Squarespace Subscriber Source", "Subscriber Source")
    add_line("Squarespace Mailing Lists", "Mailing Lists")
    add_line("Squarespace Member Areas", "Member Areas")

    has_account = clean_text(row.get("Has Account", ""))
    if is_truthy(has_account):
        note_lines.append(f"Squarespace Has Account: {has_account}")

    return "\n".join(note_lines)


def has_customer_history(row: dict[str, str]) -> bool:
    return any(
        [
            clean_text(row.get("Customer Since", "")),
            not is_zeroish(row.get("Order Count", "")),
            not is_zeroish(row.get("Total Spent", "")),
        ]
    )


def has_subscriber_history(row: dict[str, str]) -> bool:
    return any(
        [
            is_truthy(row.get("Accepts Marketing", "")),
            clean_text(row.get("Subscriber Since", "")),
            clean_text(row.get("Mailing Lists", "")),
        ]
    )


def normalize_country_code(value: str) -> str:
    text = clean_text(value)
    if not text:
        return ""
    if len(text) == 2 and text.isalpha():
        return text.upper()
    return COUNTRY_ALIASES.get(text.casefold(), text.upper())


def normalize_code(value: str) -> str:
    return clean_text(value).upper()


def normalize_phone(value: str, country_code: str) -> str:
    raw = clean_text(value)
    if not raw:
        return ""
    if raw.startswith("+"):
        digits = re.sub(r"\D", "", raw)
        return f"+{digits}" if digits else raw

    digits = re.sub(r"\D", "", raw)
    if country_code in {"US", "CA"}:
        if len(digits) == 10:
            return f"+1{digits}"
        if len(digits) == 11 and digits.startswith("1"):
            return f"+{digits}"
    return raw


def to_yes_no(value: str) -> str:
    return "yes" if is_truthy(value) else "no"


def is_truthy(value: str) -> bool:
    return clean_text(value).casefold() in TRUE_VALUES


def is_zeroish(value: str) -> bool:
    stripped = clean_text(value).replace(",", "")
    if not stripped:
        return True
    return stripped in {"0", "0.0", "0.00", "false", "False"}


def clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def blank_shopify_row() -> dict[str, str]:
    return {header: "" for header in SHOPIFY_HEADERS}


if __name__ == "__main__":
    raise SystemExit(main())
