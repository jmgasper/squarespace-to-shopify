#!/usr/bin/env python3
"""Convert a Squarespace product export into a Shopify product CSV.

The converter targets the cleanup steps documented in
`squarespace-to-shopify-migration-flow.md`:

- map core Squarespace product fields into Shopify CSV columns
- convert weights from pounds to grams
- normalize inventory values for Shopify
- sanitize handles
- preserve variant structure and SKUs
- expand Squarespace's space-delimited image URLs into Shopify image rows

Example:
    python3 scripts/squarespace_to_shopify.py \
      /path/to/SquarespaceProductDownload.csv \
      -o /path/to/shopify-products.csv
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Iterable


SHOPIFY_HEADERS = [
    "Handle",
    "Title",
    "Body (HTML)",
    "Vendor",
    "Type",
    "Tags",
    "Published",
    "Option1 Name",
    "Option1 Value",
    "Option2 Name",
    "Option2 Value",
    "Option3 Name",
    "Option3 Value",
    "Variant SKU",
    "Variant Grams",
    "Variant Inventory Tracker",
    "Variant Inventory Qty",
    "Variant Inventory Policy",
    "Variant Fulfillment Service",
    "Variant Price",
    "Variant Compare-at Price",
    "Variant Requires Shipping",
    "Variant Taxable",
    "Image Src",
    "Image Position",
]

SQUARESPACE_OPTION_LIMIT = 6
SHOPIFY_OPTION_LIMIT = 3
POUNDS_TO_GRAMS = Decimal("453.59237")


@dataclass
class ProductBlock:
    source_line: int
    primary_row: dict[str, str]
    variant_rows: list[dict[str, str]]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Convert a Squarespace product CSV into a Shopify product CSV."
    )
    parser.add_argument("input_csv", type=Path, help="Path to the Squarespace CSV export.")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="Output Shopify CSV path. Defaults to '<input>.shopify.csv'.",
    )
    parser.add_argument(
        "--default-vendor",
        default="",
        help="Fallback Vendor value when the Squarespace export has no vendor column.",
    )
    parser.add_argument(
        "--infer-vendor-from-title",
        action="store_true",
        help="Use the title prefix before ' - ' as Vendor when no explicit vendor exists.",
    )
    parser.add_argument(
        "--use-sale-price",
        action="store_true",
        help="If Sale Price is lower than Price, import it as Variant Price and keep Price as Compare-at Price.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    input_csv = args.input_csv.expanduser().resolve()
    output_csv = (
        args.output.expanduser().resolve()
        if args.output
        else input_csv.with_name(f"{input_csv.stem}.shopify.csv")
    )

    if not input_csv.exists():
        print(f"Input file not found: {input_csv}", file=sys.stderr)
        return 1

    with input_csv.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            print(f"Input CSV has no header row: {input_csv}", file=sys.stderr)
            return 1
        squarespace_headers = set(reader.fieldnames)
        source_rows = list(reader)

    warnings: list[str] = []
    try:
        products = group_products(source_rows)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    validate_options(products, warnings)
    validate_handles(products, warnings)

    shopify_rows: list[dict[str, str]] = []
    duplicate_skus = find_duplicate_skus(products)
    if duplicate_skus:
        warnings.append(
            "Duplicate SKUs detected: "
            + ", ".join(f"{sku} ({count} rows)" for sku, count in sorted(duplicate_skus.items()))
        )

    unlimited_inventory_count = 0
    ignored_sale_price_count = 0
    blank_vendor_count = 0

    for product in products:
        vendor = determine_vendor(
            product.primary_row,
            squarespace_headers=squarespace_headers,
            default_vendor=args.default_vendor.strip(),
            infer_from_title=args.infer_vendor_from_title,
        )
        if not vendor:
            blank_vendor_count += 1

        product_type = determine_product_type(
            categories=product.primary_row.get("Categories", ""),
            title=product.primary_row.get("Title", ""),
        )
        tags = determine_tags(
            source_tags=product.primary_row.get("Tags", ""),
            categories=product.primary_row.get("Categories", ""),
            product_type=product_type,
        )
        handle = sanitize_handle(
            product.primary_row.get("Product URL", "") or product.primary_row.get("Title", "")
        )
        if not handle:
            warnings.append(
                f"Line {product.source_line}: unable to derive a Shopify handle; product skipped."
            )
            continue

        images = parse_image_urls(product.primary_row.get("Hosted Image URLs", ""))
        published = to_shopify_boolean(product.primary_row.get("Visible", ""))

        for variant_index, variant_row in enumerate(product.variant_rows):
            inventory = normalize_inventory(variant_row.get("Stock", ""))
            if inventory.unlimited:
                unlimited_inventory_count += 1

            prices = normalize_prices(
                variant_row.get("Price", ""),
                variant_row.get("Sale Price", ""),
                use_sale_price=args.use_sale_price,
            )
            if prices.sale_price_present_but_unused:
                ignored_sale_price_count += 1

            option_fields = build_option_fields(product.variant_rows, variant_row)

            shopify_row = blank_shopify_row()
            shopify_row["Handle"] = handle
            shopify_row["Variant SKU"] = variant_row.get("SKU", "").strip()
            shopify_row["Variant Grams"] = pounds_to_grams(variant_row.get("Weight", ""))
            shopify_row["Variant Inventory Tracker"] = inventory.tracker
            shopify_row["Variant Inventory Qty"] = inventory.quantity
            shopify_row["Variant Inventory Policy"] = inventory.policy
            shopify_row["Variant Fulfillment Service"] = "manual"
            shopify_row["Variant Price"] = prices.price
            shopify_row["Variant Compare-at Price"] = prices.compare_at_price
            shopify_row["Variant Requires Shipping"] = "TRUE"
            shopify_row["Variant Taxable"] = "TRUE"
            for key, value in option_fields.items():
                shopify_row[key] = value

            if variant_index == 0:
                shopify_row["Title"] = product.primary_row.get("Title", "").strip()
                shopify_row["Body (HTML)"] = product.primary_row.get("Description", "").strip()
                shopify_row["Vendor"] = vendor
                shopify_row["Type"] = product_type
                shopify_row["Tags"] = tags
                shopify_row["Published"] = published
                if images:
                    shopify_row["Image Src"] = images[0]
                    shopify_row["Image Position"] = "1"

            shopify_rows.append(shopify_row)

        for index, image_url in enumerate(images[1:], start=2):
            image_row = blank_shopify_row()
            image_row["Handle"] = handle
            image_row["Image Src"] = image_url
            image_row["Image Position"] = str(index)
            shopify_rows.append(image_row)

    with output_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=SHOPIFY_HEADERS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(shopify_rows)

    print(
        f"Wrote {len(shopify_rows)} Shopify rows for {len(products)} products "
        f"({sum(len(product.variant_rows) for product in products)} variants) to {output_csv}"
    )
    if unlimited_inventory_count:
        print(
            f"- {unlimited_inventory_count} variants used unlimited stock and were exported "
            "with inventory tracking disabled."
        )
    if ignored_sale_price_count:
        print(
            f"- {ignored_sale_price_count} variants had a lower Sale Price than Price, "
            "but Price was kept. Re-run with --use-sale-price to import sale pricing."
        )
    if blank_vendor_count:
        print(
            f"- {blank_vendor_count} products have an empty Vendor. "
            "Use --default-vendor or --infer-vendor-from-title if needed."
        )
    if warnings:
        print("Warnings:", file=sys.stderr)
        for warning in warnings:
            print(f"- {warning}", file=sys.stderr)

    return 0


def group_products(rows: Iterable[dict[str, str]]) -> list[ProductBlock]:
    products: list[ProductBlock] = []
    current_product: ProductBlock | None = None

    for line_number, row in enumerate(rows, start=2):
        product_url = row.get("Product URL", "").strip()
        if product_url:
            current_product = ProductBlock(
                source_line=line_number,
                primary_row=row,
                variant_rows=[row],
            )
            products.append(current_product)
            continue

        if current_product is None:
            raise ValueError(
                f"Encountered a variant row before any product row at line {line_number}."
            )
        current_product.variant_rows.append(row)

    return products


def validate_options(products: list[ProductBlock], warnings: list[str]) -> None:
    for product in products:
        extra_option_names = []
        for option_index in range(SHOPIFY_OPTION_LIMIT + 1, SQUARESPACE_OPTION_LIMIT + 1):
            key = f"Option Name {option_index}"
            if any(row.get(key, "").strip() for row in product.variant_rows):
                extra_option_names.append(str(option_index))
        if extra_option_names:
            warnings.append(
                f"Line {product.source_line}: option columns {', '.join(extra_option_names)} "
                "contain data, but the Shopify CSV only includes the first three option columns."
            )


def validate_handles(products: list[ProductBlock], warnings: list[str]) -> None:
    grouped_lines: defaultdict[str, list[int]] = defaultdict(list)
    for product in products:
        raw_value = product.primary_row.get("Product URL", "") or product.primary_row.get("Title", "")
        grouped_lines[sanitize_handle(raw_value)].append(product.source_line)

    for handle, lines in sorted(grouped_lines.items()):
        if handle and len(lines) > 1:
            warnings.append(
                f"Sanitized handle '{handle}' is duplicated across lines "
                + ", ".join(str(line) for line in lines)
                + "."
            )


def find_duplicate_skus(products: list[ProductBlock]) -> dict[str, int]:
    counts: defaultdict[str, int] = defaultdict(int)
    for product in products:
        for row in product.variant_rows:
            sku = row.get("SKU", "").strip()
            if sku:
                counts[sku] += 1
    return {sku: count for sku, count in counts.items() if count > 1}


def blank_shopify_row() -> dict[str, str]:
    return {header: "" for header in SHOPIFY_HEADERS}


def determine_vendor(
    row: dict[str, str],
    *,
    squarespace_headers: set[str],
    default_vendor: str,
    infer_from_title: bool,
) -> str:
    for candidate in ("Default Vendor Name", "Vendor"):
        if candidate in squarespace_headers:
            value = row.get(candidate, "").strip()
            if value:
                return value

    if default_vendor:
        return default_vendor

    if infer_from_title:
        title = row.get("Title", "").strip()
        if " - " in title:
            return title.split(" - ", 1)[0].strip()

    return ""


def determine_product_type(categories: str, title: str) -> str:
    category_parts = parse_categories(categories)
    title_lower = title.lower()

    if "records" in category_parts and "tops" in category_parts:
        return "bundle"
    if "records" in category_parts:
        return "vinyl"
    if "tapes" in category_parts:
        return "cassette"
    if "tops" in category_parts:
        return "merch"

    if any(term in title_lower for term in ("bundle", "combo", "2-pack", "two-pack")):
        return "bundle"
    if any(term in title_lower for term in ("cassette", "tape")):
        return "cassette"
    if any(term in title_lower for term in ('vinyl', ' lp', '7"', "7-inch", "7 inch")):
        return "vinyl"
    if any(
        term in title_lower
        for term in ("tee", "shirt", "crop top", "jacket", "tote", "tank", "hoodie", "cap")
    ):
        return "merch"

    return ""


def determine_tags(source_tags: str, categories: str, product_type: str) -> str:
    tags: list[str] = []
    seen: set[str] = set()

    def add(tag: str) -> None:
        cleaned = tag.strip()
        if not cleaned:
            return
        fingerprint = cleaned.casefold()
        if fingerprint in seen:
            return
        seen.add(fingerprint)
        tags.append(cleaned)

    for tag in source_tags.split(","):
        add(tag)

    for category in parse_categories(categories):
        if category == "records":
            add("vinyl")
        elif category == "tapes":
            add("cassette")
        elif category == "tops":
            add("merch")
        else:
            add(category)

    if product_type:
        add(product_type)

    return ", ".join(tags)


def build_option_fields(
    all_variant_rows: list[dict[str, str]],
    current_variant_row: dict[str, str],
) -> dict[str, str]:
    option_fields = {f"Option{index} Name": "" for index in range(1, SHOPIFY_OPTION_LIMIT + 1)}
    option_fields.update(
        {f"Option{index} Value": "" for index in range(1, SHOPIFY_OPTION_LIMIT + 1)}
    )

    has_any_option = any(
        row.get(f"Option Name {index}", "").strip()
        for row in all_variant_rows
        for index in range(1, SHOPIFY_OPTION_LIMIT + 1)
    )

    if not has_any_option:
        option_fields["Option1 Name"] = "Title"
        option_fields["Option1 Value"] = "Default Title"
        return option_fields

    for index in range(1, SHOPIFY_OPTION_LIMIT + 1):
        option_fields[f"Option{index} Name"] = current_variant_row.get(
            f"Option Name {index}", ""
        ).strip()
        option_fields[f"Option{index} Value"] = current_variant_row.get(
            f"Option Value {index}", ""
        ).strip()

    return option_fields


@dataclass
class InventoryValue:
    quantity: str
    tracker: str
    policy: str
    unlimited: bool = False


def normalize_inventory(value: str) -> InventoryValue:
    cleaned = value.strip()
    if not cleaned:
        return InventoryValue(quantity="", tracker="", policy="", unlimited=False)

    if cleaned.casefold() == "unlimited":
        return InventoryValue(quantity="", tracker="", policy="", unlimited=True)

    parsed = parse_number(cleaned)
    if parsed is None:
        return InventoryValue(quantity="", tracker="", policy="", unlimited=False)

    quantity = str(int(parsed))
    return InventoryValue(
        quantity=quantity,
        tracker="shopify",
        policy="deny",
        unlimited=False,
    )


@dataclass
class PriceValue:
    price: str
    compare_at_price: str
    sale_price_present_but_unused: bool = False


def normalize_prices(price_raw: str, sale_price_raw: str, *, use_sale_price: bool) -> PriceValue:
    price = parse_number(price_raw)
    sale_price = parse_number(sale_price_raw)

    price_text = format_decimal(price)
    if price is None:
        return PriceValue(price="", compare_at_price="", sale_price_present_but_unused=False)

    if sale_price is not None and sale_price > 0 and sale_price < price:
        if use_sale_price:
            return PriceValue(
                price=format_decimal(sale_price),
                compare_at_price=price_text,
                sale_price_present_but_unused=False,
            )
        return PriceValue(
            price=price_text,
            compare_at_price="",
            sale_price_present_but_unused=True,
        )

    return PriceValue(price=price_text, compare_at_price="", sale_price_present_but_unused=False)


def pounds_to_grams(value: str) -> str:
    pounds = parse_number(value)
    if pounds is None:
        return ""
    grams = (pounds * POUNDS_TO_GRAMS).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return str(int(grams))


def parse_number(value: str) -> Decimal | None:
    cleaned = value.strip().replace(",", "")
    if not cleaned:
        return None

    match = re.search(r"-?\d+(?:\.\d+)?", cleaned)
    if match is None:
        return None

    try:
        return Decimal(match.group(0))
    except InvalidOperation:
        return None


def format_decimal(value: Decimal | None) -> str:
    if value is None:
        return ""
    return f"{value.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP):f}"


def parse_categories(raw_categories: str) -> list[str]:
    categories = []
    for chunk in raw_categories.split(","):
        cleaned = chunk.strip().strip("/")
        if not cleaned:
            continue
        cleaned = cleaned.replace("&", "and").replace(" ", "-").lower()
        categories.append(cleaned)
    return categories


def sanitize_handle(value: str) -> str:
    normalized = (
        unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii").lower()
    )
    normalized = re.sub(r"[^a-z0-9]+", "-", normalized)
    normalized = re.sub(r"-{2,}", "-", normalized).strip("-")
    return normalized


def parse_image_urls(raw_urls: str) -> list[str]:
    return re.findall(r"https?://\S+", raw_urls)


def to_shopify_boolean(value: str) -> str:
    return "TRUE" if value.strip().casefold() in {"true", "yes", "1"} else "FALSE"


if __name__ == "__main__":
    raise SystemExit(main())
