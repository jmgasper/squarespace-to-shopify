# Squarespace Orders to Shopify

Import historical Squarespace orders into Shopify using Shopify's Admin GraphQL API.

This script is designed for one job:

1. clean up a Squarespace order export
2. preview the Shopify order payloads it would create
3. optionally create those orders in Shopify

It is intentionally conservative:

- it does not adjust inventory
- it does not send order receipts
- it does not send fulfillment receipts
- it keeps a local state file so you can safely rerun it without re-importing the same orders
- it can import unmatched SKUs as custom line items if a Shopify variant cannot be found

## Before You Start

- Use a Shopify dev store first if you can. It is much safer than testing against a live store.
- Your products should already exist in Shopify before you import orders.
- If you want imported line items linked to real Shopify variants, the Shopify variant SKUs should match the Squarespace `Lineitem sku` values.
- This script uses Python's standard library only. You do not need to install extra Python packages.
- You need Python 3.10 or newer.

## What This Script Creates

When you run it, the script can create up to three files:

- a cleaned CSV file
- a payload preview JSON file
- a local state file that records which Squarespace order IDs were already imported

By default:

- cleaned CSV: `<your-file>.shopify-history.csv`
- state file: `<your-file>.shopify-import-state.json`

## Recommended Folder Layout

For the easiest setup, put the script and your CSV in the same folder:

```text
squarespace-orders-to-shopify/
├── README.md
├── squarespace_orders_to_shopify.py
└── SquarespaceOrderDownload.csv
```

The commands below assume that layout.

## Step 1: Export Your Orders From Squarespace

In Squarespace:

1. Open `Products & Services`.
2. Click `Orders`.
3. Click `Export data`.
4. Click `Download CSV`.
5. Choose the order statuses you want to export.
6. Choose a date range.
7. Choose `All products`.
8. Click `Download`.

Notes:

- Squarespace says the date range is based on when the order was created, not when it was fulfilled.
- If you want a full order-history migration, choose all relevant statuses and an all-time date range.
- Save the file with a simple name such as `SquarespaceOrderDownload.csv`.

> Screenshot placeholder: Squarespace `Orders` page with the `Export data` button highlighted.
>
> Screenshot placeholder: Squarespace export modal showing order status, date range, and `All products`.

## Step 2: Prepare Shopify

### 2A. Recommended: Use a Dev Store First

If you do not already have a safe test store, create one first.

In Shopify Dev Dashboard:

1. Go to `https://dev.shopify.com/`.
2. Open `Stores`.
3. Click `Create store`.
4. Choose the plan you want for testing.
5. Create the store and log in to it.

> Screenshot placeholder: Shopify Dev Dashboard `Stores` screen with `Create store` highlighted.

### 2B. Create a Shopify App

As of March 28, 2026, Shopify's official path for new custom apps is the Dev Dashboard. Shopify states that new legacy custom apps created directly in the Shopify admin are no longer available starting January 1, 2026.

In Shopify Dev Dashboard:

1. Open `Apps`.
2. Click `Create app`.
3. Choose `Start from Dev Dashboard`.
4. Enter a name such as `Squarespace Order Import`.
5. Click `Create`.

> Screenshot placeholder: Shopify Dev Dashboard `Apps` screen with `Create app`.

### 2C. Create a Version and Choose Access Scopes

After the app is created:

1. Open the app.
2. Go to the `Versions` tab.
3. Set an app URL.
4. Choose a Webhooks API version.
5. Add the access scopes.
6. Click `Release`.

For this script:

- required: `write_orders`
- recommended: `read_products`
- optional: `write_customers`

What those scopes are for:

- `write_orders`: required for creating Shopify orders
- `read_products`: lets the script look up Shopify variants by SKU so imported line items can be attached to real Shopify products
- `write_customers`: gives the script a better chance of attaching orders to Shopify customers instead of creating order-only contact data

For the app URL:

- if you are only using this script and not building a web app, Shopify's docs say you can use `https://shopify.dev/apps/default-app-home`

Notes:

- If a SKU cannot be matched in Shopify, the order can still import, but that line item will be created as a custom line item.
- Access to some customer data may require additional Shopify approval. If Shopify rejects customer data during import, this script's default `--customer-mode auto` retries without the customer upsert block.

> Screenshot placeholder: Shopify app `Versions` tab showing scopes being added.

### 2D. Install the App on Your Store

In Shopify Dev Dashboard:

1. Open the app.
2. Go to `Home`.
3. Scroll to `Install app`.
4. Select your store.
5. Click `Install`.

If Shopify asks you to approve scopes in the store admin, approve them.

> Screenshot placeholder: Shopify app `Home` screen showing `Install app`.

### 2E. Copy the Store Domain, Client ID, and Client Secret

In Shopify Dev Dashboard:

1. Open the app.
2. Go to `Settings`.
3. Copy the `Client ID`.
4. Copy the `Client secret`.

You also need your Shopify store domain, for example:

```text
your-store.myshopify.com
```

> Screenshot placeholder: Shopify app `Settings` screen showing `Client ID` and `Client secret`.

## Step 3: Open Terminal

The examples below use macOS or Linux Terminal commands.

If you are on Windows, use PowerShell and adjust file paths accordingly.

Change into the folder where the script lives:

```bash
cd /path/to/squarespace-orders-to-shopify
```

Check that Python is available:

```bash
python3 --version
```

## Step 4: Add Your Shopify Credentials

Set these environment variables in your terminal:

```bash
export SHOPIFY_STORE_DOMAIN="your-store.myshopify.com"
export SHOPIFY_CLIENT_ID="your-client-id"
export SHOPIFY_CLIENT_SECRET="your-client-secret"
```

Important:

- do not commit these values to GitHub
- do not paste your client secret into screenshots
- if you open-source your repo, keep secrets in your shell environment or a local `.env` file that is ignored by Git

## Step 5: Run a Safe Dry Run First

This is the safest first command. It cleans the CSV, builds Shopify payloads, and writes a preview file, but it does not create any orders in Shopify.

```bash
python3 squarespace_orders_to_shopify.py \
  ./SquarespaceOrderDownload.csv \
  -o ./SquarespaceOrderDownload.cleaned.csv \
  --import-to-shopify \
  --payload-output ./payload-preview.json \
  --state-file ./import-state.json \
  --test-orders \
  --max-orders 5
```

What this does:

- writes a cleaned CSV file
- builds a JSON preview of the Shopify `orderCreate` payloads
- limits the preview to 5 orders
- marks them as test orders if you later run with `--apply`
- does not create anything in Shopify yet because `--apply` is not included

Check these files after the dry run:

- `SquarespaceOrderDownload.cleaned.csv`
- `payload-preview.json`

Review:

- order totals
- shipping addresses
- discount handling
- tax amounts
- SKU matching

## Step 6: Import a Small Test Batch

Once the dry run looks good, import a small number of test orders first:

```bash
python3 squarespace_orders_to_shopify.py \
  ./SquarespaceOrderDownload.csv \
  -o ./SquarespaceOrderDownload.cleaned.csv \
  --import-to-shopify \
  --payload-output ./payload-preview.json \
  --state-file ./import-state.json \
  --test-orders \
  --max-orders 5 \
  --apply
```

Why this is the safest first live import:

- only 5 orders are attempted
- they are marked as Shopify test orders
- the script remembers which Squarespace order IDs were imported

After the import, verify those orders in Shopify:

1. open the Shopify admin
2. go to `Orders`
3. confirm the orders were created
4. open a few orders and compare them against the Squarespace CSV

## Step 7: Import Real Orders

When you are happy with the test batch, remove `--test-orders` and increase or remove `--max-orders`.

If you already imported test orders for the same Squarespace order IDs and want to replace them with real orders, either:

- use a fresh `--state-file`
- or use `--delete-tracked-test-orders-before-import` and keep a small `--max-orders` value for the next run

Example:

```bash
python3 squarespace_orders_to_shopify.py \
  ./SquarespaceOrderDownload.csv \
  -o ./SquarespaceOrderDownload.cleaned.csv \
  --import-to-shopify \
  --payload-output ./payload-preview.json \
  --state-file ./import-state.json \
  --apply
```

The script spaces out writes by default so it stays under Shopify's documented `orderCreate` limit for development and trial stores.

## If You Want To Re-Run Test Orders

This option deletes tracked Shopify test orders first and then continues into the import run.

If you want to rerun the same small test batch, keep `--max-orders` in place:

```bash
python3 squarespace_orders_to_shopify.py \
  ./SquarespaceOrderDownload.csv \
  --import-to-shopify \
  --state-file ./import-state.json \
  --delete-tracked-test-orders-before-import \
  --test-orders \
  --max-orders 5 \
  --apply
```

Use this carefully:

- it is a delete-then-import command, not a delete-only command
- it only deletes Shopify orders recorded in the local state file
- it only deletes tracked orders that Shopify still marks as test orders

## Most Useful Options

| Option | What it does |
| --- | --- |
| `-o / --output` | Choose where the cleaned CSV should be written. |
| `--import-to-shopify` | Build Shopify order payloads after cleaning the CSV. |
| `--apply` | Actually create orders in Shopify. Without this flag, import mode is dry-run only. |
| `--payload-output` | Save the generated Shopify payloads to a JSON file for review. |
| `--state-file` | Choose where the local import state file should be stored. |
| `--ignore-state` | Try all orders again even if they were already recorded as imported. |
| `--max-orders 5` | Limit the run to the first 5 not-yet-imported orders. Great for testing. |
| `--test-orders` | Mark created Shopify orders as test orders. |
| `--skip-variant-lookup` | Skip SKU matching and import all line items as custom line items. |
| `--customer-mode auto` | Default behavior. Try customer upsert first, then retry without it if Shopify rejects the customer payload. |
| `--order-name-prefix SSQ-` | Add a prefix to the Shopify order name. |

See the full list with:

```bash
python3 squarespace_orders_to_shopify.py --help
```

## How SKU Matching Works

If the Squarespace SKU matches a Shopify variant SKU:

- the imported line item is attached to that Shopify variant

If the Squarespace SKU does not match:

- the order can still import
- that line item becomes a custom line item instead

For the cleanest historical import, make sure your Shopify products and variant SKUs are already set up before importing orders.

## Troubleshooting

### "My orders were skipped"

The script keeps a local state file and skips order IDs that were already marked as imported.

You can:

- delete or move the state file
- use a different `--state-file`
- use `--ignore-state` if you really want to attempt everything again

### "The line items did not attach to products"

Usually this means the Squarespace SKU did not exactly match a Shopify variant SKU.

Check:

- SKU spelling
- spaces or extra characters
- whether the Shopify variant exists yet

### "Customer data failed"

If Shopify rejects the customer payload, the default `--customer-mode auto` retries without the customer upsert block.

If you want a more conservative import, try:

```bash
python3 squarespace_orders_to_shopify.py \
  ./SquarespaceOrderDownload.csv \
  --import-to-shopify \
  --customer-mode none
```

### "I do not see the Dev Dashboard"

Your Shopify account may not have the right permissions.

Shopify's docs say merchants need the `Apps Developer` role to access the Dev Dashboard.

## Security Notes

- never commit CSV exports that contain customer data to a public repo
- never commit `payload-preview.json` if it contains real customer data
- never commit your `import-state.json` file if you do not want to expose internal order mappings
- never commit your Shopify client secret or access token

## Legacy Token Option

If you already have an older Shopify admin-created custom app from before January 1, 2026, this script can also use an Admin API access token instead of client credentials.

Use:

```bash
export SHOPIFY_STORE_DOMAIN="your-store.myshopify.com"
export SHOPIFY_ADMIN_ACCESS_TOKEN="shpat_xxx"
```

In that case, you do not need `SHOPIFY_CLIENT_ID` and `SHOPIFY_CLIENT_SECRET`.

## Official Docs

- Squarespace order export: https://support.squarespace.com/hc/en-us/articles/206540677-Export-Commerce-orders
- Shopify Dev Dashboard app creation: https://shopify.dev/docs/apps/build/dev-dashboard/create-apps-using-dev-dashboard
- Shopify Dev Dashboard access tokens: https://shopify.dev/docs/apps/build/dev-dashboard/get-api-access-tokens
- Shopify client credentials: https://shopify.dev/docs/apps/build/authentication-authorization/client-secrets/index
- Shopify `orderCreate` mutation: https://shopify.dev/docs/api/admin-graphql/latest/mutations/orderCreate
- Shopify dev stores: https://shopify.dev/docs/apps/build/dev-dashboard/stores/development-stores

## Disclaimer

Run this against a test store first. Historical order imports can affect reporting, customer history, and operations. Review a small batch carefully before doing a full import.
