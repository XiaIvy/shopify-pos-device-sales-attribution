# Shopify POS Device Attribution Report

This project fills a reporting gap in Shopify by attributing POS sales to device IDs for a specific staff member over a chosen time window.

Shopify's built-in reports are great for standard sales views, but they can be limiting when you need to answer operational questions like:

- Which register or iPad handled a staff member's sales?
- How much volume moved through each POS device?
- Can I cross-check staff activity against device-level transaction data?

This script pulls Shopify Admin GraphQL order data, matches POS timeline events to a staff member, inspects payment transactions for POS device IDs, and aggregates the results into a CSV that is easier to analyze outside Shopify.

## Why This Exists

Teams often need reporting that goes beyond what Shopify exposes in the admin dashboard. This utility is useful when you need to:

- Build staff-level operational audits
- Reconcile POS activity by register or device
- Create custom exports for deeper spreadsheet analysis
- Answer one-off store questions without building a full app

## How It Works

For a given day or datetime range, the script:

1. Queries Shopify POS orders through the Admin GraphQL API
2. Reads POS timeline events to identify the staff member who processed the order
3. Looks at successful payment transactions to find device IDs
4. Attributes order totals to the detected POS devices
5. Exports aggregated totals to CSV

## Project Structure

- `main.py`: main script
- `.env.example`: sample environment configuration
- `examples/sample_device_totals.csv`: sanitized sample output

## Setup

This script uses only Python's standard library, so there are no package dependencies to install.

1. Copy `.env.example` to `.env`
2. Fill in your private Shopify credentials
3. Run the script with a staff name and date filter

Example `.env` keys:

- `SHOPIFY_SHOP_DOMAIN`
- `SHOPIFY_CLIENT_ID`
- `SHOPIFY_CLIENT_SECRET`
- `SHOPIFY_API_VERSION`

## Usage

Example for a single day:

```bash
python3 main.py \
  --staff "Alex Johnson" \
  --day 2026-02-27 \
  --timezone America/Chicago \
  --output staff_device_totals.csv
```

Example for a custom range:

```bash
python3 main.py \
  --staff "Alex Johnson" \
  --start 2026-02-27T00:00:00 \
  --end 2026-02-28T00:00:00 \
  --timezone America/Chicago \
  --output staff_device_totals.csv
```

Optional flags:

- `--include-unknown` to keep orders where no POS device ID was returned
- `--limit` to cap the number of orders processed during testing
- `--match-mode` to control how staff matching is applied

## Sample Output

See `examples/sample_device_totals.csv` for a sanitized example export.

Columns:

- `device_id`
- `location_name`
- `register_hint`
- `orders_count`
- `transactions_count`
- `total_sales`
