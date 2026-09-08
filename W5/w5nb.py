# %%
import os
import pandas as pd
import numpy as np
from datetime import datetime

# %%
CUSTOMER_CSV = "customer_dataset.csv"
TRANSACTION_CSV = "transaction_dataset.csv"
OUTPUT_DIR = "./w5/"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Thresholds for transaction amounts
LOW_THRESHOLD = 2500.0
HIGH_THRESHOLD = 7500.0

# Common key mappings for city name standardization
CITY_MAP_STANDARD = {
    'Cochin': 'Kochi',
    'Cmbt': 'Coimbatore',
    'Hyd': 'Hyderabad',
    'Trivandrm': 'Trivandrum',
    'Poona': 'Pune'
}

# %%
def standardize_city(name):
    if pd.isnull(name):
        return np.nan
    s = str(name).strip().title()
    return CITY_MAP_STANDARD.get(s, s.title())

# %%
def classify_amount(amount):
    try:
        a = float(amount)
    except Exception:
        return np.nan
    if a <= 0:
        return np.nan
    if a < LOW_THRESHOLD:
        return "Low"
    elif a <= HIGH_THRESHOLD:
        return "Medium"
    else:
        return "High"

# %% [markdown]
# # 1. Load

# %%
print("Loading datasets...")
df_customers = pd.read_csv(CUSTOMER_CSV, dtype=str)
df_transactions = pd.read_csv(TRANSACTION_CSV, dtype=str)

print("\nCustomer schema & sample:")
print(df_customers.dtypes)
print(df_customers.head(5))

print("\nTransaction schema & sample:")
print(df_transactions.dtypes)
print(df_transactions.head(5))

cust = df_customers.copy()
txn = df_transactions.copy()

# %%
pd.to_numeric(txn['transaction_amount']).describe()

# %% [markdown]
# # 2. Transform

# %%
cust.columns = [c.strip() for c in cust.columns]
txn.columns = [c.strip() for c in txn.columns]

if 'city' in cust.columns:
    cust['city_original'] = cust['city']
    cust['city'] = cust['city'].apply(standardize_city)

if 'transaction_amount' in txn.columns:
    txn['transaction_amount_original'] = txn['transaction_amount']
    txn['transaction_amount'] = pd.to_numeric(txn['transaction_amount'], errors='coerce')

txn['amount_category'] = txn['transaction_amount'].apply(classify_amount)

txn['transaction_date_parsed'] = pd.to_datetime(txn['transaction_date'], errors='coerce')
txn['transaction_month'] = txn['transaction_date_parsed'].dt.month

for df in (cust, txn):
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)


# %%
txn.info()

# %% [markdown]
# # 3. Cleaning

# %%
customer_invalid_rows = pd.DataFrame(columns=cust.columns)
transaction_invalid_rows = pd.DataFrame(columns=txn.columns)

# (A) Handle duplicates: If records are identical across all columns -> Mark duplicates -> keep first, others invalid
def extract_duplicate_invalids(df, dataset_name):
    dup_mask = df.duplicated(keep='first')
    invalids = df[dup_mask].copy()
    cleaned = df[~dup_mask].copy()
    print(f"{dataset_name}: found {dup_mask.sum()} duplicate rows (kept first instance).")
    return cleaned, invalids

cust_clean, cust_dup_invalids = extract_duplicate_invalids(cust, "Customers")
txn_clean, txn_dup_invalids = extract_duplicate_invalids(txn, "Transactions")
customer_invalid_rows = pd.concat([customer_invalid_rows, cust_dup_invalids], ignore_index=True, sort=False)
transaction_invalid_rows = pd.concat([transaction_invalid_rows, txn_dup_invalids], ignore_index=True, sort=False)

# (B) Invalid customer mask
cust_invalid_mask = (
    cust_clean['customer_id'].isnull() | (cust_clean['customer_id'].astype(str).str.strip() == '') |
    (cust_clean.get('city', pd.Series()).isnull() | (cust_clean['city'].astype(str).str.strip() == '')) |
    (cust_clean.get('status', pd.Series()).isnull() | (cust_clean['status'].astype(str).str.strip() == ''))
)

# (C) Invalid transaction mask
txn_invalid_mask = (
    txn_clean['transaction_id'].isnull() | (txn_clean['transaction_id'].astype(str).str.strip() == '') |
    txn_clean['customer_id'].isnull() | (txn_clean['customer_id'].astype(str).str.strip() == '') |
    txn_clean['transaction_amount'].isnull() | (txn_clean['transaction_amount'] <= 0) |
    txn_clean['transaction_date_parsed'].isnull()
)

# (D) Referential integrity mask
existing_customer_ids = set(cust_clean['customer_id'].astype(str).unique())
txn_clean['customer_id_str'] = txn_clean['customer_id'].astype(str)
ref_invalid_mask = ~txn_clean['customer_id_str'].isin(existing_customer_ids)

# (E) Apply all masks and extract invalid rows
# Customer invalid
cust_invalids = cust_clean[cust_invalid_mask].copy()
cust_clean = cust_clean[~cust_invalid_mask].copy()
print(f"Customers: extracted {len(cust_invalids)} invalid rows (missing key fields).")

# Transaction invalid
txn_invalids = txn_clean[txn_invalid_mask].copy()
txn_clean = txn_clean[~txn_invalid_mask].copy()
print(f"Transactions: extracted {len(txn_invalids)} invalid rows (missing keys, non-positive amounts, or bad dates).")

# Referential integrity invalid transactions
ref_invalids = txn_clean[ref_invalid_mask].copy()
txn_clean = txn_clean[~ref_invalid_mask].copy()
print(f"Transactions: {len(ref_invalids)} rows with customer_id not found in customers (referential integrity).")

# (F) Concatenate invalid rows
customer_invalid_rows = pd.concat([customer_invalid_rows, cust_invalids], ignore_index=True, sort=False)
transaction_invalid_rows = pd.concat([transaction_invalid_rows, txn_invalids, ref_invalids], ignore_index=True, sort=False)

# (G) Verify and format dates as YYYY-MM-DD in cleaned txn
txn_clean['transaction_date'] = txn_clean['transaction_date_parsed'].dt.strftime('%Y-%m-%d')

# Reset indexes on all outputs
cust_clean = cust_clean.reset_index(drop=True)
txn_clean = txn_clean.reset_index(drop=True)
customer_invalid_rows = customer_invalid_rows.reset_index(drop=True)
transaction_invalid_rows = transaction_invalid_rows.reset_index(drop=True)


# %%
print("\n--- Sample invalid customer rows ---")
print(customer_invalid_rows.head(5))
print("\n--- Sample cleaned customer rows ---")
print(cust.head(5))

print("\n--- Sample invalid transaction rows ---")
print(transaction_invalid_rows.head(5))
print("\n--- Sample cleaned transaction rows ---")
print(txn.head(5))

# %% [markdown]
# # 4. Join

# %%
# Join customers and transactions on customer_id
joined = txn_clean.merge(cust_clean, how='inner', on='customer_id', suffixes=('_txn', '_cust'))

print(f"\nJoined rows: {len(joined)}")
print(joined.head(5))

# %% [markdown]
# # 5. Aggregations

# %%
# Total and average transaction amount per customer
agg_customer = (
    joined.groupby(['customer_id', 'customer_name'], as_index=False)
    .agg(
        total_transaction_amount = ('transaction_amount', 'sum'),
        average_transaction_amount = ('transaction_amount', 'mean'),
        transaction_count = ('transaction_amount', 'count')
    )
    .sort_values('total_transaction_amount', ascending=False)
)

# Total transaction amount per city
if 'city' in joined.columns:
    agg_city = (
        joined.groupby('city', as_index=False)
        .agg(total_transaction_amount = ('transaction_amount', 'sum'),
             transaction_count = ('transaction_amount', 'count'))
        .sort_values('total_transaction_amount', ascending=False)
    )
else:
    agg_city = pd.DataFrame()

# Top 3 customers by total transaction amount
top_3_customers = agg_customer.head(3).copy()

print("\n--- Aggregation: total & avg per customer (sample) ---")
print(agg_customer.head(5))

print("\n--- Aggregation: total per city (sample) ---")
print(agg_city.head(5))

print("\n--- Top 3 customers by total transaction amount ---")
print(top_3_customers)

# %%
cust.to_csv(os.path.join(OUTPUT_DIR, "cleaned_customers.csv"), index=False)
txn.to_csv(os.path.join(OUTPUT_DIR, "cleaned_transactions.csv"), index=False)

# invalid_rows:
customer_invalid_rows.to_csv(os.path.join(OUTPUT_DIR, "invalid_customers.csv"), index=False)
transaction_invalid_rows.to_csv(os.path.join(OUTPUT_DIR, "invalid_transactions.csv"), index=False)

# joined_data
joined.to_csv(os.path.join(OUTPUT_DIR, "joined_data.csv"), index=False)

# aggregates
agg_customer.to_csv(os.path.join(OUTPUT_DIR, "aggregate_total_avg_per_customer.csv"), index=False)
agg_city.to_csv(os.path.join(OUTPUT_DIR, "aggregate_total_per_city.csv"), index=False)
top_3_customers.to_csv(os.path.join(OUTPUT_DIR, "top_3_customers.csv"), index=False)

print(f"\nAll output CSVs written to {OUTPUT_DIR}")
print("Files created:")
for f in os.listdir(OUTPUT_DIR):
    print(" -", f)


