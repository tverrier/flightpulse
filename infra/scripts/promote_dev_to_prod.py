#!/usr/bin/env python3
"""Promote FLIGHTPULSE_DEV.MARTS → FLIGHTPULSE_PROD.MARTS atomically.

1. Row-count parity: every model's dev row count must be within `tolerance_pct`
   of prod (defaults: ≥99% of prod rows, ≤105% to catch runaway joins).
2. Atomic swap via `ALTER SCHEMA ... SWAP WITH`.

Usage:  python infra/scripts/promote_dev_to_prod.py [--dry-run] [--tolerance 0.01]
"""

from __future__ import annotations

import argparse
import os
import sys

import snowflake.connector


def _connect():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_ROLE", "RL_FLIGHTPULSE_TRANSFORMER"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "WH_FLIGHTPULSE_XS"),
    )


def _list_tables(cur, db: str, schema: str) -> list[str]:
    cur.execute(
        f"SELECT TABLE_NAME FROM {db}.INFORMATION_SCHEMA.TABLES "
        f"WHERE TABLE_SCHEMA = %s AND TABLE_TYPE='BASE TABLE'",
        (schema,),
    )
    return [row[0] for row in cur.fetchall()]


def _count(cur, db: str, schema: str, table: str) -> int:
    cur.execute(f'SELECT COUNT(*) FROM "{db}"."{schema}"."{table}"')
    return int(cur.fetchone()[0])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--tolerance", type=float, default=0.01,
                        help="Allowed downward delta as a fraction (default 0.01 = 1%)")
    parser.add_argument("--upper-bound", type=float, default=0.05,
                        help="Allowed upward delta as a fraction (default 0.05 = 5%)")
    parser.add_argument("--schema", default="MARTS")
    args = parser.parse_args()

    with _connect() as conn, conn.cursor() as cur:
        prod_tables = _list_tables(cur, "FLIGHTPULSE_PROD", args.schema)
        dev_tables = _list_tables(cur, "FLIGHTPULSE_DEV", args.schema)
        common = sorted(set(prod_tables) & set(dev_tables))
        if not common:
            print("No common tables — nothing to promote.")
            return 1

        problems: list[str] = []
        for table in common:
            prod_n = _count(cur, "FLIGHTPULSE_PROD", args.schema, table)
            dev_n = _count(cur, "FLIGHTPULSE_DEV", args.schema, table)
            if prod_n == 0:
                print(f"  ! {table}: prod=0, skipping ratio check (dev={dev_n})")
                continue
            ratio = dev_n / prod_n
            ok = (1.0 - args.tolerance) <= ratio <= (1.0 + args.upper_bound)
            flag = "OK" if ok else "FAIL"
            print(f"  {flag} {table}: prod={prod_n:>10}  dev={dev_n:>10}  ratio={ratio:.4f}")
            if not ok:
                problems.append(table)

        if problems:
            print(f"\nABORT: {len(problems)} table(s) outside tolerance: {problems}")
            return 2

        if args.dry_run:
            print("\n--dry-run set; not swapping.")
            return 0

        print(f"\nSwapping FLIGHTPULSE_PROD.{args.schema} <-> FLIGHTPULSE_DEV.{args.schema} ...")
        cur.execute(
            f"ALTER SCHEMA FLIGHTPULSE_PROD.{args.schema} SWAP WITH FLIGHTPULSE_DEV.{args.schema}"
        )
        print("swap complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
