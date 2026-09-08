#!/usr/bin/env python
"""Thin wrapper -> pravaha.scripts.load_test.

    python scripts/load_test/run_load_test.py --sweep 100,500,1000 --duration 30 --bootstrap --out load-test-results/run.json
"""
from pravaha.scripts.load_test import main

if __name__ == "__main__":
    main()
