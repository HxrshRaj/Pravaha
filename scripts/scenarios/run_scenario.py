#!/usr/bin/env python
"""Thin wrapper -> pravaha.scripts.scenarios.

    python scripts/scenarios/run_scenario.py list
    python scripts/scenarios/run_scenario.py run payment_failure_spike --bootstrap
"""
from pravaha.scripts.scenarios import main

if __name__ == "__main__":
    main()
