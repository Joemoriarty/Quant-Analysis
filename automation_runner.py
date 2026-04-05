from __future__ import annotations

import argparse
import json

from portfolio.automation_workflows import run_named_workflow


def main() -> None:
    parser = argparse.ArgumentParser(description="Run scheduled market maintenance jobs.")
    parser.add_argument("task", choices=["daily-update", "weekly-optimize"])
    parser.add_argument("--pool-size", type=int, default=None, help="Override the automation pool size.")
    args = parser.parse_args()

    result = run_named_workflow(args.task, pool_size=args.pool_size)
    print(json.dumps(result, ensure_ascii=False, default=str, indent=2))


if __name__ == "__main__":
    main()
