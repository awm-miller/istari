from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.config import load_settings
from src.resolution.benchmark import benchmark_historical_decisions


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare Nemotron resolution with stored decisions.")
    parser.add_argument("--database", type=Path, default=Path("data/istari_latest.db"))
    parser.add_argument("--per-status", type=int, default=4)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/benchmarks/openrouter-vs-baseline.json"),
    )
    args = parser.parse_args()

    result = benchmark_historical_decisions(
        database_path=args.database,
        settings=load_settings(),
        per_status=max(1, args.per_status),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(
        f"{result['agreement_count']}/{result['sample_count']} decisions agree "
        f"({result['agreement_rate']:.1%}); report: {args.output}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
