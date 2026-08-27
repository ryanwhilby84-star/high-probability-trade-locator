"""One-shot classifier for git status review (do not commit)."""
from __future__ import annotations

import subprocess
from pathlib import Path


def main() -> None:
    out = subprocess.check_output(
        ["git", "status", "--porcelain", "-u"],
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    entries: list[tuple[str, str]] = []
    for line in out.splitlines():
        if not line.strip():
            continue
        st = line[:2].strip()
        path = line[3:].replace("\\", "/")
        if " -> " in path:
            path = path.split(" -> ", 1)[1].strip('"')
        entries.append((st, path))

    def is_source(p: str) -> bool:
        if p == "requirements.txt":
            return True
        if p.startswith(".cursor/rules/"):
            return True
        if p.startswith("docs/"):
            return True
        if p.startswith("src/") or p.startswith("tests/"):
            return True
        if p.startswith("scripts/") and not Path(p).name.startswith("_"):
            return True
        if p.startswith("web-dashboard/src/") or p.startswith("web-dashboard/scripts/"):
            return True
        if p == "web-dashboard/vite.config.js":
            return True
        return False

    def is_temp(p: str) -> bool:
        name = Path(p).name
        if p.startswith(".cursor/") and not p.startswith(".cursor/rules/"):
            return True
        if p.startswith(".pytest_cache") or "__pycache__" in p:
            return True
        if p.startswith("scripts/") and name.startswith("_"):
            return True
        if p.endswith(".bak") or p.endswith(".log"):
            return True
        if "/exports/" in p and any(
            x in p
            for x in (
                "manual_",
                "weekly_cot_update",
                "confluence_stage_progress",
                "profile",
            )
        ):
            return True
        # EI / Playwright screenshot dumps and one-off proof artefacts
        proof_bits = (
            "audit_live_",
            "chip_click",
            "chrome_only",
            "debug_visibility",
            "full_ws_now",
            "gold_first_viewport",
            "gold_instrument_embed",
            "gold_normal_nav",
            "gold_research_",
            "gold_visibility_",
            "copper_comm",
            "copper_nc",
            "copper_price",
            "copper_ws",
            "dxy_instrument_page",
            "dxy_nav_proof",
            "dxy_scanner",
            "dxy_sidebar",
            "weekly_inspector_band",
            "weekly_inspector_drawer",
            "weekly_inspector_flow",
            "ng_valuation_v2_provisional",
            "trajectory_reasoning_debug",
            "dx_f_stooq_page",
        )
        if any(b in p for b in proof_bits):
            return True
        if p.startswith("data/audits/") and p.endswith((".png", ".svg")):
            return True
        if p.startswith("data/cache/"):
            return True
        return False

    def is_gen(p: str) -> bool:
        if p.startswith("web-dashboard/dist/"):
            return True
        if p.startswith("web-dashboard/public/data/"):
            return True
        if p.startswith("data/"):
            return True
        return False

    src: list[tuple[str, str]] = []
    gen: list[tuple[str, str]] = []
    temp: list[tuple[str, str]] = []
    other: list[tuple[str, str]] = []
    for st, p in entries:
        if is_temp(p):
            temp.append((st, p))
        elif is_source(p):
            src.append((st, p))
        elif is_gen(p):
            gen.append((st, p))
        else:
            other.append((st, p))

    def dump(title: str, rows: list[tuple[str, str]], limit: int = 500) -> None:
        print(f"\n=== {title} ({len(rows)}) ===")
        for st, p in sorted(rows, key=lambda x: x[1])[:limit]:
            print(f"{st:2} {p}")
        if len(rows) > limit:
            print(f"... +{len(rows) - limit} more")

    dump("SOURCE", src)
    dump("GENERATED", gen)
    dump("TEMP", temp)
    dump("OTHER", other)


if __name__ == "__main__":
    main()
