"""Rebuild graph HTML from consolidate_and_graph.py, then copy to netlify."""
import importlib.util
import pathlib
import sys

spec = importlib.util.spec_from_file_location("cg", str(pathlib.Path(__file__).parent / "consolidate_and_graph.py"))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from src.config import load_settings
from src.storage.repository import Repository

settings = load_settings()
repository = Repository(
    settings.database_path,
    settings.project_root / "src" / "storage" / "schema.sql",
)
repository.init_db()

rows = repository.connect().execute("select id from runs order by id").fetchall()
run_ids = [int(row["id"]) for row in rows]
if not run_ids:
    raise SystemExit("No runs found.")

print(f"Consolidating runs {run_ids}...", flush=True)
data = m.consolidate_multi_run(run_ids)
print(f"  {len(data['nodes'])} nodes, {len(data['edges'])} edges", flush=True)

html = m.render_html(data)
print(f"Rendered HTML ({len(html)} bytes)", flush=True)

out = pathlib.Path("output/latest_graph.html")
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(html, encoding="utf-8")
print(f"Wrote {out} ({len(html)} bytes)", flush=True)

netlify = pathlib.Path("netlify_graph_viewer/index.html")
if netlify.parent.exists():
    netlify.write_text(html, encoding="utf-8")
    print(f"Wrote {netlify} ({len(html)} bytes)", flush=True)

print("Done.", flush=True)
