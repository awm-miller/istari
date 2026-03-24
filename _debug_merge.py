from src.config import load_settings
from src.storage.repository import Repository

settings = load_settings()
repository = Repository(
    settings.database_path,
    settings.project_root / "src" / "storage" / "schema.sql",
)
repository.init_db()

ranked = repository.get_ranked_people_for_run(26, limit=500)
raw_edges = repository.get_run_network_edges(26)
address_rows = repository.get_run_address_edges(26)
run_row = repository.get_run(26)

print("seed:", run_row["seed_name"] if run_row else "MISSING")
print("ranked people:", len(ranked))
print("raw_edges:", len(raw_edges))
print("address_rows:", len(address_rows))
if ranked:
    print("first ranked:", dict(ranked[0]))
if raw_edges:
    print("first edge:", dict(raw_edges[0]))
