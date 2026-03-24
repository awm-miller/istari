from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    project_root: Path
    database_path: Path
    cache_dir: Path
    charity_api_key: str | None
    charity_api_base_url: str
    charity_api_key_header: str
    companies_house_api_key: str | None
    companies_house_base_url: str
    gemini_api_key: str | None
    gemini_resolution_model: str
    openai_api_key: str | None
    openai_search_model: str
    openai_resolution_model: str
    openai_base_url: str
    openai_web_search_context: str
    resolution_provider: str
    serper_api_key: str | None
    serper_base_url: str
    user_agent: str
    pdf_enrichment_enabled: bool
    pdf_enrichment_model: str
    pdf_enrichment_max_documents: int
    pdf_enrichment_max_chunks: int


def load_dotenv(dotenv_path: Path) -> None:
    if not dotenv_path.exists():
        return

    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


def load_settings(project_root: Path | None = None) -> Settings:
    root = project_root or Path(__file__).resolve().parents[1]
    load_dotenv(root / ".env")

    database_path = Path(
        os.getenv("DATABASE_PATH", str(root / "data" / "charity_links.sqlite"))
    )
    cache_dir = Path(os.getenv("CACHE_DIR", str(root / "data" / "http_cache")))

    return Settings(
        project_root=root,
        database_path=database_path,
        cache_dir=cache_dir,
        charity_api_key=os.getenv("CHARITY_COMMISSION_API_KEY") or os.getenv("CCEW_API_KEY"),
        charity_api_base_url=os.getenv(
            "CHARITY_COMMISSION_BASE_URL",
            "https://api.charitycommission.gov.uk/register/api",
        ).rstrip("/"),
        charity_api_key_header=os.getenv(
            "CHARITY_COMMISSION_API_KEY_HEADER",
            "Ocp-Apim-Subscription-Key",
        ),
        companies_house_api_key=os.getenv("COMPANIES_HOUSE_API_KEY"),
        companies_house_base_url=os.getenv(
            "COMPANIES_HOUSE_BASE_URL",
            "https://api.company-information.service.gov.uk",
        ).rstrip("/"),
        gemini_api_key=os.getenv("GEMINI_API_KEY"),
        gemini_resolution_model=os.getenv("GEMINI_RESOLUTION_MODEL", "gemini-2.5-flash"),
        openai_api_key=os.getenv("OPENAI_API_KEY"),
        openai_search_model=os.getenv("OPENAI_SEARCH_MODEL", "gpt-4.1-mini"),
        openai_resolution_model=os.getenv("OPENAI_RESOLUTION_MODEL", "gpt-4.1-mini"),
        openai_base_url=os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/"),
        openai_web_search_context=os.getenv("OPENAI_WEB_SEARCH_CONTEXT", "medium"),
        resolution_provider=os.getenv("RESOLUTION_PROVIDER", "gemini"),
        serper_api_key=os.getenv("SERPER_API_KEY"),
        serper_base_url=os.getenv("SERPER_BASE_URL", "https://google.serper.dev").rstrip("/"),
        user_agent=os.getenv("USER_AGENT", "project-istari/0.1"),
        pdf_enrichment_enabled=os.getenv("PDF_ENRICHMENT_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"},
        pdf_enrichment_model=os.getenv("PDF_ENRICHMENT_MODEL", os.getenv("GEMINI_RESOLUTION_MODEL", "gemini-2.5-flash")),
        pdf_enrichment_max_documents=int(os.getenv("PDF_ENRICHMENT_MAX_DOCUMENTS", "3")),
        pdf_enrichment_max_chunks=int(os.getenv("PDF_ENRICHMENT_MAX_CHUNKS", "4")),
    )
