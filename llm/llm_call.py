import os
import json
import time
import re
from typing import Dict, Any, List, Optional, Tuple

from dotenv import load_dotenv
from google import genai


load_dotenv()

MODEL_NAME = os.getenv("MODEL_NAME")
GCP_PROJECT = os.getenv("GCP_PROJECT", "lp-innovation")
GCP_LOCATION = os.getenv("GCP_LOCATION", "global")

LLM_TIMEOUT_SEC = int(os.getenv("LLM_TIMEOUT_SEC", "60"))
LLM_MAX_RETRIES = int(os.getenv("LLM_MAX_RETRIES", "3"))

DEFAULT_REPO_PATH = os.getenv("PROJECT_DIR", "./")
MAX_FILES = int(os.getenv("MAX_FILES", "150"))
MAX_BYTES_PER_FILE = int(os.getenv("MAX_BYTES_PER_FILE", "200000"))
MAX_TOTAL_BYTES = int(os.getenv("MAX_TOTAL_BYTES", "4000000"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
REDACT_SECRETS = os.getenv("REDACT_SECRETS", "true").lower() == "true"

# Allowed Extensions
ALLOW_EXT = {
    ".sql", ".ddl", ".dml", ".yaml", ".yml",
    ".py", ".sh", ".cfg", ".ini", ".toml", ".json", ".txt", ".md"
}

DENY_DIRS = {".git", ".venv", "venv", "__pycache__", "node_modules", ".mypy_cache", ".pytest_cache", "dist", "build"}

PREFERRED_INCLUDE_DIRS = ["engine", "data_registry"]

SECRET_PATTERNS = [
    r"(?i)(api[_-]?key|secret|token|password)\s*[:=]\s*[f'\"']?[A-Za-z0-9_\-\.]{8,}['\"']?",
    r"(?i)(jdbc|odbc):[^\s]+",
    r"(?i)aws(.{0,10})?(access|secret)[^\n]+",
    r"[\w\.-]+@[\w\.-]+\.[A-Za-z]{2,}",
]


def redact(text: str) -> str:
    if not REDACT_SECRETS or not text:
        return text
    out = text
    for pat in SECRET_PATTERNS:
        out = re.sub(pat, "[REDACTED]", out)
    return out


def log(msg: str, level: str = "INFO"):
    levels = ["DEBUG", "INFO", "WARN", "ERROR"]
    if level not in levels:
        level = "INFO"
    if levels.index(level) >= levels.index(LOG_LEVEL):
        print(f"[{level}] {msg}")


def chunk_text(text: str, max_chars: int = 120_000) -> List[str]:
    """Split large text into manageable chunks for the LLM."""
    chunks = []
    start = 0
    n = len(text)
    while start < n:
        end = min(start + max_chars, n)
        chunks.append(text[start:end])
        start = end
    return chunks


def _is_included_dir(repo_path: str, root: str, include_dirs: Optional[List[str]]) -> bool:
    if not include_dirs:
        return True
    rel = os.path.relpath(root, repo_path).split(os.sep)
    if not rel or rel[0] == '.':
        return True
    return rel[0] in include_dirs


def read_repo_files(
    repo_path: str,
    allow_ext: Optional[set] = None,
    deny_dirs: Optional[set] = None,
    include_dirs: Optional[List[str]] = None,
    max_files: int = MAX_FILES,
    max_bytes_per_file: int = MAX_BYTES_PER_FILE,
    max_total_bytes: int = MAX_TOTAL_BYTES,
) -> List[Tuple[str, str]]:
    """
    Returns a list of (relative_path, content) pairs under size budgets.
    Only includes files whose top-level dir is in include_dirs if provided.
    """
    allow_ext = allow_ext or ALLOW_EXT
    deny_dirs = deny_dirs or DENY_DIRS

    collected: List[Tuple[str, str]] = []
    total_bytes = 0

    for root, dirs, files in os.walk(repo_path):
        # prune deny dirs
        dirs[:] = [d for d in dirs if d not in deny_dirs]

        if not _is_included_dir(repo_path, root, include_dirs):
            continue

        for fn in files:
            ext = os.path.splitext(fn)[1].lower()
            if ext not in allow_ext:
                continue
            if len(collected) >= max_files:
                log("Reached MAX_FILES budget; skipping additional files", "WARN")
                return collected

            path = os.path.join(root, fn)
            try:
                size = os.path.getsize(path)
                if size > max_bytes_per_file:
                    log(f"Skipping large file {path} ({size} bytes)", "DEBUG")
                    continue
                if total_bytes + size > max_total_bytes:
                    log("Reached MAX_TOTAL_BYTES budget; stopping scan", "WARN")
                    return collected

                with open(path, "r", encoding="utf-8", errors="ignore") as f:
                    content = f.read()
                rel = os.path.relpath(path, repo_path)
                collected.append((rel, content))
                total_bytes += size
            except Exception as e:
                log(f"Failed to read {path}: {e}", "WARN")
    return collected


def make_repo_context(chunks: List[Tuple[str, str]]) -> str:
    """
    Flattens repo into a deterministic, LLM-friendly block.
    """
    parts = []
    for rel, content in chunks:
        snippet = content.strip()
        parts.append(f"\n### FILE: {rel}\n{snippet}\n")
    return "\n".join(parts)


def lineage_json_schema() -> Dict[str, Any]:
    """
    Simplified output: a single array of lineage records.
    Each record captures source/target schema+table and the relationship.
    """
    return {
        "type": "object",
        "properties": {
            "lineage": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["source_schema", "source_table", "target_schema", "target_table", "relationship"],
                    "properties": {
                        "source_schema": {"type": "string"},
                        "source_table": {"type": "string"},
                        "target_schema": {"type": "string"},
                        "target_table": {"type": "string"},
                        "relationship": {"type": "string"}
                    }
                }
            },
            "quality_issues": {
                "type": "array",
                "items": {"type": "string"}
            }
        },
        "required": ["lineage"]
    }


def build_instruction_only_prompt() -> str:
    """
    Returns a single instruction string (no repo content inside).
    Repo snapshot will be appended as separate messages to avoid interpolation issues.
    """
    schema_json = json.dumps(lineage_json_schema(), indent=2)

    instruction = (
        "You are a deterministic code and SQL lineage analyst.\n"
        "Repository structure and intent (very important for extraction):\n"
        " - 'data_registry/': registry / placeholders for table classes. Contains metadata like table names,\n"
        "   columns, and especially schema names. Use this to resolve schema/table identifiers.\n"
        " - 'engine/': pipeline/task code. For each sink, infer the TARGET table (schema+table). In the code,\n"
        "   you will also see which source tables are loaded/read before the sink writes; these define SOURCES.\n"
        " - 'utils/': general utilities (usually not lineage-relevant); ignore unless directly referencing tables.\n\n"
        "Goal: produce STRICT JSON matching the JSON-SCHEMA below. For each edge discovered, output one record with\n"
        "  source_schema, source_table, target_schema, target_table, relationship (e.g., 'reads_from', 'writes_to', 'transforms').\n"
        "Find table names and schemas from 'data_registry', find target tables (sinks) and source tables from 'engine'.\n"
        "If uncertain, add a concise note to 'quality_issues' rather than inventing.\n\n"
        "Output requirements:\n"
        " - Return ONLY valid JSON (no markdown, no prose, no comments, no code fences).\n"
        " - No trailing commas or NaN/Infinity.\n"
        " - Conform exactly to this JSON-SCHEMA:\n\n"
        f"{schema_json}\n"
    )
    return instruction


def strip_code_fences(s: str) -> str:
    s = s.strip()
    if s.startswith("```"):
        s = re.sub(r"^```[a-zA-Z]*\s*", "", s)
        s = re.sub(r"\s*```$", "", s)
    return s.strip()


def safe_gemini_call_for_repo(repo_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    repo_path = repo_path or DEFAULT_REPO_PATH
    if not os.path.exists(repo_path):
        log(f"Provided repo path does not exist: {repo_path}", "ERROR")
        return None

    # 1) Prefer scanning only the most relevant top-level dirs
    files = read_repo_files(repo_path, include_dirs=PREFERRED_INCLUDE_DIRS)
    if not files:
        # Fallback: scan everything under repo_path
        log("Preferred dirs returned no files; scanning full repo path.", "WARN")
        files = read_repo_files(repo_path, include_dirs=None)

    if not files:
        log(f"No eligible files found in {repo_path}", "WARN")
        return None

    repo_context = make_repo_context(files)
    log(redact(f"[DEBUG] Scanned {len(files)} files for lineage."), "DEBUG")

    instruction = build_instruction_only_prompt()
    repo_chunks = chunk_text(repo_context, max_chars=120_000)

    client = genai.Client(
        vertexai=True,
        project=GCP_PROJECT,
        location=GCP_LOCATION
    )

    # Retry strategy: progressively reduce repo chunks to avoid empty/overload responses
    # attempt 1 -> up to 8 chunks, attempt 2 -> 4, attempt 3 -> 2
    chunk_limits = [8, 4, 2]
    last_err = None

    for attempt in range(1, LLM_MAX_RETRIES + 1):
        try:
            max_chunks = chunk_limits[min(attempt - 1, len(chunk_limits) - 1)]
            selected_chunks = repo_chunks[:max_chunks]

            contents = []
            contents.append({"role": "user", "parts": [{"text": "[SYSTEM]\n" + instruction}]})
    
            # Append repository snapshot as separate messages
            total = len(selected_chunks)
            for idx, ch in enumerate(selected_chunks, 1):
                contents.append({
                    "role": "user",
                    "parts": [{"text": f"[REPOSITORY SNIPPET {idx}/{total}]\n{ch}"}]
                })

            resp = client.models.generate_content(
                model=MODEL_NAME,
                contents=contents,
                config={
                    "temperature": 0.0,
                    "top_p": 0.0,
                    "response_mime_type": "application/json",
                    # "max_output_tokens": 8192,
                },
            )

            raw = (resp.text or "").strip()
            if not raw:
                raise RuntimeError("Empty response from model.")

            raw = strip_code_fences(raw)
            payload = json.loads(raw)

            # Minimal shape check
            if not isinstance(payload, dict) or "lineage" not in payload:
                raise ValueError("Response missing required key: 'lineage'")

            return payload

        except Exception as e:
            last_err = e
            log(f"[Attempt {attempt}/{LLM_MAX_RETRIES}] LLM lineage extraction failed: {e}", "WARN")
            time.sleep(min(2 ** attempt, 8))  # backoff

    log(f"LLM call failed after {LLM_MAX_RETRIES} retries: {last_err}", "ERROR")
    return None


if __name__ == "__main__":
    log(f"Using model={MODEL_NAME} project={GCP_PROJECT} location={GCP_LOCATION}", "INFO")
    log(f"Repo path: {os.path.abspath(DEFAULT_REPO_PATH)}", "INFO")
    data = safe_gemini_call_for_repo(DEFAULT_REPO_PATH)
    if data is None:
        print("No lineage extracted.")
    else:
        print(json.dumps(data, indent=2))