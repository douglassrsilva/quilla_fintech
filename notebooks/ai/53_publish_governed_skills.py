# Databricks notebook source
# MAGIC %md
# MAGIC # Quilla · Governed Skills no Unity AI Gateway
# MAGIC
# MAGIC Publica de forma idempotente os três bundles de inteligência externa como
# MAGIC securables `catalog.schema.skill`, usando a API Beta oficial de Unity Catalog Skills.

# COMMAND ----------

import json
from pathlib import Path
import re

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import AlreadyExists


dbutils.widgets.text("catalog", "example_catalog")
dbutils.widgets.text("ai_schema", "example_ai")
dbutils.widgets.text("skill_root", "")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("ai_schema").strip()
SKILL_ROOT_VALUE = dbutils.widgets.get("skill_root").strip()
if not SKILL_ROOT_VALUE:
    raise ValueError("skill_root is required")
SKILL_ROOT = Path(SKILL_ROOT_VALUE)
SKILLS_API = "/api/2.1/unity-catalog/skills"
FILES_API = "/api/2.0/fs/files"
SKILL_NAMES = (
    "quilla-external-intelligence",
    "quilla-source-verification",
    "quilla-impact-scenario-analysis",
)
NAME_PATTERN = re.compile(r"(?m)^name:\s*([^\s]+)\s*$")
w = WorkspaceClient()
api = w.api_client


def load_bundle(leaf: str) -> dict[str, bytes]:
    root = SKILL_ROOT / leaf
    skill_md = root / "SKILL.md"
    if not skill_md.is_file():
        raise ValueError(f"Missing {skill_md}")
    match = NAME_PATTERN.search(skill_md.read_text(encoding="utf-8"))
    if not match or match.group(1) != leaf:
        raise ValueError(f"SKILL.md name must match governed leaf {leaf}")
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file() and not any(part.startswith(".") for part in path.parts)
    }


def publish_skill(leaf: str) -> dict:
    try:
        api.do(
            "POST",
            SKILLS_API,
            query={"parent": f"schemas/{CATALOG}.{SCHEMA}", "skill_id": leaf},
            body={},
        )
        action = "created"
    except AlreadyExists:
        action = "updated"

    bundle = load_bundle(leaf)
    for relative_path, content in sorted(bundle.items()):
        api.do(
            "PUT",
            f"{FILES_API}/Skills/{CATALOG}/{SCHEMA}/{leaf}/{relative_path}",
            headers={"Content-Type": "application/octet-stream"},
            data=content,
        )
    api.do("POST", f"{SKILLS_API}/{CATALOG}.{SCHEMA}.{leaf}/finalize")
    metadata = api.do("GET", f"{SKILLS_API}/{CATALOG}.{SCHEMA}.{leaf}")
    assert metadata.get("finalize_time"), f"Skill {leaf} was not finalized"
    assert metadata.get("bundle_name") == leaf
    return {
        "action": action,
        "full_name": f"{CATALOG}.{SCHEMA}.{leaf}",
        "id": metadata.get("id"),
        "bundle_name": metadata.get("bundle_name"),
        "finalize_time": metadata.get("finalize_time"),
        "file_count": len(bundle),
    }


results = [publish_skill(leaf) for leaf in SKILL_NAMES]
listed = api.do(
    "GET",
    SKILLS_API,
    query={"parent": f"schemas/{CATALOG}.{SCHEMA}"},
).get("skills", [])
published = {
    item.get("name", "").rsplit(".", 1)[-1]
    for item in listed
    if item.get("finalize_time")
}
assert set(SKILL_NAMES).issubset(published)

result_json = json.dumps(
    {"status": "READY", "skills": results}, ensure_ascii=False, indent=2
)
print(result_json)
dbutils.notebook.exit(result_json)
