#!/usr/bin/env python3
"""Publish Quilla Agent Skills as Unity Catalog governed securables.

Governed Skills are a Beta API and are not yet represented as a native DAB
resource. This idempotent reconciler is the versioned bridge: it creates (or
reuses) each securable, uploads the complete local bundle through the Files
API, finalizes it, and verifies the published metadata.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
from typing import Any
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen


DEFAULT_CATALOG = "example_catalog"
DEFAULT_SCHEMA = "example_ai"
SKILL_DIRECTORIES = (
    "quilla-supervisor",
    "quilla-external-intelligence",
    "quilla-source-verification",
    "quilla-impact-scenario-analysis",
)
SKILL_LEAF_PATTERN = re.compile(r"^[a-z0-9]([a-z0-9-]{0,62}[a-z0-9])?$")
FRONTMATTER_NAME_PATTERN = re.compile(r"(?m)^name:\s*([^\s]+)\s*$")


class ApiError(RuntimeError):
    def __init__(self, status: int, method: str, path: str, message: str):
        super().__init__(f"{method} {path} returned HTTP {status}: {message}")
        self.status = status


def cli_json(profile: str, *arguments: str) -> dict[str, Any]:
    env = os.environ.copy()
    env.setdefault("DATABRICKS_AUTH_STORAGE", "plaintext")
    completed = subprocess.run(
        ["databricks", *arguments, "--profile", profile, "-o", "json"],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    return json.loads(completed.stdout)


def authentication(profile: str, host_override: str | None) -> tuple[str, str]:
    token = cli_json(profile, "auth", "token").get("access_token")
    if not token:
        raise RuntimeError(f"Profile {profile!r} did not return an OAuth access token")

    host = host_override
    if not host:
        # auth env deliberately does not print a token for OAuth profiles.
        host = cli_json(profile, "auth", "env").get("env", {}).get("DATABRICKS_HOST")
    if not host:
        raise RuntimeError("Workspace host unavailable; configure the profile or pass --host")
    return host.rstrip("/"), token


class SkillRegistryClient:
    def __init__(self, host: str, token: str):
        self.host = host
        self.token = token

    def request(
        self,
        method: str,
        path: str,
        *,
        query: dict[str, str] | None = None,
        body: dict[str, Any] | None = None,
        data: bytes | None = None,
        content_type: str | None = None,
    ) -> dict[str, Any]:
        if query:
            path = f"{path}?{urlencode(query)}"
        if body is not None:
            data = json.dumps(body, ensure_ascii=False).encode("utf-8")
            content_type = "application/json"
        headers = {"Authorization": f"Bearer {self.token}"}
        if content_type:
            headers["Content-Type"] = content_type
        request = Request(
            f"{self.host}{path}",
            data=data,
            headers=headers,
            method=method,
        )
        try:
            with urlopen(request, timeout=90) as response:
                payload = response.read()
        except HTTPError as error:
            message = error.read().decode("utf-8", errors="replace")
            try:
                decoded = json.loads(message)
                message = decoded.get("message") or decoded.get("error") or message
            except json.JSONDecodeError:
                pass
            raise ApiError(error.code, method, path, str(message)) from error
        return json.loads(payload) if payload else {}

    def publish(self, catalog: str, schema: str, leaf: str, files: dict[str, bytes]) -> dict:
        skills_api = "/api/2.1/unity-catalog/skills"
        try:
            self.request(
                "POST",
                skills_api,
                query={"parent": f"schemas/{catalog}.{schema}", "skill_id": leaf},
                body={},
            )
            action = "created"
        except ApiError as error:
            if error.status != 409:
                raise
            action = "updated"

        for relative_path, content in sorted(files.items()):
            encoded_path = "/".join(quote(part, safe="") for part in relative_path.split("/"))
            self.request(
                "PUT",
                f"/api/2.0/fs/files/Skills/{quote(catalog, safe='')}/"
                f"{quote(schema, safe='')}/{quote(leaf, safe='')}/{encoded_path}",
                data=content,
                content_type="application/octet-stream",
            )

        self.request("POST", f"{skills_api}/{catalog}.{schema}.{leaf}/finalize", body={})
        metadata = self.request("GET", f"{skills_api}/{catalog}.{schema}.{leaf}")
        if not metadata.get("finalize_time"):
            raise RuntimeError(f"Skill {catalog}.{schema}.{leaf} did not finalize")
        if metadata.get("bundle_name") != leaf:
            raise RuntimeError(
                f"Skill {catalog}.{schema}.{leaf} finalized with unexpected bundle_name "
                f"{metadata.get('bundle_name')!r}"
            )
        return {
            "action": action,
            "full_name": f"{catalog}.{schema}.{leaf}",
            "id": metadata.get("id"),
            "bundle_name": metadata.get("bundle_name"),
            "finalize_time": metadata.get("finalize_time"),
            "file_count": len(files),
        }

    def list_skills(self, catalog: str, schema: str) -> list[dict[str, Any]]:
        response = self.request(
            "GET",
            "/api/2.1/unity-catalog/skills",
            query={"parent": f"schemas/{catalog}.{schema}"},
        )
        return response.get("skills", [])


def load_bundle(skill_directory: Path) -> tuple[str, dict[str, bytes]]:
    leaf = skill_directory.name
    if not SKILL_LEAF_PATTERN.fullmatch(leaf):
        raise ValueError(f"Invalid governed skill leaf name: {leaf!r}")
    skill_md = skill_directory / "SKILL.md"
    if not skill_md.is_file():
        raise ValueError(f"Missing {skill_md}")
    skill_text = skill_md.read_text(encoding="utf-8")
    match = FRONTMATTER_NAME_PATTERN.search(skill_text)
    if not match or match.group(1) != leaf:
        raise ValueError(f"SKILL.md name must exactly match governed leaf {leaf!r}")

    files = {
        path.relative_to(skill_directory).as_posix(): path.read_bytes()
        for path in skill_directory.rglob("*")
        if path.is_file() and not any(part.startswith(".") for part in path.parts)
    }
    return leaf, files


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="DEFAULT")
    parser.add_argument("--host")
    parser.add_argument("--catalog", default=DEFAULT_CATALOG)
    parser.add_argument("--schema", default=DEFAULT_SCHEMA)
    parser.add_argument(
        "--skill",
        action="append",
        choices=SKILL_DIRECTORIES,
        help="Publish only this skill; repeat to select multiple. Defaults to all configured skills.",
    )
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    selected = tuple(args.skill or SKILL_DIRECTORIES)
    bundles = [load_bundle(project_root / "agent_skills" / name) for name in selected]
    host, token = authentication(args.profile, args.host)
    client = SkillRegistryClient(host, token)

    results = [
        client.publish(args.catalog, args.schema, leaf, files) for leaf, files in bundles
    ]
    expected = {leaf for leaf, _ in bundles}
    published = {
        item.get("name", "").rsplit(".", 1)[-1]
        for item in client.list_skills(args.catalog, args.schema)
        if item.get("finalize_time")
    }
    missing = expected - published
    if missing:
        raise RuntimeError(f"Final list validation did not find: {sorted(missing)}")

    print(
        json.dumps(
            {
                "status": "READY",
                "workspace": host,
                "schema": f"{args.catalog}.{args.schema}",
                "skills": results,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
