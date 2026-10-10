"""Autenticação Databricks sem credenciais persistidas no modelo."""

from __future__ import annotations

import json
import os
import subprocess
from typing import Mapping


class DatabricksAuth:
    def __init__(
        self,
        profile: str | None = None,
        *,
        authorization: str | None = None,
        workspace_host: str | None = None,
    ):
        self.profile = profile or os.getenv("DATABRICKS_CONFIG_PROFILE") or os.getenv("DATABRICKS_PROFILE")
        self.authorization = (authorization or "").strip()
        self.workspace_host = (workspace_host or "").strip()

    def _workspace_client(self):
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.credentials_provider import ModelServingUserCredentials

        if self.profile:
            return WorkspaceClient(profile=self.profile)
        # UserAuthPolicy only declares the scopes. The SDK must explicitly use
        # the model-serving OBO credential strategy to read the invoker token
        # injected into the request context. Outside serving, this strategy
        # safely falls back to Databricks unified authentication.
        return WorkspaceClient(credentials_strategy=ModelServingUserCredentials())

    def headers(self) -> dict[str, str]:
        if self.authorization:
            value = self.authorization
            if not value.lower().startswith("bearer "):
                value = f"Bearer {value}"
            return {"Authorization": value}
        try:
            authenticated = self._workspace_client().config.authenticate()
            if isinstance(authenticated, Mapping):
                return dict(authenticated)
        except Exception:
            if not self.profile:
                raise
        completed = subprocess.run(
            ["databricks", "auth", "token", self.profile, "-o", "json"],
            check=True,
            capture_output=True,
            text=True,
            env={**os.environ, "DATABRICKS_AUTH_STORAGE": "plaintext"},
        )
        token = json.loads(completed.stdout)["access_token"]
        return {"Authorization": f"Bearer {token}"}

    def host(self) -> str:
        host = self.workspace_host or os.getenv("DATABRICKS_HOST", "").strip()
        if host:
            return host.rstrip("/") if host.startswith("http") else f"https://{host.rstrip('/')}"
        try:
            return self._workspace_client().config.host.rstrip("/")
        except Exception:
            if not self.profile:
                raise
        completed = subprocess.run(
            ["databricks", "auth", "env", "--profile", self.profile, "-o", "json"],
            check=True,
            capture_output=True,
            text=True,
            env={**os.environ, "DATABRICKS_AUTH_STORAGE": "plaintext"},
        )
        return json.loads(completed.stdout)["env"]["DATABRICKS_HOST"].rstrip("/")
