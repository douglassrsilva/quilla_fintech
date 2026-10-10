#!/usr/bin/env python3
"""Provision the governed tags that back Quilla Discovery Hub domains.

Discovery domain metadata and publication are currently managed in the UI. This
script provisions the public-API portion idempotently so the same governed tags
can be selected when recreating the domain hierarchy in another workspace.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from dataclasses import dataclass


@dataclass(frozen=True)
class DomainTag:
    key: str
    description: str


DOMAIN_TAGS = (
    DomainTag(
        "[Douglas] Quilla",
        "Dominio raíz de la demo Quilla: productos gobernados de datos e IA para una fintech LATAM sintética.",
    ),
    DomainTag(
        "[Douglas] Quilla/Cliente 360 y Crecimiento",
        "Cliente 360, segmentación, churn, LTV, campañas y recomendaciones Next Best Action de Quilla.",
    ),
    DomainTag(
        "[Douglas] Quilla/Pagos y Tiempo Real",
        "Pagos, autorizaciones, rechazos, señales de fraude y eventos en tiempo real mediante Zerobus.",
    ),
    DomainTag(
        "[Douglas] Quilla/Crédito y Precios",
        "Crédito responsable, capacidad de pago, riesgo, precios y escenarios macroeconómicos.",
    ),
    DomainTag(
        "[Douglas] Quilla/Riesgo, Fraude y Cumplimiento",
        "Fraude, AML, KYC, siniestros, casos, controles regulatorios y confianza operacional.",
    ),
    DomainTag(
        "[Douglas] Quilla/Inversiones y Patrimonio",
        "Portafolios, idoneidad, interacción, estrés macro e inteligencia para inversiones.",
    ),
    DomainTag(
        "[Douglas] Quilla/Inteligencia Externa e IA",
        "Inteligencia externa, regulación, búsqueda híbrida, agentes, skills gobernadas y evaluación GenAI.",
    ),
    DomainTag(
        "[Douglas] Quilla/Plataforma, Gobierno y Calidad",
        "Arquitectura medallion y LTAP, calidad, clasificación, linaje, MLflow y serving gobernados.",
    ),
)


def run_cli(profile: str, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["databricks", *args, "--profile", profile, "-o", "json"],
        capture_output=True,
        text=True,
        timeout=60,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="DEFAULT")
    args = parser.parse_args()

    outcomes = []
    for domain in DOMAIN_TAGS:
        current = run_cli(args.profile, "tag-policies", "get-tag-policy", domain.key)
        if current.returncode == 0:
            payload = json.loads(current.stdout)
            state = "EXISTING"
        else:
            created = run_cli(
                args.profile,
                "tag-policies",
                "create-tag-policy",
                domain.key,
                "--description",
                domain.description,
            )
            if created.returncode != 0:
                raise RuntimeError(f"Failed to create {domain.key}: {created.stderr.strip()}")
            payload = json.loads(created.stdout)
            state = "CREATED"
        outcomes.append({"tag_key": domain.key, "state": state, "id": payload.get("id")})

    print(json.dumps({"status": "READY_FOR_DISCOVERY_UI", "tags": outcomes}, ensure_ascii=False))


if __name__ == "__main__":
    main()
