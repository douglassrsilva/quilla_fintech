"""Emite eventos financeiros sintéticos PT/ES usando Zerobus Ingest.

Plano aprovado: produtor financeiro bounded, seed determinística, 12 mercados,
cenários normal/fraude/cross-border/qualidade/misto e destino governado no UC.
"""

from __future__ import annotations

import argparse
import random
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from zerobus.sdk.shared import TableProperties
from zerobus.sdk.sync import ZerobusSdk


COUNTRIES = (
    ("BR", "pt", "BRL", "America/Sao_Paulo", 31.0),
    ("MX", "es", "MXN", "America/Mexico_City", 21.0),
    ("CO", "es", "COP", "America/Bogota", 12.0),
    ("AR", "es", "ARS", "America/Argentina/Buenos_Aires", 8.0),
    ("CL", "es", "CLP", "America/Santiago", 7.0),
    ("PE", "es", "PEN", "America/Lima", 7.0),
    ("EC", "es", "USD", "America/Guayaquil", 3.5),
    ("GT", "es", "GTQ", "America/Guatemala", 3.0),
    ("DO", "es", "DOP", "America/Santo_Domingo", 2.5),
    ("CR", "es", "CRC", "America/Costa_Rica", 2.0),
    ("PA", "es", "PAB", "America/Panama", 1.5),
    ("UY", "es", "UYU", "America/Montevideo", 1.5),
)

CURRENCY_PER_USD = {
    "BRL": 5.40,
    "MXN": 19.20,
    "COP": 4100.0,
    "ARS": 1050.0,
    "CLP": 940.0,
    "PEN": 3.75,
    "USD": 1.0,
    "GTQ": 7.70,
    "DOP": 60.0,
    "CRC": 515.0,
    "PAB": 1.0,
    "UYU": 42.0,
}

EVENT_TYPES = (
    "merchant_payment",
    "p2p_transfer",
    "card_authorization",
    "cash_withdrawal",
    "cash_deposit",
    "bill_payment",
    "cross_border_transfer",
    "fx_conversion",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--server-endpoint", required=True)
    parser.add_argument("--workspace-url", required=True)
    parser.add_argument("--table-name", required=True)
    parser.add_argument("--secret-scope", required=True)
    parser.add_argument("--event-count", type=int, default=1000)
    parser.add_argument(
        "--scenario",
        choices=("normal", "fraud_burst", "cross_border", "dq_violations", "mixed"),
        default="mixed",
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--simulation-id", required=True)
    parser.add_argument("--batch-size", type=int, default=1000)
    args = parser.parse_args()

    if not 1 <= args.event_count <= 100_000:
        parser.error("event-count deve estar entre 1 e 100000")
    if not 1 <= args.batch_size <= 10_000:
        parser.error("batch-size deve estar entre 1 e 10000")
    if not args.server_endpoint.startswith("https://"):
        parser.error("server-endpoint deve usar HTTPS")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", args.secret_scope):
        parser.error("secret-scope contém caracteres não permitidos")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", args.simulation_id):
        parser.error("simulation-id contém caracteres não permitidos")
    return args


def token(prefix: str, country_index: int, capacity: int, rng: random.Random) -> str:
    numeric = country_index + 12 * rng.randrange(capacity) + 1
    return f"{prefix}-{numeric:012d}"


def epoch_microseconds(value: datetime) -> int:
    """Encode Delta TIMESTAMP values as the Zerobus-required int64 epoch micros."""
    utc_value = value.astimezone(timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    return int((utc_value - epoch).total_seconds() * 1_000_000)


def choose_profile(scenario: str, sequence: int, rng: random.Random) -> str:
    if scenario != "mixed":
        return scenario
    bucket = rng.random()
    if bucket < 0.75:
        return "normal"
    if bucket < 0.87:
        return "fraud_burst"
    if bucket < 0.96:
        return "cross_border"
    return "dq_violations"


def build_record(
    args: argparse.Namespace,
    sequence: int,
    base_time: datetime,
    rng: random.Random,
) -> dict[str, Any]:
    country_codes = [item[0] for item in COUNTRIES]
    country_weights = [item[4] for item in COUNTRIES]
    country_code = rng.choices(country_codes, weights=country_weights, k=1)[0]
    country_index = country_codes.index(country_code)
    _, language, currency, event_timezone, _ = COUNTRIES[country_index]
    profile = choose_profile(args.scenario, sequence, rng)

    event_type = rng.choices(
        EVENT_TYPES,
        weights=(30, 20, 12, 8, 8, 10, 8, 4),
        k=1,
    )[0]
    if profile == "cross_border":
        event_type = rng.choice(("cross_border_transfer", "fx_conversion"))

    is_cross_border = profile == "cross_border" or event_type in {
        "cross_border_transfer",
        "fx_conversion",
    }
    if profile == "fraud_burst":
        is_cross_border = rng.random() < 0.65

    amount_usd_base = rng.lognormvariate(3.6, 1.0)
    if profile == "fraud_burst":
        amount_usd_base *= rng.uniform(25.0, 90.0)
    amount_local = round(max(0.50, amount_usd_base * CURRENCY_PER_USD[currency]), 2)
    amount_usd = round(amount_local / CURRENCY_PER_USD[currency], 2)

    risk_score = round(rng.uniform(0.03, 0.78), 4)
    fraud_label = False
    if profile == "fraud_burst":
        risk_score = round(rng.uniform(0.91, 0.999), 4)
        fraud_label = rng.random() < 0.70
    elif is_cross_border:
        risk_score = round(min(0.95, risk_score + 0.08), 4)

    event_status = rng.choices(
        ("completed", "pending", "declined", "reversed"),
        weights=(91, 5, 3, 1),
        k=1,
    )[0]
    event_time = base_time + timedelta(milliseconds=sequence * 25)
    producer_time = datetime.now(timezone.utc)

    currency_code = currency
    payload_variant = "CANONICAL"
    schema_version = "1.0"
    if profile == "dq_violations":
        defect = sequence % 4
        if defect == 0:
            amount_local = None
            amount_usd = None
        elif defect == 1:
            currency_code = "BRL" if currency == "USD" else "USD"
        elif defect == 2:
            event_status = "STATUS_INVALIDO"
            payload_variant = "INVALID_STATUS"
        else:
            risk_score = 1.5
            schema_version = "1.1-invalid"

    merchant_event = event_type in {"merchant_payment", "card_authorization"}
    counterparty = None
    if is_cross_border:
        alternatives = [code for code in country_codes if code != country_code]
        counterparty = rng.choice(alternatives)

    event_id = f"QRT-{args.simulation_id}-{sequence:08d}"
    description = (
        f"Evento financeiro em tempo real: {event_type.replace('_', ' ')}"
        if language == "pt"
        else f"Evento financiero en tiempo real: {event_type.replace('_', ' ')}"
    )

    return {
        "source_record_id": f"QRTSRC-{args.simulation_id}-{sequence:08d}",
        "event_id": event_id,
        "country_code": country_code,
        "party_id": token("QPTY", country_index, 12_000, rng),
        "account_id": token("QACC", country_index, 20_000, rng),
        "card_id": token("QCARD", country_index, 13_333, rng) if merchant_event else None,
        "merchant_id": token("QMRC", country_index, 1_666, rng) if merchant_event else None,
        "device_id": token("QDEV", country_index, 12_500, rng),
        "product_id": rng.randint(1, 25),
        "event_type": event_type,
        "event_status": event_status,
        "channel": rng.choice(("mobile", "web", "pos", "api", "atm")),
        "direction": "credit" if event_type in {"cash_deposit"} else "debit",
        "amount_local": amount_local,
        "currency_code": currency_code,
        "amount_usd": amount_usd,
        "event_ts_utc": epoch_microseconds(event_time),
        "event_timezone": event_timezone,
        "authorized_ts": epoch_microseconds(event_time + timedelta(seconds=2)),
        "settled_ts": (
            epoch_microseconds(event_time + timedelta(days=1))
            if event_status == "completed"
            else None
        ),
        "ingested_at": epoch_microseconds(producer_time),
        "is_cross_border": is_cross_border,
        "counterparty_country_code": counterparty,
        "risk_score": risk_score,
        "fraud_label": fraud_label,
        "description": description,
        "source_system": f"QUILLA_ZEROBUS_{country_code}",
        "schema_version": schema_version,
        "payload_variant": payload_variant,
        "event_month": event_time.strftime("%Y-%m"),
        "synthetic_flag": True,
        "_producer_run_id": args.simulation_id,
        "_producer_scenario": profile,
        "_producer_sequence": sequence,
        "_producer_language": language,
        "_producer_ingested_at": epoch_microseconds(producer_time),
    }


def main() -> None:
    args = parse_args()
    rng = random.Random(f"{args.seed}:{args.simulation_id}")
    base_time = datetime.now(timezone.utc)

    # Secret references are deliberately resolved inside the Databricks runtime.
    # Passing {{secrets/...}} as a Python task argument can leave the template
    # uninterpreted, and it also makes credentials part of the task interface.
    runtime_dbutils = globals().get("dbutils")
    if runtime_dbutils is None:
        raise RuntimeError("dbutils não está disponível para resolver o Secret Scope")
    client_id = runtime_dbutils.secrets.get(args.secret_scope, "client_id")
    client_secret = runtime_dbutils.secrets.get(args.secret_scope, "client_secret")

    sdk = ZerobusSdk(
        args.server_endpoint,
        args.workspace_url,
        application_name="quilla-realtime-demo/1.0",
    )
    stream = sdk.create_stream(
        client_id,
        client_secret,
        TableProperties(args.table_name),
    )

    emitted = 0
    try:
        while emitted < args.event_count:
            stop = min(emitted + args.batch_size, args.event_count)
            records = [
                build_record(args, sequence, base_time, rng)
                for sequence in range(emitted, stop)
            ]
            stream.ingest_records_offset(records)
            emitted = stop
        stream.flush()
    finally:
        stream.close()

    print(
        {
            "status": "ACKNOWLEDGED",
            "transport": "zerobus",
            "table": args.table_name,
            "simulation_id": args.simulation_id,
            "scenario": args.scenario,
            "events_emitted": emitted,
            "languages": ["pt", "es"],
            "countries": len(COUNTRIES),
        }
    )


if __name__ == "__main__":
    main()
