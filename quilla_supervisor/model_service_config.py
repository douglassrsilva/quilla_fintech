"""Pure builders for reproducible Unity AI Gateway model-service routing."""

from __future__ import annotations

from typing import Iterable


DESTINATION_TYPE = "DESTINATION_TYPE_PAY_PER_TOKEN_FOUNDATION_MODEL"


def destination(name: str, model: str, *, traffic_percentage: int | None = None) -> dict:
    value = {
        "name": name,
        "destination_type": DESTINATION_TYPE,
        "pay_per_token_config": {"model": f"models/system.ai.{model}"},
    }
    if traffic_percentage is not None:
        value["traffic_percentage"] = traffic_percentage
    return value


def routing_config(primary_model: str, fallback_models: Iterable[str] = ()) -> dict:
    fallbacks = tuple(fallback_models)
    routing = {
        "destinations": [destination("primary", primary_model, traffic_percentage=100)],
    }
    if fallbacks:
        routing["fallback"] = {
            "destinations": [
                destination(f"fallback-{number}", model)
                for number, model in enumerate(fallbacks, start=1)
            ]
        }
    return routing


def service_config(primary_model: str, fallback_models: Iterable[str] = ()) -> dict:
    return {
        "routing": routing_config(primary_model, fallback_models),
        "usage_tracking": {"enabled": True},
        "rate_limits": [
            {
                "key": "RATE_LIMIT_KEY_SERVICE",
                "renewal_period": "RATE_LIMIT_RENEWAL_PERIOD_MINUTE",
                "requests": "600",
            }
        ],
    }


def update_config(existing: dict, primary_model: str, fallback_models: Iterable[str] = ()) -> dict:
    """Build a full UpdateModelService config while preserving governance controls."""
    current = existing.get("config", existing)
    config = {
        "routing": routing_config(primary_model, fallback_models),
        "rate_limits": current.get("rate_limits", []),
        "usage_tracking": current.get("usage_tracking", {"enabled": True}),
    }
    if "service_policies" in current:
        allowed_policy_fields = {"name", "policy_type", "handler", "rank", "options"}
        config["service_policies"] = [
            {key: value for key, value in policy.items() if key in allowed_policy_fields}
            for policy in current["service_policies"]
        ]
    if "inference_table" in current:
        config["inference_table"] = current["inference_table"]
    return config
