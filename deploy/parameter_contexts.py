"""Create and update NiFi parameter contexts from environment config files."""

import logging
from typing import Any

import nipyapi
from nipyapi.nifi import models as nifi_models

logger = logging.getLogger(__name__)

SENSITIVE_PARAMS = {
    "salesforce_client_id",
    "salesforce_client_secret",
    "snowflake_password",
    "postgres_password",
}


def _build_parameter_entity(name: str, value: str, sensitive: bool) -> dict:
    return {
        "parameter": {
            "name": name,
            "value": value if not sensitive else None,
            "sensitive": sensitive,
        },
        "canWrite": True,
    }


def upsert_parameter_context(context_name: str, params: dict) -> Any:
    """Create or update a NiFi parameter context. Returns the context entity."""
    existing = _find_parameter_context(context_name)

    parameter_entities = [
        _build_parameter_entity(k, v, k in SENSITIVE_PARAMS)
        for k, v in params.items()
    ]

    if existing is None:
        body = {
            "revision": {"version": 0},
            "component": {
                "name": context_name,
                "description": f"Auto-managed by openflow deploy — {context_name}",
                "parameters": parameter_entities,
            },
        }
        result = nipyapi.nifi.ParameterContextsApi().create_parameter_context(body=body)
        logger.info("Created parameter context: %s", context_name)
        return result

    # Update existing context
    current_version = existing.revision.version
    body = {
        "revision": {"version": current_version},
        "id": existing.id,
        "component": {
            "id": existing.id,
            "name": context_name,
            "description": f"Auto-managed by openflow deploy — {context_name}",
            "parameters": parameter_entities,
        },
    }
    result = nipyapi.nifi.ParameterContextsApi().update_parameter_context(
        id=existing.id, body=body
    )
    logger.info("Updated parameter context: %s (version %d → %d)", context_name, current_version, result.revision.version)
    return result


def _find_parameter_context(name: str) -> Any | None:
    try:
        contexts = nipyapi.nifi.ParameterContextsApi().get_parameter_contexts()
        if contexts and contexts.parameter_contexts:
            for ctx in contexts.parameter_contexts:
                if ctx.component.name == name:
                    return ctx
    except Exception as exc:
        logger.warning("Could not list parameter contexts: %s", exc)
    return None


def deploy_all_parameter_contexts(resolved_params: dict) -> dict:
    """Deploy all parameter contexts. Returns mapping of context_name → context_id."""
    context_ids = {}
    for ctx_name, params in resolved_params.items():
        entity = upsert_parameter_context(ctx_name, params)
        context_ids[ctx_name] = entity.id
        logger.info("Parameter context '%s' id=%s", ctx_name, entity.id)
    return context_ids
