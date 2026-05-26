"""Create and update NiFi parameter contexts from environment config files."""

import logging
import time
from typing import Any

import nipyapi
import requests as _requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

SENSITIVE_PARAMS = {
    "salesforce_client_secret",
    "snowflake_password",
    "postgres_password",
}


def _build_parameter_entity(name: str, value: str, sensitive: bool) -> dict:
    return {
        "parameter": {
            "name": name,
            "value": value,
            "sensitive": sensitive,
        },
        "canWrite": True,
    }


def _nifi_headers() -> dict:
    token = (nipyapi.config.nifi_config.api_key or {}).get("tokenAuth", "")
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _update_parameter_context_async(existing_id: str, current_version: int, context_name: str, parameter_entities: list) -> Any:
    """Use NiFi's async update endpoint, which handles disabling/re-enabling
    controller services and processors that reference the context."""
    nifi_base = nipyapi.config.nifi_config.host
    headers = _nifi_headers()

    body = {
        "revision": {"version": current_version},
        "id": existing_id,
        "component": {
            "id": existing_id,
            "name": context_name,
            "description": f"Auto-managed by openflow deploy — {context_name}",
            "parameters": parameter_entities,
        },
    }

    resp = _requests.post(
        f"{nifi_base}/parameter-contexts/{existing_id}/update-requests",
        json=body,
        headers=headers,
        verify=False,
        timeout=15,
    )
    resp.raise_for_status()
    req = resp.json().get("request", resp.json())
    request_id = req.get("requestId")

    deadline = time.time() + 120
    while time.time() < deadline:
        poll = _requests.get(
            f"{nifi_base}/parameter-contexts/{existing_id}/update-requests/{request_id}",
            headers=headers,
            verify=False,
            timeout=15,
        ).json()
        req = poll.get("request", poll)
        if req.get("complete", False):
            if req.get("failureReason"):
                raise RuntimeError(f"Parameter context update failed: {req['failureReason']}")
            break
        time.sleep(2)

    _requests.delete(
        f"{nifi_base}/parameter-contexts/{existing_id}/update-requests/{request_id}",
        headers=headers,
        verify=False,
        timeout=15,
    )

    return _find_parameter_context(context_name)


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

    current_version = existing.revision.version
    result = _update_parameter_context_async(existing.id, current_version, context_name, parameter_entities)
    logger.info("Updated parameter context: %s (version %d → %d)", context_name, current_version, result.revision.version)
    return result


def _find_parameter_context(name: str) -> Any | None:
    try:
        # NiFi 2.0: parameter contexts are listed via FlowApi, not ParameterContextsApi
        contexts = nipyapi.nifi.FlowApi().get_parameter_contexts()
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
