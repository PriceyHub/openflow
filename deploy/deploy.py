#!/usr/bin/env python3
"""
OpenFlow NiFi deployment script.

Usage:
    python deploy.py --env dev
    python deploy.py --env test --flow salesforce_ingestion
    python deploy.py --env prod --dry-run
"""

import json
import logging
import sys
import time
from pathlib import Path
from typing import Optional

import click
import nipyapi
import urllib3

from parameter_contexts import deploy_all_parameter_contexts
from utils import (
    configure_nipyapi,
    get_or_create_registry_bucket,
    get_root_pg_id,
    load_env_config,
    load_flow_json,
    load_parameter_context_file,
    resolve_secrets_from_aws,
    wait_for_nifi,
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("openflow.deploy")

FLOW_NAMES = {
    "salesforce_ingestion": "Salesforce Ingestion",
    "postgres_cdc": "PostgreSQL CDC",
}


def _get_nifi_password(env_config: dict) -> str:
    """Read NiFi admin password from env or raise."""
    import os
    password = os.environ.get("NIFI_ADMIN_PASSWORD")
    if not password:
        raise EnvironmentError(
            "NIFI_ADMIN_PASSWORD environment variable must be set. "
            "In CI this is injected from GitHub Secrets."
        )
    return password


def register_flow_in_registry(
    bucket: object,
    flow_name: str,
    flow_contents: dict,
    description: str,
) -> object:
    """Create or update a versioned flow in NiFi Registry."""
    existing_flows = nipyapi.versioning.list_flows_in_bucket(bucket.identifier)

    target_flow = None
    if existing_flows:
        for f in existing_flows:
            if f.name == flow_name:
                target_flow = f
                break

    if target_flow is None:
        target_flow = nipyapi.versioning.create_flow(
            bucket_id=bucket.identifier,
            flow_name=flow_name,
            flow_desc=description,
        )
        logger.info("Created registry flow: %s", flow_name)

    # Commit new version
    new_version = nipyapi.versioning.create_flow_version(
        flow=target_flow,
        flow_snapshot=nipyapi.registry.models.VersionedFlowSnapshot(
            flow_contents=flow_contents,
        ),
    )
    logger.info(
        "Committed flow '%s' version %d to registry",
        flow_name,
        new_version.snapshot_metadata.version,
    )
    return new_version


def deploy_flow_to_nifi(
    flow_snapshot_version: object,
    process_group_name: str,
    registry_client_id: str,
    bucket_id: str,
    flow_id: str,
    version: int,
    context_id: str,
    dry_run: bool,
) -> Optional[object]:
    """Deploy a versioned flow from registry onto the NiFi canvas."""
    if dry_run:
        logger.info("[DRY RUN] Would deploy '%s' (version %d)", process_group_name, version)
        return None

    root_pg_id = get_root_pg_id()

    # Check if process group already exists
    existing_pgs = nipyapi.canvas.list_all_process_groups(pg_id=root_pg_id)
    existing = next((pg for pg in existing_pgs if pg.component.name == process_group_name), None)

    if existing:
        logger.info("Updating existing process group: %s", process_group_name)
        _stop_process_group(existing.id)
        time.sleep(3)
        updated = nipyapi.versioning.update_flow_ver(process_group=existing, target_version=version)
        _update_parameter_context(existing.id, context_id)
        _start_process_group(existing.id)
        logger.info("Updated and restarted: %s", process_group_name)
        return updated

    # Create new process group from registry
    import random
    position = nipyapi.nifi.models.PositionDTO(
        x=float(random.randint(0, 800)),
        y=float(random.randint(0, 600)),
    )

    pg = nipyapi.versioning.deploy_flow_version(
        parent_id=root_pg_id,
        location=position,
        bucket_id=bucket_id,
        flow_id=flow_id,
        reg_client_id=registry_client_id,
        version=version,
    )

    _update_parameter_context(pg.id, context_id)
    time.sleep(2)
    _start_process_group(pg.id)
    logger.info("Deployed and started new process group: %s", process_group_name)
    return pg


def _update_parameter_context(pg_id: str, context_id: str) -> None:
    try:
        pg_entity = nipyapi.nifi.ProcessGroupsApi().get_process_group(pg_id)
        revision = pg_entity.revision
        body = {
            "revision": {"version": revision.version, "clientId": revision.client_id},
            "component": {
                "id": pg_id,
                "parameterContext": {"id": context_id},
            },
        }
        nipyapi.nifi.ProcessGroupsApi().update_process_group(id=pg_id, body=body)
        logger.info("Bound parameter context %s to PG %s", context_id, pg_id)
    except Exception as exc:
        logger.warning("Could not bind parameter context: %s", exc)


def _stop_process_group(pg_id: str) -> None:
    try:
        nipyapi.canvas.schedule_process_group(pg_id, scheduled=False)
        logger.info("Stopped process group %s", pg_id)
    except Exception as exc:
        logger.warning("Could not stop PG %s: %s", pg_id, exc)


def _start_process_group(pg_id: str) -> None:
    try:
        nipyapi.canvas.schedule_process_group(pg_id, scheduled=True)
        logger.info("Started process group %s", pg_id)
    except Exception as exc:
        logger.warning("Could not start PG %s: %s", pg_id, exc)


def get_or_create_registry_client(registry_url: str) -> object:
    """Ensure NiFi has a Registry client pointing at the shared registry."""
    clients = nipyapi.versioning.list_registry_clients()
    if clients:
        for c in clients:
            if c.component.uri == registry_url or registry_url in (c.component.uri or ""):
                logger.info("Registry client exists: %s", c.component.name)
                return c

    client = nipyapi.versioning.create_registry_client(
        name="OpenFlow Registry",
        uri=registry_url,
        description="Shared NiFi Registry for OpenFlow flows",
    )
    logger.info("Created registry client: %s → %s", client.component.name, registry_url)
    return client


@click.command()
@click.option("--env", required=True, type=click.Choice(["dev", "test", "prod"]), help="Target environment")
@click.option("--flow", default=None, type=click.Choice(list(FLOW_NAMES.keys())), help="Deploy specific flow only")
@click.option("--dry-run", is_flag=True, default=False, help="Print plan without making changes")
@click.option("--skip-secrets", is_flag=True, default=False, help="Skip AWS Secrets Manager lookup (use placeholder values)")
@click.option("--version", "target_version", default=None, type=int, help="Deploy specific registry version (default: latest)")
def main(env: str, flow: Optional[str], dry_run: bool, skip_secrets: bool, target_version: Optional[int]) -> None:
    logger.info("=== OpenFlow Deploy — env=%s dry_run=%s ===", env, dry_run)

    config = load_env_config(env)
    nifi_url = config["nifi_url"]
    registry_url = config["nifi_registry_url"]
    nifi_username = config["nifi_username"]

    # Wait for NiFi to be up (useful in CI pipelines after infra provisioning)
    wait_for_nifi(nifi_url, timeout=180)

    nifi_password = _get_nifi_password(config)
    configure_nipyapi(nifi_url, registry_url, nifi_username, nifi_password)

    # Resolve secrets from AWS Secrets Manager into parameter context values
    flows_to_deploy = config["flows"]
    if flow:
        flows_to_deploy = [f for f in flows_to_deploy if FLOW_NAMES.get(flow) == f["name"]]

    # Collect all parameter context files needed by selected flows
    all_param_contexts: dict = {}
    for flow_cfg in flows_to_deploy:
        ctx_data = load_parameter_context_file(flow_cfg["parameter_context_file"])
        all_param_contexts.update(ctx_data)

    if not skip_secrets:
        logger.info("Resolving secrets from AWS Secrets Manager (%s/%s)", config["aws_region"], config["aws_secrets_prefix"])
        resolved_params = resolve_secrets_from_aws(
            all_param_contexts,
            secrets_prefix=config["aws_secrets_prefix"],
            region=config["aws_region"],
        )
    else:
        logger.warning("--skip-secrets set: using placeholder parameter values")
        resolved_params = all_param_contexts

    if dry_run:
        logger.info("[DRY RUN] Parameter contexts that would be created/updated:")
        for ctx_name in resolved_params:
            logger.info("  - %s", ctx_name)

    # Create/update parameter contexts in NiFi
    context_ids = deploy_all_parameter_contexts(resolved_params) if not dry_run else {}

    # Ensure registry client exists
    registry_client = get_or_create_registry_client(registry_url) if not dry_run else None

    # Ensure bucket exists in registry
    bucket_name = flows_to_deploy[0]["registry_bucket"] if flows_to_deploy else "openflow-flows"
    bucket = get_or_create_registry_bucket(bucket_name) if not dry_run else None

    # Deploy each flow
    results = []
    for flow_cfg in flows_to_deploy:
        flow_name = flow_cfg["name"]
        pg_name = flow_cfg["process_group_name"]
        logger.info("--- Deploying flow: %s ---", flow_name)

        flow_data = load_flow_json(flow_cfg["flow_file"])
        flow_contents = flow_data.get("flowContents", flow_data)

        # Determine which parameter context this flow uses
        ctx_name = flow_contents.get("parameterContextName")
        context_id = context_ids.get(ctx_name) if not dry_run else "dry-run-ctx-id"

        if dry_run:
            logger.info("[DRY RUN] Would register '%s' in registry bucket '%s'", flow_name, bucket_name)
            logger.info("[DRY RUN] Would deploy to process group '%s' with context '%s'", pg_name, ctx_name)
            results.append({"flow": flow_name, "status": "dry-run"})
            continue

        # Register in NiFi Registry
        snapshot = register_flow_in_registry(
            bucket=bucket,
            flow_name=flow_name,
            flow_contents=flow_contents,
            description=flow_contents.get("comments", ""),
        )

        version_to_deploy = target_version or snapshot.snapshot_metadata.version

        pg = deploy_flow_to_nifi(
            flow_snapshot_version=snapshot,
            process_group_name=pg_name,
            registry_client_id=registry_client.id,
            bucket_id=bucket.identifier,
            flow_id=snapshot.snapshot_metadata.bucket_identifier,
            version=version_to_deploy,
            context_id=context_id,
            dry_run=dry_run,
        )

        results.append({"flow": flow_name, "status": "deployed", "version": version_to_deploy})

    logger.info("=== Deploy complete ===")
    for r in results:
        logger.info("  %s → %s", r["flow"], r["status"])

    if any(r["status"] == "error" for r in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
