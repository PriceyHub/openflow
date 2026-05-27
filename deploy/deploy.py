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

    # Determine next version number
    flow_id = getattr(target_flow, "identifier", None) or getattr(target_flow, "id", None)
    bucket_id = getattr(target_flow, "bucket_identifier", None) or bucket.identifier
    try:
        latest = nipyapi.versioning.get_latest_flow_version(flow=target_flow)
        next_version_num = latest.snapshot_metadata.version + 1
    except Exception:
        next_version_num = 1

    # Commit new version
    new_version = nipyapi.versioning.create_flow_version(
        flow=target_flow,
        flow_snapshot=nipyapi.registry.models.VersionedFlowSnapshot(
            flow_contents=flow_contents,
            snapshot_metadata=nipyapi.registry.models.VersionedFlowSnapshotMetadata(
                bucket_identifier=bucket_id,
                flow_identifier=flow_id,
                version=next_version_num,
                comments=description,
            ),
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

    # Remove any stale PGs from the old naming convention (e.g. "PostgreSQL CDC [dev]")
    _delete_stale_process_groups(root_pg_id, process_group_name)

    # Check if process group already exists
    existing_pgs = nipyapi.canvas.list_all_process_groups(pg_id=root_pg_id)
    existing = next((pg for pg in existing_pgs if pg.component.name == process_group_name), None)

    if existing:
        logger.info("Updating existing process group: %s", process_group_name)
        _stop_process_group(existing.id)
        time.sleep(3)
        try:
            _update_flow_version_direct(existing.id, registry_client_id, bucket_id, flow_id, version)
            _update_parameter_context(existing.id, context_id)
            _start_process_group(existing.id)
            logger.info("Updated and restarted: %s", process_group_name)
            return existing
        except Exception as exc:
            logger.warning(
                "Could not version-update existing PG '%s' (%s) — deleting and recreating",
                process_group_name, exc,
            )
            try:
                nipyapi.canvas.delete_process_group(existing, force=True)
            except Exception as del_exc:
                logger.warning("Could not delete unversioned PG: %s", del_exc)
            existing = None

    # Purge any lingering same-name PGs (e.g. if a previous delete failed)
    remaining = nipyapi.canvas.list_all_process_groups(pg_id=root_pg_id)
    for dup in [pg for pg in remaining if pg.component.name == process_group_name]:
        try:
            _stop_process_group(dup.id)
            time.sleep(2)
            nipyapi.canvas.delete_process_group(dup, force=True)
            logger.info("Purged lingering PG before recreate: %s (%s)", dup.component.name, dup.id)
        except Exception as exc:
            logger.warning("Could not purge lingering PG %s: %s", dup.id, exc)

    # Create new process group from registry.
    # Use the NiFi REST API directly — nipyapi.versioning.deploy_flow_version
    # internally calls list_flow_versions which proxies through NiFi's registry
    # client and can fail when the client URL was recently updated (stale pool).
    import random
    x = float(random.randint(0, 800))
    y = float(random.randint(0, 600))
    body = {
        "revision": {"version": 0},
        "component": {
            "name": process_group_name,
            "position": {"x": x, "y": y},
            "versionControlInformation": {
                "registryId": registry_client_id,
                "bucketId": bucket_id,
                "flowId": flow_id,
                "version": version,
            },
        },
    }
    pg = nipyapi.nifi.ProcessGroupsApi().create_process_group(id=root_pg_id, body=body)

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


def _delete_stale_process_groups(root_pg_id: str, canonical_name: str) -> None:
    """Delete PGs whose name is the canonical name followed by an env suffix like ' [dev]'."""
    import re
    pattern = re.compile(r"^" + re.escape(canonical_name) + r"\s+\[.+\]$")
    all_pgs = nipyapi.canvas.list_all_process_groups(pg_id=root_pg_id)
    stale = [pg for pg in all_pgs if pattern.match(pg.component.name)]
    for pg in stale:
        try:
            _stop_process_group(pg.id)
            time.sleep(2)
            nipyapi.canvas.delete_process_group(pg, force=True)
            logger.info("Deleted stale process group: %s", pg.component.name)
        except Exception as exc:
            logger.warning("Could not delete stale PG %s: %s", pg.component.name, exc)


def _update_flow_version_direct(pg_id: str, registry_client_id: str, bucket_id: str, flow_id: str, version: int) -> None:
    """Update a versioned process group to a new registry version in-place via NiFi 2.0 API.

    Preserves processor state (e.g. QueryDatabaseTableRecord watermarks, QuerySalesforceObject
    age-field cursors) unlike the delete+recreate fallback path.
    """
    import requests as _requests

    nifi_base = nipyapi.config.nifi_config.host
    token = (nipyapi.config.nifi_config.api_key or {}).get("tokenAuth", "")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    pg_entity = nipyapi.nifi.ProcessGroupsApi().get_process_group(pg_id)
    revision = pg_entity.revision

    body = {
        "processGroupRevision": {
            "version": revision.version,
            "clientId": revision.client_id or "",
        },
        "disconnectedNodeAcknowledged": False,
        "versionControlInformation": {
            "groupId": pg_id,
            "registryId": registry_client_id,
            "bucketId": bucket_id,
            "flowId": flow_id,
            "version": str(version),
        },
    }

    resp = _requests.put(
        f"{nifi_base}/versions/process-groups/{pg_id}",
        json=body,
        headers=headers,
        verify=False,
        timeout=30,
    )
    if not resp.ok:
        logger.warning(
            "NiFi version update failed %s — request body: %s — response: %s",
            resp.status_code, json.dumps(body), resp.text[:500],
        )
    resp.raise_for_status()
    logger.info("Updated PG %s to flow version %d in-place", pg_id, version)


def _stop_process_group(pg_id: str) -> None:
    try:
        nipyapi.canvas.schedule_process_group(pg_id, scheduled=False)
        logger.info("Stopped process group %s", pg_id)
    except Exception as exc:
        logger.warning("Could not stop PG %s: %s", pg_id, exc)


def _enable_controller_services(pg_id: str) -> None:
    """Enable all controller services in a process group before starting processors."""
    import requests as _requests

    try:
        flow_api = nipyapi.nifi.FlowApi()
        services = flow_api.get_controller_services_from_group(pg_id).controller_services or []
        disabled = [s for s in services if s.component.state != "ENABLED"]
        if not disabled:
            return

        nifi_base = nipyapi.config.nifi_config.host
        token = (nipyapi.config.nifi_config.api_key or {}).get("tokenAuth", "")
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        for svc in disabled:
            try:
                body = {
                    "revision": {"version": svc.revision.version},
                    "state": "ENABLED",
                    "disconnectedNodeAcknowledged": False,
                }
                resp = _requests.put(
                    f"{nifi_base}/controller-services/{svc.id}/run-status",
                    json=body,
                    headers=headers,
                    verify=False,
                    timeout=15,
                )
                resp.raise_for_status()
            except Exception as exc:
                logger.warning("Could not enable controller service %s: %s", svc.component.name, exc)

        # Poll until all are enabled (up to 300s — Maven jar download on cold start)
        deadline = time.time() + 300
        while time.time() < deadline:
            states = {
                s.component.name: s.component.state
                for s in (flow_api.get_controller_services_from_group(pg_id).controller_services or [])
            }
            if all(v == "ENABLED" for v in states.values()):
                break
            time.sleep(3)
        logger.info("Controller services enabled for PG %s: %s", pg_id, list(states.keys()))
    except Exception as exc:
        logger.warning("Could not enable controller services for PG %s: %s", pg_id, exc)


def _fix_convert_record_cs_refs(pg_id: str) -> None:
    """Resolve ConvertRecord CS references after deploy.

    NiFi 2.0 does not reliably map versionedComponentId → instanceIdentifier for
    Record Reader/Writer properties when deploying hand-crafted flow definitions.
    This looks up each CS by name and patches ConvertRecord processors that still
    reference a non-existent identifier.
    """
    import requests as _requests

    try:
        nifi_base = nipyapi.config.nifi_config.host
        headers = {"Authorization": f"Bearer {(nipyapi.config.nifi_config.api_key or {}).get('tokenAuth', '')}",
                   "Content-Type": "application/json"}

        # Build name → instance-id map for all CSes in the PG
        svc_resp = nipyapi.nifi.FlowApi().get_controller_services_from_group(pg_id)
        cs_by_name = {
            s.component.name: s.id
            for s in (svc_resp.controller_services or [])
        }

        procs = _requests.get(f"{nifi_base}/process-groups/{pg_id}/processors",
                              headers=headers, verify=False, timeout=15).json()

        for proc in procs.get("processors", []):
            if "ConvertRecord" not in proc["component"]["name"]:
                continue
            props = proc["component"]["config"]["properties"]
            reader_val = props.get("Record Reader", "")
            writer_val = props.get("Record Writer", "")

            # Check if either property is an unresolved versionedComponentId
            # (i.e., it doesn't match any known CS instance id)
            known_ids = set(cs_by_name.values())
            needs_fix = (reader_val and reader_val not in known_ids) or \
                        (writer_val and writer_val not in known_ids)
            if not needs_fix:
                continue

            # Match by CS name substring: JsonTreeReader → Record Reader, JsonRecordSetWriter → Record Writer
            new_reader = next((cid for cname, cid in cs_by_name.items() if "TreeReader" in cname or "JsonTreeReader" in cname), None)
            new_writer = next((cid for cname, cid in cs_by_name.items() if "RecordSetWriter" in cname or "JsonRecordSetWriter" in cname), None)
            if not new_reader or not new_writer:
                logger.warning("Could not find JsonTreeReader/JsonRecordSetWriter CS in PG %s to fix ConvertRecord", pg_id)
                continue

            entity = _requests.get(f"{nifi_base}/processors/{proc['id']}",
                                   headers=headers, verify=False, timeout=15).json()
            entity["component"]["config"]["properties"]["Record Reader"] = new_reader
            entity["component"]["config"]["properties"]["Record Writer"] = new_writer
            # Null out any stale lowercase-hyphen variants
            entity["component"]["config"]["properties"]["record-reader"] = None
            entity["component"]["config"]["properties"]["record-writer"] = None
            resp = _requests.put(f"{nifi_base}/processors/{proc['id']}",
                                 headers=headers, verify=False, json=entity, timeout=15)
            if resp.ok:
                logger.info("Fixed ConvertRecord '%s' CS refs in PG %s", proc["component"]["name"], pg_id)
            else:
                logger.warning("Could not fix ConvertRecord '%s': %s", proc["component"]["name"], resp.text[:200])

        # Also auto-terminate any unconnected 'set state fail' relationships
        for proc in procs.get("processors", []):
            rels = proc["component"].get("relationships", [])
            if not any(r["name"] == "set state fail" for r in rels):
                continue
            auto_term = proc["component"]["config"].get("autoTerminatedRelationships", [])
            if "set state fail" in auto_term:
                continue
            entity = _requests.get(f"{nifi_base}/processors/{proc['id']}",
                                   headers=headers, verify=False, timeout=15).json()
            entity["component"]["config"].setdefault("autoTerminatedRelationships", [])
            if "set state fail" not in entity["component"]["config"]["autoTerminatedRelationships"]:
                entity["component"]["config"]["autoTerminatedRelationships"].append("set state fail")
                resp = _requests.put(f"{nifi_base}/processors/{proc['id']}",
                                     headers=headers, verify=False, json=entity, timeout=15)
                if resp.ok:
                    logger.info("Auto-terminated 'set state fail' on '%s'", proc["component"]["name"])

    except Exception as exc:
        logger.warning("Could not fix ConvertRecord CS refs in PG %s: %s", pg_id, exc)


def _fix_aws_credentials_provider(pg_id: str) -> None:
    """Ensure AWSCredentialsProviderControllerService uses default (IAM role) credentials.

    NiFi can deploy this CS with anonymous-credentials=true from a stale registry snapshot.
    We correct it before enabling so S3 writes succeed via the EC2 instance profile.
    """
    import requests as _requests

    try:
        flow_api = nipyapi.nifi.FlowApi()
        services = flow_api.get_controller_services_from_group(pg_id).controller_services or []
        aws_svcs = [s for s in services if "AWSCredentials" in (s.component.name or "")]
        if not aws_svcs:
            return

        nifi_base = nipyapi.config.nifi_config.host
        token = (nipyapi.config.nifi_config.api_key or {}).get("tokenAuth", "")
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        for svc in aws_svcs:
            props = getattr(getattr(svc, "component", None), "properties", {}) or {}
            if props.get("anonymous-credentials") != "true":
                continue
            logger.info("Fixing AWSCredentialsProviderControllerService %s: resetting to default-credentials", svc.id)
            # GET the full entity so the PUT body has all required fields (name, type, bundle)
            get_resp = _requests.get(
                f"{nifi_base}/controller-services/{svc.id}",
                headers=headers,
                verify=False,
                timeout=15,
            )
            get_resp.raise_for_status()
            entity = get_resp.json()
            entity["revision"]["version"] = entity["revision"]["version"]
            entity["component"]["properties"]["default-credentials"] = "true"
            entity["component"]["properties"]["anonymous-credentials"] = "false"
            resp = _requests.put(
                f"{nifi_base}/controller-services/{svc.id}",
                json=entity,
                headers=headers,
                verify=False,
                timeout=15,
            )
            resp.raise_for_status()
            logger.info("AWSCredentialsProviderControllerService %s corrected to default-credentials", svc.id)
    except Exception as exc:
        logger.warning("Could not fix AWS credentials provider CS in PG %s: %s", pg_id, exc)


def _start_process_group(pg_id: str) -> None:
    try:
        _fix_convert_record_cs_refs(pg_id)
        _fix_aws_credentials_provider(pg_id)
        _enable_controller_services(pg_id)
        nipyapi.canvas.schedule_process_group(pg_id, scheduled=True)
        logger.info("Started process group %s", pg_id)
    except Exception as exc:
        logger.warning("Could not start PG %s: %s", pg_id, exc)


def get_or_create_registry_client(registry_url: str, registry_internal_url: str) -> object:
    """Ensure NiFi has a Registry client pointing at the shared registry.

    registry_url: URL this script uses (may be an SSM tunnel).
    registry_internal_url: URL NiFi itself must use to reach the registry over the VPC.
    """

    def _find_by_name(client_list: list) -> object:
        for c in client_list:
            name = getattr(getattr(c, "component", None), "name", "") or ""
            if name == "OpenFlow Registry":
                return c
        return None

    def _get_client_uri(c: object) -> str:
        comp = getattr(c, "component", None)
        # NiFi 2.0 stores URL in component.properties.url; component.uri is the API self-link
        props = getattr(comp, "properties", {}) or {}
        prop_url = props.get("url", "") or props.get("uri", "") or ""
        # NiFi 1.x stored it directly on component.uri
        legacy_uri = getattr(comp, "uri", "") or ""
        # Prefer properties.url; fall back to legacy uri only if it looks like a registry URL
        if prop_url:
            return prop_url
        if legacy_uri and "nifi-api" not in legacy_uri:
            return legacy_uri
        return ""

    def _update_client_uri(c: object) -> object:
        """PUT updated URL back to NiFi using the NiFi 2.0 properties.url format."""
        import requests as _requests
        try:
            revision = getattr(c, "revision", None)
            rev_version = getattr(revision, "version", 0)
            client_id = getattr(c, "id", None)
            # NiFi 2.0 stores the registry URL in component.properties.url, not component.uri
            body = {
                "revision": {"version": rev_version},
                "id": client_id,
                "component": {
                    "id": client_id,
                    "name": "OpenFlow Registry",
                    "description": "Shared NiFi Registry for OpenFlow flows",
                    "type": "org.apache.nifi.registry.flow.NifiRegistryFlowRegistryClient",
                    "bundle": {
                        "group": "org.apache.nifi",
                        "artifact": "nifi-flow-registry-client-nar",
                        "version": "2.0.0",
                    },
                    "properties": {"url": registry_internal_url},
                },
            }
            nifi_base = nipyapi.config.nifi_config.host  # ends with /nifi-api
            token = (nipyapi.config.nifi_config.api_key or {}).get("tokenAuth", "")
            resp = _requests.put(
                f"{nifi_base}/controller/registry-clients/{client_id}",
                json=body,
                headers={"Authorization": f"Bearer {token}"},
                verify=False,
                timeout=15,
            )
            resp.raise_for_status()
            logger.info("Updated registry client URL → %s", registry_internal_url)
        except Exception as exc:
            logger.warning("Could not update registry client URI: %s", exc)
        return c

    clients = nipyapi.versioning.list_registry_clients()
    client_list = getattr(clients, "registries", None) or []
    existing = _find_by_name(client_list)

    if existing:
        current_uri = _get_client_uri(existing)
        if registry_internal_url not in current_uri:
            logger.info("Registry client URI mismatch (%s) — updating to %s", current_uri, registry_internal_url)
            existing = _update_client_uri(existing)
        else:
            logger.info("Registry client exists with correct URI: %s", current_uri)
        return existing

    try:
        client = nipyapi.versioning.create_registry_client(
            name="OpenFlow Registry",
            uri=registry_internal_url,
            description="Shared NiFi Registry for OpenFlow flows",
        )
        logger.info("Created registry client: %s → %s", client.component.name, registry_internal_url)
        return client
    except (ValueError, Exception) as exc:
        if "already exists" not in str(exc):
            raise
        clients = nipyapi.versioning.list_registry_clients()
        client_list = getattr(clients, "registries", None) or []
        existing = _find_by_name(client_list)
        if existing:
            return existing
        raise RuntimeError("Registry client 'OpenFlow Registry' reported as duplicate but not found in list") from exc


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
    wait_for_nifi(nifi_url, timeout=600)

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
            profile=config.get("aws_profile"),
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

    # Ensure registry client exists — NiFi itself connects via the internal VPC URL,
    # not the SSM tunnel URL used by this script.
    registry_internal_url = config.get("nifi_registry_internal_url", registry_url)
    registry_client = get_or_create_registry_client(registry_url, registry_internal_url) if not dry_run else None

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
            flow_id=snapshot.snapshot_metadata.flow_identifier,
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
