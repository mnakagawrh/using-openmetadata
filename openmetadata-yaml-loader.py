#!/usr/bin/env python3
"""中間 YAML を読み、OpenMetadata API（:8585）へ Lineage を載せるローダ。

使い方:
  work/.venv/bin/python work/openmetadata-yaml-loader.py work/lineage/shipment-ingest.yaml
  work/.venv/bin/python work/openmetadata-yaml-loader.py work/lineage/datagrid-cache-dummy.yaml

node.type:
  topic / pipeline … そのまま
  table / cache   … Database の Table（DataGrid キャッシュの見立て）

実 Kafka / System には接続しない。既定はローカル Quick Start
(http://localhost:8585, admin / admin)。
"""

from __future__ import annotations

import base64
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

try:
    import yaml
except ImportError:
    raise SystemExit("PyYAML が必要です。例: python3 -m pip install pyyaml") from None

BASE = os.environ.get("OM_HOST", "http://localhost:8585").rstrip("/")
EMAIL = os.environ.get("OM_EMAIL", "admin@open-metadata.org")
PASSWORD = os.environ.get("OM_PASSWORD", "admin")

DEFAULT_YAML = Path(__file__).resolve().parent / "lineage" / "shipment-ingest.yaml"

NODE_TYPE_TO_OM = {
    "topic": "topic",
    "pipeline": "pipeline",
    "table": "table",
    "cache": "table",
}

UI_PATH = {
    "topic": "topic",
    "pipeline": "pipeline",
    "table": "table",
    "cache": "table",
}


def request(method: str, path: str, token: str | None = None, body: dict | None = None) -> dict:
    data = None if body is None else json.dumps(body).encode()
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(f"{BASE}{path}", data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as err:
        detail = err.read().decode()
        raise SystemExit(f"{method} {path} -> HTTP {err.code}\n{detail}") from err


def login() -> str:
    encoded = base64.b64encode(PASSWORD.encode()).decode()
    result = request("POST", "/api/v1/users/login", body={"email": EMAIL, "password": encoded})
    token = result.get("accessToken")
    if not token:
        raise SystemExit(f"login に accessToken が無い: {result}")
    return token


def put(token: str, path: str, body: dict) -> dict:
    return request("PUT", path, token=token, body=body)


def load_spec(path: Path) -> dict:
    with path.open(encoding="utf-8") as fh:
        spec = yaml.safe_load(fh)
    if not isinstance(spec, dict):
        raise SystemExit(f"YAML のルートは mapping である必要がある: {path}")
    return spec


def upsert_services(token: str, spec: dict) -> None:
    services = spec.get("services") or {}
    for svc in services.get("messaging") or []:
        created = put(
            token,
            "/api/v1/services/messagingServices",
            {
                "name": svc["name"],
                "displayName": svc.get("displayName", svc["name"]),
                "serviceType": svc["serviceType"],
                "description": svc.get("description", ""),
                "connection": {"config": svc["connection"]},
            },
        )
        print(f"messaging service: {created['fullyQualifiedName']}")
    for svc in services.get("pipelines") or []:
        created = put(
            token,
            "/api/v1/services/pipelineServices",
            {
                "name": svc["name"],
                "displayName": svc.get("displayName", svc["name"]),
                "serviceType": svc["serviceType"],
                "description": svc.get("description", ""),
                "connection": {"config": svc["connection"]},
            },
        )
        print(f"pipeline service: {created['fullyQualifiedName']}")
    for svc in services.get("databases") or []:
        created = put(
            token,
            "/api/v1/services/databaseServices",
            {
                "name": svc["name"],
                "displayName": svc.get("displayName", svc["name"]),
                "serviceType": svc.get("serviceType", "CustomDatabase"),
                "description": svc.get("description", ""),
                "connection": {"config": svc["connection"]},
            },
        )
        print(f"database service: {created['fullyQualifiedName']}")
        db_name = svc.get("database", "caches")
        schema_name = svc.get("schema", "default")
        ensure_schema(token, svc["name"], db_name, schema_name, svc.get("description", ""))


def ensure_schema(
    token: str, service: str, database: str, schema: str, description: str = ""
) -> str:
    db = put(
        token,
        "/api/v1/databases",
        {
            "name": database,
            "service": service,
            "description": description or "DataGrid caches (dummy)",
        },
    )
    sch = put(
        token,
        "/api/v1/databaseSchemas",
        {
            "name": schema,
            "database": db["fullyQualifiedName"],
            "description": description or "DataGrid cache namespace (dummy)",
        },
    )
    print(f"schema: {sch['fullyQualifiedName']}")
    return sch["fullyQualifiedName"]


def upsert_nodes(token: str, spec: dict) -> dict[str, dict]:
    created: dict[str, dict] = {}
    for node in spec.get("nodes") or []:
        node_id = node["id"]
        node_type = node["type"]
        if node_type == "topic":
            entity = put(
                token,
                "/api/v1/topics",
                {
                    "name": node["name"],
                    "displayName": node.get("displayName", node["name"]),
                    "service": node["service"],
                    "description": node.get("description", ""),
                    "partitions": node.get("partitions", 1),
                },
            )
        elif node_type == "pipeline":
            entity = put(
                token,
                "/api/v1/pipelines",
                {
                    "name": node["name"],
                    "displayName": node.get("displayName", node["name"]),
                    "service": node["service"],
                    "description": node.get("description", ""),
                    "tasks": node.get("tasks") or [],
                },
            )
        elif node_type in ("table", "cache"):
            schema_fqn = ensure_schema(
                token,
                node["service"],
                node.get("database", "caches"),
                node.get("schema", "default"),
                node.get("description", ""),
            )
            columns = node.get("columns") or [
                {"name": "key", "dataType": "VARCHAR", "dataLength": 256}
            ]
            entity = put(
                token,
                "/api/v1/tables",
                {
                    "name": node["name"],
                    "displayName": node.get("displayName", node["name"]),
                    "databaseSchema": schema_fqn,
                    "description": node.get("description", ""),
                    "tableType": "Regular",
                    "columns": columns,
                },
            )
        else:
            raise SystemExit(f"未対応の node.type: {node_type} (id={node_id})")
        created[node_id] = entity
        print(f"{node_type}: {entity['fullyQualifiedName']}")
    return created


def upsert_edges(token: str, spec: dict, nodes: dict[str, dict]) -> None:
    for edge in spec.get("edges") or []:
        src_id = edge["from"]
        dst_id = edge["to"]
        if src_id not in nodes:
            raise SystemExit(f"edges.from が nodes に無い: {src_id}")
        if dst_id not in nodes:
            raise SystemExit(f"edges.to が nodes に無い: {dst_id}")
        src = nodes[src_id]
        dst = nodes[dst_id]
        src_type = next(n["type"] for n in spec["nodes"] if n["id"] == src_id)
        dst_type = next(n["type"] for n in spec["nodes"] if n["id"] == dst_id)
        put(
            token,
            "/api/v1/lineage",
            {
                "edge": {
                    "fromEntity": {"id": src["id"], "type": NODE_TYPE_TO_OM[src_type]},
                    "toEntity": {"id": dst["id"], "type": NODE_TYPE_TO_OM[dst_type]},
                    "description": edge.get("description", ""),
                }
            },
        )
        print(f"lineage: {src['fullyQualifiedName']} -> {dst['fullyQualifiedName']}")


def main(argv: list[str]) -> int:
    yaml_path = Path(argv[1]) if len(argv) > 1 else DEFAULT_YAML
    if not yaml_path.is_file():
        raise SystemExit(f"YAML が無い: {yaml_path}")

    spec = load_spec(yaml_path)
    meta = spec.get("metadata") or {}
    print(f"OpenMetadata: {BASE}")
    print(f"YAML: {yaml_path} ({meta.get('name', yaml_path.stem)})")
    token = login()
    print("login: ok")

    upsert_services(token, spec)
    nodes = upsert_nodes(token, spec)
    upsert_edges(token, spec, nodes)

    print()
    print("UI で確認:")
    for node in spec.get("nodes") or []:
        entity = nodes[node["id"]]
        kind = node["type"]
        path = UI_PATH.get(kind, kind)
        print(f"  {kind:8s} {BASE}/{path}/{entity['fullyQualifiedName']}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
