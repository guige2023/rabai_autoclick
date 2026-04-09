"""Data mesh action module for RabAI AutoClick.

Provides data mesh architecture operations:
- DataMeshDomainAction: Define and manage data mesh domains
- DataMeshProductAction: Manage data products
- DataMeshFederationAction: Federate data across domains
- DataMeshOwnershipAction: Assign and manage data ownership
- DataMeshDiscoveryAction: Discover data products across the mesh
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Set

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DomainType(Enum):
    """Data mesh domain types."""
    SOURCE_DOMAIN = auto()
    PROCESSING_DOMAIN = auto()
    AGGREGATION_DOMAIN = auto()
    CONSUMPTION_DOMAIN = auto()


class DataProductState(Enum):
    """Data product states."""
    DRAFT = auto()
    DEVELOPMENT = auto()
    STAGING = auto()
    PRODUCTION = auto()
    DEPRECATED = auto()
    RETIRED = auto()


class DataMeshDomainAction(BaseAction):
    """Define and manage data mesh domains."""
    action_type = "data_mesh_domain"
    display_name = "数据网格域"
    description = "定义和管理数据网格域"

    def __init__(self) -> None:
        super().__init__()
        self._domains: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "")
            if action == "create":
                return self._create_domain(params)
            elif action == "update":
                return self._update_domain(params)
            elif action == "list":
                return self._list_domains()
            elif action == "add_product":
                return self._add_product_to_domain(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Data mesh domain failed: {e}")

    def _create_domain(self, params: Dict[str, Any]) -> ActionResult:
        name = params.get("name", "")
        domain_type = params.get("domain_type", "SOURCE_DOMAIN")
        description = params.get("description", "")
        owner = params.get("owner", "")
        if not name:
            return ActionResult(success=False, message="name is required")
        try:
            dtype = DomainType[domain_type.upper()]
        except KeyError:
            dtype = DomainType.SOURCE_DOMAIN

        domain_id = str(uuid.uuid4())
        self._domains[name] = {
            "id": domain_id,
            "name": name,
            "domain_type": dtype.name,
            "description": description,
            "owner": owner,
            "products": [],
            "created_at": datetime.now(timezone.utc).isoformat(),
            "version": "1.0.0",
        }
        return ActionResult(success=True, message=f"Domain '{name}' created as {dtype.name}", data=self._domains[name])

    def _update_domain(self, params: Dict[str, Any]) -> ActionResult:
        name = params.get("name", "")
        updates = params.get("updates", {})
        if name not in self._domains:
            return ActionResult(success=False, message=f"Domain not found: {name}")
        self._domains[name].update(updates)
        return ActionResult(success=True, message=f"Domain '{name}' updated")

    def _list_domains(self) -> ActionResult:
        return ActionResult(success=True, message=f"{len(self._domains)} domains", data={"domains": list(self._domains.values())})

    def _add_product_to_domain(self, params: Dict[str, Any]) -> ActionResult:
        domain_name = params.get("domain_name", "")
        product_id = params.get("product_id", "")
        if domain_name not in self._domains:
            return ActionResult(success=False, message=f"Domain not found: {domain_name}")
        self._domains[domain_name]["products"].append(product_id)
        return ActionResult(success=True, message=f"Product added to domain '{domain_name}'")


class DataMeshProductAction(BaseAction):
    """Manage data products in the mesh."""
    action_type = "data_mesh_product"
    display_name = "数据产品"
    description = "管理数据网格中的数据产品"

    def __init__(self) -> None:
        super().__init__()
        self._products: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "")
            if action == "create":
                return self._create_product(params)
            elif action == "publish":
                return self._publish_product(params)
            elif action == "deprecate":
                return self._deprecate_product(params)
            elif action == "list":
                return self._list_products(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Data product failed: {e}")

    def _create_product(self, params: Dict[str, Any]) -> ActionResult:
        name = params.get("name", "")
        domain = params.get("domain", "")
        schema = params.get("schema", {})
        description = params.get("description", "")
        if not name:
            return ActionResult(success=False, message="name is required")

        product_id = str(uuid.uuid4())
        self._products[product_id] = {
            "id": product_id,
            "name": name,
            "domain": domain,
            "schema": schema,
            "description": description,
            "state": DataProductState.DRAFT.name,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "version": "1.0.0",
            "consumers": [],
        }
        return ActionResult(success=True, message=f"Data product '{name}' created", data=self._products[product_id])

    def _publish_product(self, params: Dict[str, Any]) -> ActionResult:
        product_id = params.get("product_id", "")
        if product_id not in self._products:
            return ActionResult(success=False, message="Product not found")
        self._products[product_id]["state"] = DataProductState.PRODUCTION.name
        self._products[product_id]["published_at"] = datetime.now(timezone.utc).isoformat()
        return ActionResult(success=True, message=f"Product '{product_id[:8]}' published to production")

    def _deprecate_product(self, params: Dict[str, Any]) -> ActionResult:
        product_id = params.get("product_id", "")
        migration_guide = params.get("migration_guide", "")
        if product_id not in self._products:
            return ActionResult(success=False, message="Product not found")
        self._products[product_id]["state"] = DataProductState.DEPRECATED.name
        self._products[product_id]["migration_guide"] = migration_guide
        return ActionResult(success=True, message=f"Product '{product_id[:8]}' deprecated")

    def _list_products(self, params: Dict[str, Any]) -> ActionResult:
        state = params.get("state", "")
        domain = params.get("domain", "")
        products = list(self._products.values())
        if state:
            products = [p for p in products if p["state"] == state.upper()]
        if domain:
            products = [p for p in products if p["domain"] == domain]
        return ActionResult(success=True, message=f"{len(products)} products", data={"products": products})


class DataMeshFederationAction(BaseAction):
    """Federate data across domains."""
    action_type = "data_mesh_federation"
    display_name = "数据联合"
    description = "跨域联邦数据"

    def __init__(self) -> None:
        super().__init__()
        self._federations: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "")
            if action == "create":
                return self._create_federation(params)
            elif action == "query":
                return self._query_federated_data(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Data federation failed: {e}")

    def _create_federation(self, params: Dict[str, Any]) -> ActionResult:
        name = params.get("name", "")
        source_products = params.get("source_products", [])
        join_keys = params.get("join_keys", [])
        if not name:
            return ActionResult(success=False, message="name is required")

        fed_id = str(uuid.uuid4())
        self._federations[fed_id] = {
            "id": fed_id,
            "name": name,
            "source_products": source_products,
            "join_keys": join_keys,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        return ActionResult(success=True, message=f"Federation '{name}' created with {len(source_products)} sources", data=self._federations[fed_id])

    def _query_federated_data(self, params: Dict[str, Any]) -> ActionResult:
        federation_id = params.get("federation_id", "")
        filters = params.get("filters", {})
        if federation_id not in self._federations:
            return ActionResult(success=False, message="Federation not found")
        fed = self._federations[federation_id]
        return ActionResult(
            success=True,
            message=f"Query executed on federation '{fed['name']}'",
            data={"federation_id": federation_id, "sources_queried": len(fed["source_products"]), "filters": filters},
        )


class DataMeshOwnershipAction(BaseAction):
    """Assign and manage data ownership."""
    action_type = "data_mesh_ownership"
    display_name = "数据所有权"
    description = "分配和管理数据所有权"

    def __init__(self) -> None:
        super().__init__()
        self._ownership_records: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "")
            if action == "assign":
                return self._assign_ownership(params)
            elif action == "transfer":
                return self._transfer_ownership(params)
            elif action == "list":
                return self._list_ownership()
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Data ownership failed: {e}")

    def _assign_ownership(self, params: Dict[str, Any]) -> ActionResult:
        asset_id = params.get("asset_id", "")
        owner_id = params.get("owner_id", "")
        owner_name = params.get("owner_name", "")
        ownership_type = params.get("ownership_type", "FULL")
        if not asset_id or not owner_id:
            return ActionResult(success=False, message="asset_id and owner_id are required")
        self._ownership_records[asset_id] = {
            "asset_id": asset_id,
            "owner_id": owner_id,
            "owner_name": owner_name,
            "ownership_type": ownership_type,
            "assigned_at": datetime.now(timezone.utc).isoformat(),
        }
        return ActionResult(success=True, message=f"Ownership assigned: {owner_name} owns {asset_id[:8]}")

    def _transfer_ownership(self, params: Dict[str, Any]) -> ActionResult:
        asset_id = params.get("asset_id", "")
        new_owner_id = params.get("new_owner_id", "")
        new_owner_name = params.get("new_owner_name", "")
        if asset_id not in self._ownership_records:
            return ActionResult(success=False, message="Asset not found")
        record = self._ownership_records[asset_id]
        record.update({
            "previous_owner_id": record["owner_id"],
            "owner_id": new_owner_id,
            "owner_name": new_owner_name,
            "transferred_at": datetime.now(timezone.utc).isoformat(),
        })
        return ActionResult(success=True, message=f"Ownership transferred to {new_owner_name}")

    def _list_ownership(self) -> ActionResult:
        return ActionResult(success=True, message=f"{len(self._ownership_records)} ownership records", data={"records": self._ownership_records})


class DataMeshDiscoveryAction(BaseAction):
    """Discover data products across the mesh."""
    action_type = "data_mesh_discovery"
    display_name = "数据发现"
    description = "跨网格发现数据产品"

    def __init__(self) -> None:
        super().__init__()
        self._registry: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "")
            if action == "register":
                return self._register_product(params)
            elif action == "search":
                return self._search_products(params)
            elif action == "lineage":
                return self._get_lineage(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Data discovery failed: {e}")

    def _register_product(self, params: Dict[str, Any]) -> ActionResult:
        product_id = params.get("product_id", "")
        name = params.get("name", "")
        domain = params.get("domain", "")
        tags = params.get("tags", [])
        if not name:
            return ActionResult(success=False, message="name is required")
        self._registry[name] = {
            "product_id": product_id,
            "name": name,
            "domain": domain,
            "tags": tags,
            "registered_at": datetime.now(timezone.utc).isoformat(),
            "discoverable": True,
        }
        return ActionResult(success=True, message=f"Product '{name}' registered in discovery")

    def _search_products(self, params: Dict[str, Any]) -> ActionResult:
        query = params.get("query", "").lower()
        domain = params.get("domain", "")
        tags = params.get("tags", [])
        results = [
            p for p in self._registry.values()
            if (query in p["name"].lower() or any(query in t.lower() for t in p.get("tags", [])))
            and (not domain or p["domain"] == domain)
            and (not tags or any(t in p["tags"] for t in tags))
        ]
        return ActionResult(success=True, message=f"Found {len(results)} products", data={"results": results})

    def _get_lineage(self, params: Dict[str, Any]) -> ActionResult:
        product_name = params.get("product_name", "")
        depth = params.get("depth", 3)
        lineage = {
            "product": product_name,
            "upstream": [],
            "downstream": [],
            "depth": depth,
        }
        return ActionResult(success=True, message=f"Lineage for '{product_name}'", data=lineage)
