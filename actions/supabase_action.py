"""Supabase action module for RabAI AutoClick.

Provides Supabase database operations including queries, inserts, updates, and auth.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SupabaseQueryAction(BaseAction):
    """Execute queries on Supabase PostgreSQL database."""
    action_type = "supabase_query"
    display_name = "Supabase查询"
    description = "Supabase数据库查询"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute Supabase query.

        Args:
            context: Execution context.
            params: Dict with keys:
                - url: Supabase project URL
                - key: Supabase anon/public key
                - table: Table name
                - select: Columns to select
                - filters: Dict of filter conditions
                - limit: Row limit

        Returns:
            ActionResult with query results.
        """
        url = params.get('url', '') or os.environ.get('SUPABASE_URL')
        key = params.get('key', '') or os.environ.get('SUPABASE_KEY')
        table = params.get('table', '')
        select = params.get('select', '*')
        filters = params.get('filters', {})
        limit = params.get('limit', 100)

        if not url or not key:
            return ActionResult(success=False, message="url and key are required")
        if not table:
            return ActionResult(success=False, message="table is required")

        try:
            from supabase import create_client, Client
        except ImportError:
            return ActionResult(success=False, message="supabase-py not installed. Run: pip install supabase")

        start = time.time()
        try:
            client: Client = create_client(url, key)
            query = client.table(table).select(select)
            for col, val in filters.items():
                query = query.eq(col, val)
            query = query.limit(limit)
            response = query.execute()
            duration = time.time() - start
            return ActionResult(
                success=True, message=f"Query returned {len(response.data)} rows",
                data={'rows': response.data, 'count': len(response.data)}, duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Supabase query error: {str(e)}")


class SupabaseInsertAction(BaseAction):
    """Insert records into Supabase table."""
    action_type = "supabase_insert"
    display_name = "Supabase插入"
    description = "Supabase数据插入"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Insert into Supabase.

        Args:
            context: Execution context.
            params: Dict with keys:
                - url, key: Supabase credentials
                - table: Table name
                - data: Dict or list of dicts to insert

        Returns:
            ActionResult with inserted records.
        """
        url = params.get('url', '') or os.environ.get('SUPABASE_URL')
        key = params.get('key', '') or os.environ.get('SUPABASE_KEY')
        table = params.get('table', '')
        data = params.get('data', {})

        if not url or not key:
            return ActionResult(success=False, message="url and key are required")
        if not table:
            return ActionResult(success=False, message="table is required")
        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            from supabase import create_client
        except ImportError:
            return ActionResult(success=False, message="supabase-py not installed")

        start = time.time()
        try:
            client = create_client(url, key)
            response = client.table(table).insert(data).execute()
            duration = time.time() - start
            return ActionResult(
                success=True, message=f"Inserted {len(response.data)} records",
                data={'records': response.data}, duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Supabase insert error: {str(e)}")
