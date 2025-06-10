from __future__ import annotations

from collections import defaultdict, deque
from typing import Dict, List, Set, Optional

from sqlalchemy import inspect
from sqlalchemy.engine import Engine


class DependencyGraph:
    """
    Build a FK dependency graph for every visible schema and
    return a minimal parent-before-child load order.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        A live SQLAlchemy Engine.
    include_schemas : set[str] | None, default None
        If given, only these schemas are scanned.
    exclude_schemas : set[str] | None, default {'pg_catalog', 'information_schema'}
        Schemas to ignore.

    Raises
    ------
    ValueError
        If cycles are found during topological sort.
    KeyError
        If `_resolve()` cannot map a bare table name.
    """

    # ────────────────────────────────────────────────────────────────── init ──
    def __init__(
        self,
        engine: Engine,
        *,
        include_schemas: Optional[Set[str]] = None,
        exclude_schemas: Optional[Set[str]] = None,
    ) -> None:
        self.inspector = inspect(engine)
        self.include_schemas = include_schemas
        self.exclude_schemas = exclude_schemas or {
            "pg_catalog",
            "information_schema",
        }

        # child → {parents}
        self.parents: Dict[str, Set[str]] = defaultdict(set)
        # parent → {children}
        self.children: Dict[str, Set[str]] = defaultdict(set)

        self._build()

    # ────────────────────────────────────────────────────────── utilities ──
    @staticmethod
    def _qual(schema: str, table: str) -> str:
        """Return schema-qualified table name `schema.table`."""
        return f"{schema}.{table}"

    def _resolve(self, name: str) -> str:
        """
        Map a bare table name to a unique schema-qualified name.

        If `name` already contains a dot, it is returned unchanged.

        Raises
        ------
        KeyError
            If the bare name is absent or ambiguous across schemas.
        """
        if "." in name:
            return name  # already qualified

        # search across all known tables
        candidates = [
            t
            for t in set(self.parents).union(self.children)
            if t.endswith(f".{name}")
        ]
        if not candidates:
            raise KeyError(
                f"Table '{name}' not found in any scanned schema. "
                "Specify schema.table."
            )
        if len(candidates) > 1:
            raise KeyError(
                f"Table name '{name}' is ambiguous across schemas: {candidates}. "
                "Specify schema.table."
            )
        return candidates[0]

    # ─────────────────────────────────────────────────────────── build ──
    def _build(self) -> None:
        """Populate `parents` and `children` dictionaries."""
        for schema in self.inspector.get_schema_names():
            if self.include_schemas and schema not in self.include_schemas:
                continue
            if schema in self.exclude_schemas:
                continue

            for table in self.inspector.get_table_names(schema=schema):
                child = self._qual(schema, table)

                for fk in self.inspector.get_foreign_keys(
                    table_name=table, schema=schema
                ):
                    ref_schema = fk.get("referred_schema") or schema
                    parent = self._qual(ref_schema, fk["referred_table"])

                    if parent == child:  # skip self-reference loops
                        continue
                    self.parents[child].add(parent)
                    self.children[parent].add(child)

    # ────────────────────────────────────────── public interface ──
    def sorted_tables(self, selected: Optional[List[str]] = None) -> List[str]:
        """
        Return a parent-first order for `selected` tables plus all ancestors.

        If `selected` is None or empty, the full database order is returned.
        Bare names are resolved automatically.
        """
        if not selected:
            selected_set = set(self.parents) | set(self.children)
        else:
            selected_set = {self._resolve(n) for n in selected}

        required = self._closure_up(selected_set)
        return self._toposort(required)

    # ───────────────────────────────────────────── helpers ──
    def _closure_up(self, tables: Set[str]) -> Set[str]:
        """
        Add every transitive parent of *tables*.  Returns the expanded set.
        """
        q = deque(tables)
        while q:
            tbl = q.popleft()
            for parent in self.parents.get(tbl, []):
                if parent not in tables:
                    tables.add(parent)
                    q.append(parent)
        return tables

    def _toposort(self, subset: Set[str]) -> List[str]:
        """
        Kahn topological sort on the induced subgraph.

        Raises
        ------
        ValueError
            If a FK cycle exists inside `subset`.
        """
        indeg = {t: 0 for t in subset}
        for child in subset:
            for parent in self.parents.get(child, []):
                if parent in subset:
                    indeg[child] += 1

        q = deque(sorted(t for t, d in indeg.items() if d == 0))
        order: List[str] = []

        while q:
            node = q.popleft()
            order.append(node)
            for child in sorted(self.children.get(node, [])):
                if child in indeg:
                    indeg[child] -= 1
                    if indeg[child] == 0:
                        q.append(child)

        if len(order) != len(subset):
            cycle = ", ".join(sorted(subset - set(order)))
            raise ValueError(f"Cycle detected in FK graph involving: {cycle}")

        return order

    # convenience
    def __iter__(self):
        return iter(self.sorted_tables())
