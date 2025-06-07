from __future__ import annotations
from collections import defaultdict, deque
from typing import Dict, List, Set
from sqlalchemy import inspect
from sqlalchemy.engine import Engine


class DependencyGraph:
    """Build FK graph and return minimal parent-first order."""

    def __init__(self, engine: Engine) -> None:
        self.inspector = inspect(engine)
        self.parents_of:  Dict[str, Set[str]] = defaultdict(set)  # child → {parents}
        self.children_of: Dict[str, Set[str]] = defaultdict(set)  # parent → {children}
        self._build()

    def _build(self):
        for tbl in self.inspector.get_table_names():
            for fk in self.inspector.get_foreign_keys(tbl):
                p = fk["referred_table"]; c = tbl
                self.parents_of[c].add(p)
                self.children_of[p].add(c)

    def sorted_tables(self, selected: List[str]) -> List[str]:
        """selection + all parents, FK-safe order."""
        req = self._closure_up(set(selected))
        return self._toposort(req)

    def _closure_up(self, tables: Set[str]) -> Set[str]:
        q = deque(tables)
        while q:
            t = q.popleft()
            for p in self.parents_of.get(t, []):
                if p not in tables:
                    tables.add(p); q.append(p)
        return tables

    def _toposort(self, subset: Set[str]) -> List[str]:
        deg = {t: 0 for t in subset}
        for c in subset:
            for p in self.parents_of.get(c, []):
                if p in subset: deg[c] += 1
        q = deque([t for t, d in deg.items() if d == 0])
        order: List[str] = []
        while q:
            n = q.popleft(); order.append(n)
            for c in self.children_of.get(n, []):
                if c in deg:
                    deg[c] -= 1
                    if deg[c] == 0: q.append(c)
        if len(order) != len(subset):
            raise ValueError("Cycle detected in FK graph.")
        return order
