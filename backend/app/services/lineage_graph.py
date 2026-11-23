"""数据血缘领域服务：管理表级和字段级的数据血缘图、上下游追踪和断裂检测。

数据链路位置：元数据层，维护数据从源到目标的完整血缘关系图。

归档完成判定：启用归档策略、生成归档计划都不算归档完成，只有归档
执行写入策略的「已完成分区清单」才算确认。断裂扫描与分区血缘查询
都以这份完成记录为准。
"""

from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.table_archive import archived_partition_index
from app.store import store


class LineageGraphService(BaseModuleService):
    MODULE = "lineage_graph"
    ENTITY = "血缘关系"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["目标表", "来源表列表", "转换逻辑", "中间层级", "血缘深度"]
    REQUIRED_FIELDS = ["目标表", "来源表列表", "转换逻辑"]
    STATUS_ORDER = ["已建立", "待验证", "已断裂"]
    ACTION_RULES = {"刷新血缘": "已建立", "标记断裂": "已断裂"}
    NEGATIVE_ACTIONS = []

    def trace_upstream(self, target_table: str, max_depth: int = 5) -> dict[str, Any]:
        """从目标表向上追溯所有上游来源表和转换链路。

        返回完整的上游依赖树，每个节点包含表名、所在层级和转换逻辑。
        用于变更影响分析。
        """
        all_entries = store.rows(self.MODULE)
        visited: set[str] = set()
        tree: list[dict[str, Any]] = []
        queue = [(target_table, 0)]
        while queue and len(visited) < 50:
            current, depth = queue.pop(0)
            if current in visited or depth > max_depth:
                continue
            visited.add(current)
            for entry in all_entries:
                if entry.get("目标表") == current and entry.get("status") == "已建立":
                    sources = entry.get("来源表列表", "").split(",")
                    node = {
                        "table": current,
                        "depth": depth,
                        "sources": [s.strip() for s in sources if s.strip()],
                        "transform": entry.get("转换逻辑"),
                        "lineage_id": entry.get("id"),
                    }
                    tree.append(node)
                    for src in node["sources"]:
                        if src not in visited:
                            queue.append((src, depth + 1))

        return {
            "target_table": target_table,
            "max_depth": max_depth,
            "total_nodes": len(tree),
            "upstream_tree": tree,
            "leaf_tables": [n["table"] for n in tree if not n["sources"]],
        }

    def trace_downstream(self, source_table: str, max_depth: int = 5) -> dict[str, Any]:
        """从来源表向下追溯所有下游受影响表和转换链路。

        用于评估某个表变更时的影响范围，联动质量规则模块通知下游。
        """
        all_entries = store.rows(self.MODULE)
        visited: set[str] = set()
        tree: list[dict[str, Any]] = []
        queue = [(source_table, 0)]
        while queue and len(visited) < 50:
            current, depth = queue.pop(0)
            if current in visited or depth > max_depth:
                continue
            visited.add(current)
            for entry in all_entries:
                sources = entry.get("来源表列表", "").split(",")
                source_set = {s.strip() for s in sources if s.strip()}
                if current in source_set and entry.get("status") == "已建立":
                    target = entry.get("目标表", "")
                    node = {
                        "table": target,
                        "depth": depth + 1,
                        "upstream_of": current,
                        "transform": entry.get("转换逻辑"),
                        "lineage_id": entry.get("id"),
                    }
                    tree.append(node)
                    if target not in visited:
                        queue.append((target, depth + 1))

        return {
            "source_table": source_table,
            "max_depth": max_depth,
            "total_affected": len(tree),
            "downstream_tree": tree,
            "end_consumers": [n["table"] for n in tree if not any(
                e.get("来源表列表", "").find(n["table"]) >= 0
                for e in all_entries if e.get("status") == "已建立"
            )],
        }

    def detect_broken_lineage(self) -> dict[str, Any]:
        """扫描并检测断裂的血缘关系。

        保留已经被标记为断裂的血缘记录。表归档是按分区执行的，
        某个来源表的某一天分区已确认归档不代表整张来源表不可用，
        因此扫描不会因为归档而断开整条血缘，也不会把启用策略或
        生成计划当作归档完成。
        """
        all_entries = store.rows(self.MODULE)
        broken: list[dict[str, Any]] = []
        for entry in all_entries:
            if entry.get("status") == "已断裂":
                broken.append({
                    "lineage_id": entry.get("id"),
                    "target_table": entry.get("目标表"),
                    "sources": entry.get("来源表列表"),
                    "needs_revalidation": True,
                })

        return {
            "broken_count": len(broken),
            "broken_lineages": broken,
            "suggestion": "请重新校验断裂血缘并更新目标表关联",
        }

    def trace_partition_sources(
        self,
        target_table: str,
        partition_date: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """按分区日期查询目标表的来源表及对应来源分区的归档状态。

        只有归档执行确认过的来源分区才显示归档存储和分区路径；
        其他日期以及同一条血缘里的独立来源保持可查询状态。
        """
        partition_date = partition_date or kwargs.get("date") or kwargs.get("dt") or ""
        entries = [
            entry for entry in store.rows(self.MODULE)
            if entry.get("目标表") == target_table and entry.get("status") == "已建立"
        ]
        if not entries:
            return {
                "ok": False,
                "message": f"未找到目标表 {target_table} 的已建立血缘",
                "target_table": target_table,
                "partition_date": partition_date,
                "sources": [],
            }

        source_tables: list[str] = []
        for entry in entries:
            for raw in entry.get("来源表列表", "").split(","):
                source = raw.strip()
                if source and source not in source_tables:
                    source_tables.append(source)

        archived_index = archived_partition_index()
        sources: list[dict[str, Any]] = []
        for source in source_tables:
            record = archived_index.get((source, partition_date))
            archived = record is not None
            item: dict[str, Any] = {
                "source_table": source,
                "partition_date": partition_date,
                "archived": archived,
                "已归档": archived,
                "available": not archived,
                "archive_storage": record.get("storage") if record else None,
                "归档存储": record.get("storage") if record else None,
                "partition_path": record.get("partition") if record else None,
                "分区路径": record.get("partition") if record else None,
            }
            if record:
                item["archived_at"] = record.get("archived_at")
                item["policy_id"] = record.get("policy_id")
            sources.append(item)

        return {
            "ok": True,
            "target_table": target_table,
            "partition_date": partition_date,
            "sources": sources,
            "source_tables": source_tables,
            "archived_sources": [s["source_table"] for s in sources if s["archived"]],
            "available_sources": [s["source_table"] for s in sources if not s["archived"]],
        }

    # 同一能力的别名，便于不同调用方按习惯命名访问。
    get_partition_sources = trace_partition_sources
    trace_partition_lineage = trace_partition_sources

    def build_inheritance_chain(self, table_name: str) -> dict[str, Any]:
        """为指定表构建完整的继承链（上下游合起来）。

        从该表出发，同时向上追溯祖先表和向下追溯后代表，
        生成完整的数据流转全景图。
        """
        upstream = self.trace_upstream(table_name)
        downstream = self.trace_downstream(table_name)
        return {
            "table": table_name,
            "upstream_depth": upstream["total_nodes"],
            "downstream_affected": downstream["total_affected"],
            "upstream_chain": upstream["upstream_tree"],
            "downstream_chain": downstream["downstream_tree"],
            "orphan_risk": upstream["total_nodes"] == 0 and downstream["total_affected"] > 0,
        }

    def impact_analysis(self, entry_id: int) -> dict[str, Any]:
        """变更影响分析：当某个血缘条目的转换逻辑发生变更时，
        评估对下游的级联影响范围和受影响的数据质量规则。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "血缘条目不存在"}

        target_table = entry.get("目标表", "")
        downstream = self.trace_downstream(target_table)

        quality_rows = store.rows("data_quality_rule")
        affected_quality_rules = [
            q for q in quality_rows
            if q.get("目标表") in [n["table"] for n in downstream["downstream_tree"]]
        ]

        return {
            "lineage_id": entry_id,
            "source_table": target_table,
            "change_type": "transform_logic_changed",
            "direct_downstream_tables": len(downstream["downstream_tree"]),
            "cascaded_tables": downstream["total_affected"],
            "affected_quality_rule_count": len(affected_quality_rules),
            "affected_quality_rules": [
                {"rule_id": r["id"], "rule_name": r.get("规则名称"), "target_table": r.get("目标表")}
                for r in affected_quality_rules[:10]
            ],
            "recommendation": "建议在变更后立即执行受影响的质量规则校验",
        }