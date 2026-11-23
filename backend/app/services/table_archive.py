"""表归档领域服务：规划表归档的执行计划、执行归档和归档安全检查。

数据链路位置：数据归档层，将历史数据表归档到低成本存储。

归档完成判定：启用策略、生成计划都不算归档完成，只有执行归档并
写入策略的「已完成分区清单」才算确认。重新规划和数据血缘追溯都
依据这份完成记录判定归档范围。
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

#: 归档策略条目上保存已完成分区清单的字段名。
#: 只有归档执行确认后才写入；启用策略、生成计划都不会改动它。
COMPLETED_PARTITIONS_FIELD = "已完成分区清单"


def archived_partition_index() -> dict[tuple[str, str], dict[str, Any]]:
    """汇总全部归档策略的已完成分区清单，构建 (来源表, 分区日期) -> 归档记录 索引。

    数据血缘模块据此判断某个来源表在某一天的分区是否已确认归档，
    与表归档模块重新规划读取的是同一份完成记录。
    """
    index: dict[tuple[str, str], dict[str, Any]] = {}
    for policy in store.rows("table_archive"):
        for record in policy.get(COMPLETED_PARTITIONS_FIELD) or []:
            key = (record.get("source_table"), record.get("partition_date"))
            if key[0] and key[1]:
                index.setdefault(key, record)
    return index


class TableArchiveService(BaseModuleService):
    MODULE = "table_archive"
    ENTITY = "归档策略"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["策略名称", "目标表", "归档条件", "归档存储", "原始保留天数"]
    REQUIRED_FIELDS = ["策略名称", "目标表", "归档条件"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用归档": "已启用", "停用归档": "已停用"}
    NEGATIVE_ACTIONS = ["停用归档"]

    STORAGE_TIERS = frozenset({"S3_Standard", "S3_IA", "S3_Glacier", "HDFS_COLD", "OSS_Archive"})

    def plan_archive_execution(self, entry_id: int) -> dict[str, Any]:
        """规划归档执行范围。

        根据归档条件计算需要归档的表分区列表，
        估算归档后节省的存储成本。
        已确认归档的分区（已完成分区清单）不会再次纳入计划，
        因此归档回执丢失后重新规划也不会重复返回已处理的分区。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "归档策略不存在"}

        target_table = entry.get("目标表", "")
        archive_condition = entry.get("归档条件", "")
        archive_storage = entry.get("归档存储", "S3_IA")
        retention_days = int(entry.get("原始保留天数", "30"))

        expired = self._compute_expired_partitions(target_table, retention_days)
        completed = self._completed_partitions(entry)
        completed_set = {record["partition"] for record in completed}
        partitions_to_archive = [p for p in expired if p not in completed_set]
        return {
            "ok": True,
            "policy_id": entry_id,
            "policy_name": entry.get("策略名称"),
            "target_table": target_table,
            "archive_condition": archive_condition,
            "target_storage": archive_storage,
            "retention_days": retention_days,
            "partitions_to_archive": len(partitions_to_archive),
            "partitions": partitions_to_archive[:30],
            "partition_scope": partitions_to_archive,
            "already_archived_partitions": len(completed),
            "estimated_data_size_gb": len(partitions_to_archive) * 0.45,
            "storage_cost_savings_percent": 68 if archive_storage in ("S3_Glacier", "S3_IA") else 40,
        }

    def execute_archive(
        self,
        entry_id: int,
        plan: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """执行归档操作。

        将符合条件的表分区数据迁移到指定的归档存储层，
        完成后把执行结果同步到策略的已完成分区清单，
        供冷数据恢复和数据血缘模块查询。

        可以传入之前生成（值班员已看过）的执行计划，按计划范围归档；
        不传计划时按当前策略重新计算归档范围。两种方式都会从范围中
        剔除已确认归档的分区，只把本次新增归档的分区写入完成记录，
        重复提交不会重复计数。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "归档策略不存在"}
        if entry.get("status") != "已启用":
            return {"ok": False, "message": "只有已启用的策略才能执行归档"}

        if plan is None:
            plan = self.plan_archive_execution(entry_id)
        else:
            plan_policy = plan.get("policy_id")
            if plan_policy is not None and int(plan_policy) != entry_id:
                return {"ok": False, "message": "执行计划与归档策略不匹配"}
            plan_table = plan.get("target_table")
            if plan_table and plan_table != entry.get("目标表", ""):
                return {"ok": False, "message": "执行计划与归档策略不匹配"}

        target_table = entry.get("目标表", "")
        target_storage = plan.get("target_storage") or entry.get("归档存储", "S3_IA")
        scope = plan.get("partition_scope") or plan.get("partitions") or []

        completed = self._completed_partitions(entry)
        completed_set = {record["partition"] for record in completed}
        new_partitions = [p for p in scope if p not in completed_set]

        new_records = []
        for partition in new_partitions:
            new_records.append({
                "policy_id": entry_id,
                "source_table": target_table,
                "partition": partition,
                "partition_date": self._partition_date(partition),
                "storage": target_storage,
                "archived_at": "2026-11-21T08:00:00Z",
                "status": "archived",
            })
        # 执行结果完整同步到已完成分区清单：回执摘要（archive_records）
        # 可能截断，但清单里必须包含本次全部新增归档分区，且每个分区只记一次。
        completed.extend(new_records)

        return {
            "ok": True,
            "policy_id": entry_id,
            "target_table": target_table,
            "target_storage": target_storage,
            "archived_partitions": len(new_records),
            "archived_data_gb": len(new_records) * 0.45,
            "archive_records": new_records[:10],
            "newly_archived_partitions": [r["partition"] for r in new_records],
            "newly_archived_partition_dates": [r["partition_date"] for r in new_records],
            "skipped_already_archived": len(scope) - len(new_partitions),
            "total_completed_partitions": len(completed),
            "restore_available": True,
            "message": (
                f"已归档 {len(new_records)} 个分区到 {target_storage}"
                if new_records
                else "没有新的分区需要归档"
            ),
        }

    def check_archive_safety(self, entry_id: int) -> dict[str, Any]:
        """归档安全性检查。

        在归档前检查目标表是否正在被其他流水线使用、
        是否有未完成的增量合并在进行中、是否有未满足的保留要求。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "归档策略不存在"}

        target_table = entry.get("目标表", "")
        pipeline_rows = store.rows("etl_pipeline")
        active_pipelines = [
            p for p in pipeline_rows
            if p.get("目标端类型") == target_table and p.get("status") in ("运行中",)
        ]
        merge_rows = store.rows("delta_merge")
        active_merges = [
            m for m in merge_rows
            if m.get("目标表") == target_table and m.get("status") in ("排队中", "合并中")
        ]
        safe = len(active_pipelines) == 0 and len(active_merges) == 0
        return {
            "safe_to_archive": safe,
            "blockers": {
                "active_pipelines": len(active_pipelines),
                "active_merges": len(active_merges),
            },
            "message": "可以安全归档" if safe else "存在活跃任务，请等待完成后归档",
        }

    def get_archived_tables_inventory(self) -> dict[str, Any]:
        """获取已归档表的清单。

        供冷数据恢复模块查询哪些表有可恢复的归档数据。
        已完成分区清单来自归档执行确认记录，执行回执摘要里
        没有列出的分区同样包含在清单中。
        """
        all_policies = store.rows(self.MODULE)
        enabled = [p for p in all_policies if p.get("status") == "已启用"]
        completed_records = [
            record
            for policy in all_policies
            for record in (policy.get(COMPLETED_PARTITIONS_FIELD) or [])
        ]
        return {
            "archived_table_count": len(enabled),
            "tables": [
                {
                    "table": p.get("目标表"),
                    "storage": p.get("归档存储"),
                    "policy_name": p.get("策略名称"),
                    "original_retention": p.get("原始保留天数"),
                    "completed_partition_count": len(p.get(COMPLETED_PARTITIONS_FIELD) or []),
                }
                for p in enabled
            ],
            "completed_partition_total": len(completed_records),
            "completed_partitions": completed_records,
        }

    def get_completed_partitions(self, entry_id: int) -> dict[str, Any]:
        """读取策略的已完成分区清单（归档执行确认过的完整记录）。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "归档策略不存在"}
        completed = list(entry.get(COMPLETED_PARTITIONS_FIELD) or [])
        return {
            "ok": True,
            "policy_id": entry_id,
            "policy_name": entry.get("策略名称"),
            "target_table": entry.get("目标表"),
            "total": len(completed),
            "completed_partitions": completed,
        }

    def _completed_partitions(self, entry: dict[str, Any]) -> list[dict[str, Any]]:
        completed = entry.get(COMPLETED_PARTITIONS_FIELD)
        if not isinstance(completed, list):
            completed = []
            entry[COMPLETED_PARTITIONS_FIELD] = completed
        return completed

    @staticmethod
    def _partition_date(partition: str) -> str:
        if "dt=" in partition:
            return partition.split("dt=", 1)[1].strip("/")
        return partition.rsplit("/", 1)[-1]

    def _compute_expired_partitions(self, table: str, retention_days: int) -> list[str]:
        today = datetime(2026, 11, 21)
        partitions = []
        for i in range(retention_days, retention_days + 15):
            d = today - timedelta(days=i)
            partitions.append(f"{table}/dt={d.strftime('%Y-%m-%d')}")
        return partitions
