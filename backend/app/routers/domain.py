"""领域能力接口：把数据血缘追溯和表归档规划/执行等领域方法暴露为 HTTP 入口。

通用资源路由（/api/{module}）只覆盖列表/登记/动作；这里的入口承载
领域服务里的业务能力：断裂扫描、血缘追溯、按日期查分区血缘来源、
归档规划与执行、已完成分区清单查询。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.services import service_for

router = APIRouter(prefix="/api/domain", tags=["领域能力"])


def _require(module: str):
    service = service_for(module)
    if service is None:
        raise HTTPException(status_code=404, detail=f"未知业务模块：{module}")
    return service


class ExecuteArchivePayload(BaseModel):
    """归档执行提交体：可携带值班员已看过的执行计划；不携带则按当前策略重算。"""

    plan: dict[str, Any] | None = None


@router.get("/lineage_graph/trace_upstream")
def trace_upstream(
    target_table: str = Query(...),
    max_depth: int = Query(default=5),
) -> dict[str, Any]:
    return _require("lineage_graph").trace_upstream(target_table, max_depth)


@router.get("/lineage_graph/trace_partition_sources")
def trace_partition_sources(
    target_table: str = Query(...),
    partition_date: str = Query(...),
) -> dict[str, Any]:
    return _require("lineage_graph").trace_partition_sources(target_table, partition_date)


@router.get("/lineage_graph/detect_broken_lineage")
def detect_broken_lineage() -> dict[str, Any]:
    return _require("lineage_graph").detect_broken_lineage()


@router.get("/table_archive/inventory")
def archived_tables_inventory() -> dict[str, Any]:
    return _require("table_archive").get_archived_tables_inventory()


@router.get("/table_archive/{entry_id}/plan")
def plan_archive_execution(entry_id: int) -> dict[str, Any]:
    return _require("table_archive").plan_archive_execution(entry_id)


@router.post("/table_archive/{entry_id}/execute")
def execute_archive(entry_id: int, payload: ExecuteArchivePayload | None = None) -> dict[str, Any]:
    plan = payload.plan if payload else None
    return _require("table_archive").execute_archive(entry_id, plan=plan)


@router.get("/table_archive/{entry_id}/completed_partitions")
def completed_partitions(entry_id: int) -> dict[str, Any]:
    return _require("table_archive").get_completed_partitions(entry_id)
