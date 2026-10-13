import pytest
import asyncio
from asyncflow.dag import WorkflowEngine

@pytest.mark.asyncio
async def test_dag_execution():
    flow = WorkflowEngine()
    flow.add_task("A", lambda: 10)
    flow.add_task("B", lambda: 20, dependencies=["A"])
    flow.add_task("C", lambda: 30, dependencies=["A", "B"])

    res = await flow.execute()
    assert res["A"]["result"] == 10
    assert res["B"]["result"] == 20
    assert res["C"]["result"] == 30

@pytest.mark.asyncio
async def test_async_task():
    flow = WorkflowEngine()
    async def fetch_data():
        await asyncio.sleep(0.01)
        return "async_ok"
    flow.add_task("fetch", fetch_data)
    res = await flow.execute()
    assert res["fetch"]["result"] == "async_ok"
