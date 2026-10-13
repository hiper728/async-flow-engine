import asyncio
import time
from typing import Callable, Dict, List, Set, Any

class TaskNode:
    def __init__(self, name: str, fn: Callable[[], Any], dependencies: List[str] = None):
        self.name = name
        self.fn = fn
        self.dependencies: Set[str] = set(dependencies or [])
        self.result: Any = None
        self.duration_ms: float = 0.0

class WorkflowEngine:
    def __init__(self):
        self.tasks: Dict[str, TaskNode] = {}

    def add_task(self, name: str, fn: Callable[[], Any], dependencies: List[str] = None) -> "WorkflowEngine":
        self.tasks[name] = TaskNode(name, fn, dependencies)
        return self

    async def execute(self) -> Dict[str, Any]:
        completed: Set[str] = set()
        results: Dict[str, Any] = {}

        while len(completed) < len(self.tasks):
            ready = [
                task for task in self.tasks.values()
                if task.name not in completed and task.dependencies.issubset(completed)
            ]
            if not ready:
                raise RuntimeError("Cyclic dependency or unresolvable task detected in workflow DAG")

            async def _run(node: TaskNode):
                t0 = time.perf_counter()
                if asyncio.iscoroutinefunction(node.fn):
                    res = await node.fn()
                else:
                    res = node.fn()
                node.duration_ms = round((time.perf_counter() - t0) * 1000, 2)
                node.result = res
                return node

            batch_res = await asyncio.gather(*[_run(n) for n in ready])
            for n in batch_res:
                completed.add(n.name)
                results[n.name] = {"result": n.result, "duration_ms": n.duration_ms}

        return results
