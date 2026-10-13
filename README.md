# AsyncFlow Engine &mdash; Asynchronous DAG Task Orchestrator

[![Python 3.13](https://img.shields.io/badge/Python-3.13-blue.svg)](https://python.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Type Checked](https://img.shields.io/badge/Typing-Strict-brightgreen.svg)](https://github.com/hiper728)

**AsyncFlow** is a minimalist, ultra-fast asynchronous directed acyclic graph (DAG) task runner for Python 3.13. It resolves dependency ordering on-the-fly and executes parallel batches concurrently with bounded latency.

---

## ⚡ Architecture Flow

```
[Task A: Fetch Data]
        |
        +-----> [Task B: Filter & Clean] -----+
        |                                      |
        +-----> [Task C: Feature Extraction] --+----> [Task D: Aggregate Report]
```

---

## 🚀 Quick Example

```python
import asyncio
from asyncflow.dag import WorkflowEngine

async def main():
    flow = WorkflowEngine()
    flow.add_task("fetch_db", lambda: [1, 2, 3, 4, 5])
    flow.add_task("compute_avg", lambda: sum([1, 2, 3, 4, 5]) / 5, dependencies=["fetch_db"])

    results = await flow.execute()
    print("Workflow Output:", results["compute_avg"]["result"])

asyncio.run(main())
```

Developed by [Leonard Johnson](https://github.com/hiper728) &bull; [Portfolio](https://hiper728.github.io)
