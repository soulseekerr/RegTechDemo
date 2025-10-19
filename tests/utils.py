from api.orchestrator import Task, Context

class MockTask(Task):
    """A deterministic mock task for DAG and Runner tests."""
    def __init__(self, 
                 name: str, 
                 should_fail=False, max_retries=0, permanent_fail=False,
                 retry_sleep_s: float = 1.0):
        super().__init__(name, max_retries=max_retries, retry_sleep_s=retry_sleep_s)
        self.should_fail = should_fail
        self.permanent_fail = permanent_fail
        self.runs = 0

    def run(self, ctx: Context) -> dict[str, object]:
        self.runs += 1

        # Always fail if permanent_fail is True
        if self.should_fail:
            if self.permanent_fail:
                raise RuntimeError("permanent failure")
        
        # Otherwise fail until we exceed the max_retries count
        if self.runs <= self.max_retries:
            raise RuntimeError("intentional failure")

        ctx[self.name] = f"done-{self.runs}"
        return {self.name: ctx[self.name]}
