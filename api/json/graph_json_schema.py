GRAPH_SCHEMA = {
    "type": "object",
    "required": ["version", "tasks", "edges"],
    "properties": {
        "version": {"type": "integer", "minimum": 1},
        "name": {"type": "string"},
        "defaults": {
            "type": "object",
            "properties": {
                "max_retries": {"type": "integer", "minimum": 0},
                "retry_sleep_s": {"type": "number", "minimum": 0}
            }
        },
        "context": {"type": "object"},
        "tasks": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["id", "type"],
                "properties": {
                    "id": {"type": "string"},
                    "type": {"type": "string"},
                    "params": {"type": "object"},
                    "max_retries": {"type": "integer", "minimum": 0},
                    "retry_sleep_s": {"type": "number", "minimum": 0}
                }
            }
        },
        "edges": {
            "type": "array",
            "items": {
                "type": "array",
                "minItems": 2,
                "maxItems": 2,
                "items": {"type": "string"}
            }
        }
    }
}
