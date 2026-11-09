"""Central configuration for the lakehouse pipeline."""
from dataclasses import dataclass

@dataclass(frozen=True)
class PipelineConfig:
    source_path: str
    base_path: str

    @property
    def bronze_path(self):
        return f"{self.base_path.rstrip('/')}/bronze"

    @property
    def silver_path(self):
        return f"{self.base_path.rstrip('/')}/silver"

    @property
    def gold_path(self):
        return f"{self.base_path.rstrip('/')}/gold"
