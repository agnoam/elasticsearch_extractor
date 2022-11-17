from dataclasses import dataclass

@dataclass
class Arguments:
    index: str
    host: str
    max_workers: int
    batch_size: int
    output_dir: str