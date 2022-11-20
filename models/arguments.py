from dataclasses import dataclass

@dataclass
class Arguments:
    index: str
    host: str
    max_workers: int
    batch_size: int
    output: str
    scroll_time: str
    thread_creation_sleep: float