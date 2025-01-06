from dataclasses import dataclass


@dataclass(frozen=True)
class BlockId:
    filename: str
    number: int

    def __str__(self) -> str:
        return f"[file {self.filename}, block {self.number}]"
