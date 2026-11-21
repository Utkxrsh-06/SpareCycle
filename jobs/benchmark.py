import math
def benchmark(start: int, end: int):
    """Deterministic CPU-bound sum used only for comparable benchmark runs."""
    return sum(math.sqrt(i) for i in range(start, end))
