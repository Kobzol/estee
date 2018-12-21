import numpy as np


def process_imode_exact(graph):
    """Sets the expected durations and sizes equal to the real durations and sizes."""
    for t in graph.tasks:
        t.expected_duration = t.duration

    for o in graph.outputs:
        o.expected_size = o.size


def process_imode_blind(graph):
    """Sets both expected durations and expected sizes to None."""
    _set_consts(graph, None, None)


def process_imode_user(graph):
    """No-op (leaves the expected durations and sizes as they were)."""
    pass


def process_imode_mean(graph):
    """Sets the expected durations to the mean of all task durations in the graph.
    Sets the expected sizes to the mean of all task output sizes in the graph."""
    durations = np.array([t.duration for t in graph.tasks])
    sizes = np.array([o.size for o in graph.outputs])
    _set_consts(graph,
                durations.mean() if graph.tasks else 0,
                sizes.mean() if graph.outputs else 0)


def _set_consts(graph, duration, size):
    for t in graph.tasks:
        t.expected_duration = duration

    for o in graph.outputs:
        o.expected_size = size
