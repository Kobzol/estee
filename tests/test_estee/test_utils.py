import pytest

from estee.common import TaskGraph, DataObject
from estee.schedulers import SchedulerBase
from estee.simulator import InstantNetModel, Simulator, Worker


@pytest.fixture
def plan1():
    """
        a1/1 a2/3/3
        |    |
        a3/1 | a4/1/6
        |\  / /|
        o o |/ |
        | a5/1 a6/6 a7/2
        |  \   |   /
        |   \  |  /
         \--- a8/1
    """  # noqa
    task_graph = TaskGraph()
    tasks = []
    oid = 0

    for i, (duration, outputs) in enumerate([
        (2, [1]),       # a1
        (3, [3]),       # a2
        (2, [1, 1]),    # a3
        (1, [6]),       # a4
        (1, [1]),       # a5
        (6, [1]),       # a6
        (1, [2]),       # a7
        (1, [])         # a8
    ]):
        objects = []
        for size in outputs:
            objects.append(DataObject(oid, size, size))
            oid += 1
        tasks.append(task_graph.new_task("a{}".format(i + 1),
                                         duration=duration,
                                         expected_duration=duration,
                                         outputs=objects))
    a1, a2, a3, a4, a5, a6, a7, a8 = tasks

    a3.add_input(a1)
    a5.add_inputs([a3.outputs[0], a2, a4])
    a6.add_input(a4)
    a8.add_inputs([a5, a6, a7, a3.outputs[1]])

    task_graph.validate()

    return task_graph


def plan_reverse_cherry1():
    """
        a1/10/1  a2/10/1
          \     /
           \   /
             a3
    """  # noqa
    task_graph = TaskGraph()
    a1 = task_graph.new_task("a1", 10, 1)
    a2 = task_graph.new_task("a2", 10, 1)
    a3 = task_graph.new_task("a3", 1)

    a3.add_input(a1)
    a3.add_input(a2)
    return task_graph


def do_sched_test(task_graph, workers, scheduler,
                  netmodel=None, trace=False, return_simulator=False,
                  min_scheduling_interval=None, scheduling_time=None):

    if netmodel is None:
        netmodel = InstantNetModel()

    if isinstance(workers, int):
        workers = [Worker() for _ in range(workers)]
    elif isinstance(workers[0], int):
        workers = [Worker(cpus=cpus) for cpus in workers]
    else:
        assert isinstance(workers[0], Worker)
    simulator = Simulator(task_graph, workers, scheduler, netmodel,
                          trace=trace,
                          scheduling_time=scheduling_time,
                          min_scheduling_interval=min_scheduling_interval)
    result = simulator.run()
    if return_simulator:
        return simulator
    else:
        return result


def task_by_name(plan, name):
    return [t for t in plan.tasks.values() if t.name == name][0]


def fixed_scheduler(assignments, steps=False, reassigning=False):

    class FixScheduler(SchedulerBase):

        def __init__(self):
            super().__init__("fix_scheduler", "0", reassigning=reassigning)
            self.step = 0

        def start(self):
            self.step = 0
            return super().start()

        def schedule(self, update):
            if not self.task_graph.tasks:
                return ()

            step = self.step
            self.step += 1

            if steps:
                if len(assignments) <= step:
                    return ()
                a = assignments[step]
            else:
                a = assignments
                if step > 0:
                    return ()

            for definition in a:
                if definition[0] is None:
                    worker = None
                else:
                    worker = self.workers[definition[0]]
                task = self.task_graph.tasks[definition[1].id]
                self.assign(worker, task, *definition[2:])

    return FixScheduler()
