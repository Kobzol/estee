
from enum import Enum
from simpy import Environment, Event


class TaskState(Enum):
    Waiting = 1
    Ready = 2
    Assigned = 3
    Finished = 4


class TaskRuntimeInfo:

    __slots__ = ("state",
                 "assign_time",
                 "end_time",
                 "assigned_workers",
                 "unfinished_inputs")

    def __init__(self, task):
        self.state = TaskState.Waiting
        self.assign_time = None
        self.end_time = None
        self.assigned_workers = []
        self.unfinished_inputs = len(task.inputs)

    @property
    def is_running(self):
        return self.state == TaskState.Running

    @property
    def is_ready(self):
        return self.state == TaskState.Ready

    @property
    def is_finished(self):
        return self.state == TaskState.Finished

    @property
    def is_waiting(self):
        return self.state == TaskState.Waiting


class Simulator:

    def __init__(self, task_graph, workers, scheduler):
        self.workers = workers
        self.task_graph = task_graph
        self.scheduler = scheduler
        scheduler.simulator = self
        self.new_finished = []
        self.new_ready = []
        self.wakeup_event = None

    def schedule(self, ready_tasks, finished_tasks):
        for worker, task in self.scheduler.schedule(ready_tasks, finished_tasks):
            info = task.info
            if info.state == TaskState.Finished:
                raise Exception("Scheduler tries to assign a finished task ({})".format(task))
            info.state = TaskState.Assigned
            info.assigned_workers.append(worker)
            worker.assign_task(task)

    def _master_process(self, env):
        self.schedule(self.task_graph.source_nodes(), [])

        while self.unprocessed_tasks > 0:
            self.wakeup_event = Event(env)
            yield self.wakeup_event
            self.schedule(self.new_ready, self.new_finished)
            self.new_finished = []
            self.new_ready = []

    def on_task_finished(self, worker, task):
        print("TASK FINISHED", task)
        info = task.info
        assert info.state == TaskState.Assigned
        assert worker in info.assigned_workers
        info.state = TaskState.Finished
        self.new_finished.append(task)
        self.unprocessed_tasks -= 1

        for t in task.consumers:
            t_info = t.info
            t_info.unfinished_inputs -= 1
            if t_info.unfinished_inputs <= 0:
                if t_info.unfinished_inputs < 0:
                    raise Exception("Invalid number of unfinished inputs: {}, task {}".format(
                        t_info.unfinished_inputs, t
                    ))
                assert t_info.unfinished_inputs == 0
                assert t_info.state == TaskState.Waiting
                t_info.state = TaskState.Ready
                self.new_ready.append(t)

        if not self.wakeup_event.triggered:
            self.wakeup_event.succeed()

    def run(self):
        for task in self.task_graph.tasks:
            task.info = TaskRuntimeInfo(task)

        self.unprocessed_tasks = self.task_graph.task_count

        env = Environment()
        for worker in self.workers:
            env.process(worker.run(env, self))

        master_process = env.process(self._master_process(env))
        self.scheduler.init(self)

        env.run(master_process)
        return env.now