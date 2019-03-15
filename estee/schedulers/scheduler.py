
from ..simulator.runtimeinfo import TaskState
from .tasks import SchedulerTaskGraph, SchedulerTask, SchedulerDataObject
from collections import namedtuple

import logging
logger = logging.getLogger(__name__)

class SchedulerInterface:

    def send_message(self, message):
        raise NotImplementedError()

    def start(self):
        raise NotImplementedError()

    def stop(self):
        pass


class SchedulerWorker:

    def __init__(self, worker_id, cpus):
        self.worker_id = worker_id
        self.cpus = cpus


class Update:

    def __init__(self,
                 new_workers,
                 network_update,
                 new_objects,
                 new_tasks,
                 new_ready_tasks,
                 new_finished_tasks):

        self.new_workers = new_workers
        self.network_update = network_update
        self.new_objects = new_objects
        self.new_tasks = new_tasks
        self.new_ready_tasks = new_ready_tasks
        self.new_finished_tasks = new_finished_tasks

    @property
    def graph_changed(self):
        return self.new_objects or self.new_tasks

    @property
    def cluster_changed(self):
        return self.new_workers or self.network_update


class SchedulerBase(SchedulerInterface):

    PROTOCOL_VERSION = 0

    _simulator = None  # If running in simulator, this variable is filled before calling start()
                       # Only for testing purpose, scheduler should not depends on this variable

    _disable_cleanup = False

    def __init__(self, name, version, reassigning=False):
        self.workers = {}
        self.task_graph = SchedulerTaskGraph()
        self._name = name
        self._version = version
        self.network_bandwidth = None
        self.assignments = None
        self.reassigning = reassigning


    def send_message(self, message):
        message_type = message["type"]
        if message_type == "update":
            return self._process_update(message)
        else:
            raise Exception("Unkown message type: '{}'".format(message_type))

    def start(self):
        return {
            "type": "register",
            "protocol_version": self.PROTOCOL_VERSION,
            "scheduler_name": self._name,
            "scheduler_version": self._version,
            "reassigning": self.reassigning
        }

    def schedule(self, update):
        raise NotImplementedError()

    def _process_update(self, message):

        task_graph = self.task_graph
        workers = self.workers

        ready_tasks = []
        finished_tasks = []

        if message.get("new_workers"):
            new_workers = []
            for w in message["new_workers"]:
                worker_id = w["id"]
                if worker_id in workers:
                    raise Exception(
                        "Registering already registered worker '{}'".format(worker_id))
                worker = SchedulerWorker(worker_id, w["cpus"])
                new_workers.append(worker)
                workers[worker_id] = worker
        else:
            new_workers = ()

        network_update = False
        if "network_bandwidth" in message:
            bandwidth = message["network_bandwidth"]
            if bandwidth != self.network_bandwidth:
                network_update = True
                self.network_bandwidth = bandwidth

        if message.get("new_objects"):
            objects = self.task_graph.objects
            new_objects = []
            for o in message["new_objects"]:
                object_id = o["id"]
                obj = SchedulerDataObject(object_id, o["expected_size"], o.get("size"))
                new_objects.append(obj)
                objects[object_id] = obj
        else:
            new_objects = ()

        if message.get("new_tasks"):
            tasks = self.task_graph.tasks
            objects = self.task_graph.objects
            new_tasks = []
            for t in message["new_tasks"]:
                task_id = t["id"]
                inputs = [objects[o] for o in t["inputs"]]
                outputs = [objects[o] for o in t["outputs"]]
                task = SchedulerTask(
                    task_id,
                    inputs,
                    outputs,
                    t["expected_duration"],
                    t["cpus"])
                new_tasks.append(task)
                for o in outputs:
                    o.parent = task
                for o in inputs:
                    o.consumers.add(task)
                if task.unfinished_inputs == 0:
                    ready_tasks.append(task)
                tasks[task_id] = task
        else:
            new_tasks = ()

        for tu in message.get("tasks_update", ()):
            assert tu["state"] == TaskState.Finished
            task = task_graph.tasks[tu["id"]]
            task.state = TaskState.Finished
            task.computed_by = workers[tu["worker"]]
            finished_tasks.append(task)
            for o in task.outputs:
                for t in o.consumers:
                    t.unfinished_inputs -= 1
                    if t.unfinished_inputs <= 0:
                        assert t.unfinished_inputs == 0
                        ready_tasks.append(t)

        for ou in message.get("objects_update", ()):
            o = task_graph.objects[ou["id"]]
            o.placing = [workers[w] for w in ou["placing"]]
            o.availability = [workers[w] for w in ou["availability"]]
            size = ou.get("size")
            if size is not None:
                o.size = size

        self.assignments = {}

        print(new_tasks, message.get("new_tasks"))

        self.schedule(Update(
            new_workers,
            network_update,
            new_objects,
            new_tasks,
            ready_tasks,
            finished_tasks))

        return list(self.assignments.values())

    def assign(self, worker, task, priority=None, blocking=None):
        task.state = TaskState.Assigned
        task.scheduled_worker = worker

        for o in task.inputs:
            o.scheduled.add(worker)

        for o in task.outputs:
            o.scheduled.add(worker)

        result = {
            "worker": worker.worker_id if worker else None,
            "task": task.id,
        }
        if priority is not None:
            result["priority"] = priority
        if blocking is not None:
            result["blocking"] = blocking
        self.assignments[task] = result

    def stop(self):
        if self._disable_cleanup:
            return
        self.workers.clear()
        self.task_graph.tasks.clear()
        self.task_graph.objects.clear()
        self.network_bandwidth = None


class StaticScheduler(SchedulerBase):

    def schedule(self, update):
        if update.graph_changed or update.cluster_changed:
            return self.static_schedule()
        else:
            return ()

    def static_schedule(self):
        raise NotImplementedError()


class FixedScheduler(StaticScheduler):
    def __init__(self, schedules):
        super().__init__()
        self.schedules = schedules

    def static_schedule(self):
        return self.schedules


class TracingScheduler(SchedulerBase):
    def __init__(self, scheduler):
        self.scheduler = scheduler

    def init(self, simulator):
        self.schedules = []
        self.scheduler.init(simulator)

    def schedule(self, new_ready, new_finished):
        results = self.scheduler.schedule(new_ready, new_finished)
        self.schedules += results
        return results
