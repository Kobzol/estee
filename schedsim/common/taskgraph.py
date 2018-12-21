from .task import Task
from .utils import flat_list


class TaskGraph:
    """DAG of tasks and data objects that represents a computation.

    :param tasks: list of :class:`.Task`
    """

    def __init__(self, tasks=None):
        if tasks is None:
            self.tasks = []
            self.outputs = []
        else:
            outputs = []
            for i, task in enumerate(tasks):
                task.id = i
                for output in task.outputs:
                    output.id = len(outputs)
                    outputs.append(output)
            self.tasks = tasks
            self.outputs = outputs

    @staticmethod
    def merge(task_graphs):
        """Merges a list of task graphs into one task graph"""
        tasks = flat_list(tg._copy_tasks() for tg in task_graphs)
        return TaskGraph(tasks=tasks)

    @property
    def task_count(self):
        return len(self.tasks)

    @property
    def arcs(self):
        for task in self.tasks:
            for t in task.inputs:
                yield (task, t)

    def copy(self):
        return TaskGraph(tasks=self._copy_tasks())

    def new_task(self,
                 name=None,
                 outputs=(),
                 duration=1,
                 expected_duration=None,
                 cpus=1,
                 output_size=None):
        """Adds a new task to the graph"""
        task = Task(name, outputs, duration, cpus, output_size, expected_duration)
        task.id = len(self.tasks)

        output_id = len(self.outputs)
        for o in task.outputs:
            o.id = output_id
            output_id += 1

        self.tasks.append(task)
        self.outputs += task.outputs

        return task

    def remove_task(self, task):
        """Removes a task from the graph"""
        outputs = self.outputs
        for o in task.outputs:
            outputs.remove(o)
            for t in o.consumers:
                t.inputs.remove(o)
        self.tasks.remove(task)
        for o in task.inputs:
            o.consumers.remove(task)

    def source_tasks(self):
        """Returns the root tasks with no dependencies"""
        return [t for t in self.tasks if not t.inputs]

    def leaf_tasks(self):
        """Returns the tasks that have no dependent tasks"""
        return [t for t in self.tasks if t.is_leaf]

    def validate(self):
        tasks = set(self.tasks)
        outputs = set(self.outputs)

        for i, task in enumerate(self.tasks):
            assert task.id == i
            task.validate()

            for o in task.inputs:
                assert o in outputs
                assert o.parent in tasks

            for o in task.outputs:
                assert o in outputs
                assert o.parent is task
                for c in o.consumers:
                    assert c in tasks

    def normalize(self):
        for t in self.tasks:
            t.normalize()

    def to_dot(self, name):
        """Renders the graph to a DOT string"""
        stream = ["digraph ", name, " {\n"]

        for task in self.tasks:
            label = "{}\\n{}\\n{:.2f}".format(task.label, task.cpus, task.duration)
            stream.append("t{} [shape=oval,label=\"{}\"]\n".format(task.id, label))

        for output in self.outputs:
            label = "{}\\n{:.2f}".format(output.id, output.size)
            stream.append("o{} [shape=box,label=\"{}\"]\n".format(output.id, label))

        for task in self.tasks:
            for o in task.inputs:
                stream.append("o{} -> t{}\n".format(o.id, task.id))
            for o in task.outputs:
                stream.append("t{} -> o{}\n".format(task.id, o.id))

        stream.append("}\n")
        return "".join(stream)

    def write_dot(self, filename):
        """Writes the graph in the DOT format to a file"""
        dot = self.to_dot("g")
        with open(filename, "w") as f:
            f.write(dot)

    def __repr__(self):
        return "<TaskGraph #t={}>".format(len(self.tasks))

    def _copy_tasks(self):
        tasks = [task.simple_copy() for task in self.tasks]
        outputs = flat_list(task.outputs for task in tasks)

        for old_task, task in zip(self.tasks, tasks):
            for inp in old_task.inputs:
                task.add_input(outputs[inp.id])
        return tasks
