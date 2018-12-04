import random
from heapq import heappop, heappush

from deap import algorithms, base, creator
from deap.gp import tools

from schedsim.schedulers.utils import compute_b_level_duration_size, worker_estimate_earliest_time
from schedsim.worker.worker import RunningTask
from .scheduler import FixedScheduler, StaticScheduler, TracingScheduler
from ..communication import SimpleNetModel
from ..simulator import Simulator, TaskAssignment

creator.create("FitnessMin", base.Fitness, weights=(-1.0,))
creator.create("Individual", list, fitness=creator.FitnessMin)


class GeneticScheduler(StaticScheduler):
    """
    Scheduler using a genetic algorithm with operators described in
    Genetic algorithms for task scheduling problem (2010).

    :param bootstrap_scheduler Scheduler to be used as a baseline schedule for genetic individuals
    :param simulate_fitness_eval True if a full simulation should be used to evaluate fitness of
    individuals. Otherwise an estimate will be produced.
    """
    def __init__(self, bootstrap_scheduler=None, simulate_fitness_eval=False):
        self.bootstrap_scheduler = bootstrap_scheduler
        self.simulate_fitness_eval = simulate_fitness_eval

    def init(self, simulator):
        super().init(simulator)

        toolbox = base.Toolbox()

        graph = simulator.task_graph
        workers = simulator.workers

        if self.bootstrap_scheduler:
            generator = self.generator_individual_bootstrap(graph, workers,
                                                            self.simulator.netmodel,
                                                            self.bootstrap_scheduler)
        else:
            generator = self.generator_individual_alap(graph, workers, self.simulator.netmodel)

        toolbox.register("individual", tools.initIterate, creator.Individual, generator)
        toolbox.register("population", tools.initRepeat, list, toolbox.individual)

        def order_crossover(tasks1, tasks2):
            point = random.randint(0, graph.task_count - 1)
            tasks = tasks1[:point]
            visited = set(tasks)
            for t in tasks2:
                if t not in visited:
                    tasks.append(t)
            assert len(tasks) == len(tasks1)
            return tasks

        def mate(ind1, ind2):
            (mapping1, tasks1) = self.split_individual(ind1, graph.task_count)
            (mapping2, tasks2) = self.split_individual(ind2, graph.task_count)

            if random.random() < 0.5:
                (mapping1, mapping2) = tools.cxTwoPoint(mapping1, mapping2)
            else:
                (tasks1, tasks2) = (order_crossover(tasks1, tasks2),
                                    order_crossover(tasks2, tasks1))

            return (creator.Individual(mapping1 + tasks1), creator.Individual(mapping2 + tasks2))

        def mutate(individual):
            (mapping, tasks) = self.split_individual(individual, graph.task_count)

            tasks = tools.mutShuffleIndexes(tasks, indpb=0.1)
            mapping = tools.mutUniformInt(mapping, 0, len(workers) - 1, indpb=0.1)

            return (creator.Individual(mapping[0] + tasks[0]),)

        toolbox.register("evaluate", self.evaluate)
        toolbox.register("mate", mate)
        toolbox.register("mutate", mutate)
        toolbox.register("select", tools.selTournament, tournsize=3)

        pop = toolbox.population(n=50)
        hof = tools.HallOfFame(1)

        algorithms.eaSimple(pop, toolbox,
                            cxpb=0.8,
                            mutpb=0.05,
                            ngen=20,
                            halloffame=hof,
                            verbose=False)
        self.best_individual = hof.items[0]
        assert self.is_schedule_valid(self.best_individual, graph, workers)

    def generator_individual_bootstrap(self, graph, workers, netmodel, bootstrap):
        netmodel = SimpleNetModel(netmodel.bandwidth)
        new_graph = graph.copy()
        new_workers = [w.copy() for w in workers]

        tracer = TracingScheduler(bootstrap)
        Simulator(new_graph, new_workers, tracer, netmodel).run()

        def gen():
            tasks = [assignment.task.id for assignment in tracer.schedules]
            mapping = [0] * graph.task_count
            for assignment in tracer.schedules:
                mapping[assignment.task.id] = assignment.worker.id

            yield from mapping
            yield from tasks
        return gen

    def generator_individual_alap(self, graph, workers, netmodel):
        alap = compute_b_level_duration_size(None, graph, netmodel.bandwidth)

        def gen():
            yield from [random.randint(0, len(workers) - 1) for _ in range(graph.task_count)]
            yield from [t.id for t in sorted(graph.tasks[:], key=lambda t: alap[t])]
        return gen

    def evaluate(self, individual):
        graph = self.simulator.task_graph
        workers = self.simulator.workers
        netmodel = self.simulator.netmodel

        if not self.is_schedule_valid(individual, graph, workers):
            return 10e10,

        if self.simulate_fitness_eval:
            return self.simulate_individual(individual, graph, workers, netmodel),
        return estimate_schedule(self.create_schedule(individual, graph.tasks, workers),
                                 graph, netmodel),

    def simulate_individual(self, individual, graph, workers, netmodel):
        netmodel = SimpleNetModel(netmodel.bandwidth)
        new_graph = graph.copy()
        new_workers = [w.copy() for w in workers]

        schedule = self.create_schedule(individual, new_graph.tasks, new_workers)

        return Simulator(new_graph, new_workers, FixedScheduler(schedule), netmodel).run()

    def is_schedule_valid(self, schedule, graph, workers):
        (mapping, tasks) = self.split_individual(schedule, graph.task_count)
        for t in tasks:
            if workers[mapping[t]].cpus < graph.tasks[t].cpus:
                return False
        return True

    def split_individual(self, individual, count):
        return individual[:count], individual[count:]

    def create_schedule(self, individual, graph_tasks, workers):
        (mapping, tasks) = self.split_individual(individual, len(graph_tasks))

        schedule = []
        for tid in tasks:
            task = graph_tasks[tid]
            worker = workers[mapping[tid]]
            assert worker.cpus >= task.cpus
            schedule.append(TaskAssignment(worker, task))
        return schedule

    def static_schedule(self):
        return self.create_schedule(self.best_individual, self.simulator.task_graph.tasks,
                                    self.simulator.workers)


def estimate_schedule(schedule, graph, netmodel):
    task_to_worker = {assignment.task: assignment.worker for assignment in schedule}
    finish_time = {t: 0 for t in graph.tasks}
    start_time = dict(finish_time)

    events = []
    index = 0
    for task in graph.tasks:
        if not task.inputs:
            heappush(events, (0, index, task, task_to_worker[task], "start"))
            index += 1

    finished = [False] * graph.task_count

    while events:
        (time, _, task, worker, type) = heappop(events)
        if type == "start":
            dta = (transfer_cost_parallel_finished(task_to_worker, worker, task) /
                   netmodel.bandwidth)
            rt = worker_estimate_earliest_time(worker, task, time)
            start_time[task] = time + max(rt, dta)
            finish_time[task] = start_time[task] + task.expected_duration
            worker.running_tasks[task] = RunningTask(task, start_time[task])
            heappush(events, (finish_time[task], index, task, worker, "end"))
            index += 1
        elif type == "end":
            del worker.running_tasks[task]
            finished[task.id] = True
            for consumer in task.consumers():
                if all([finished[input.parent.id] for input in consumer.inputs]):
                    heappush(events,
                             (time, index, consumer, task_to_worker[consumer], "start"))
                    index += 1

    return max(finish_time.values())


def transfer_cost_parallel_finished(task_to_worker, worker, task):
    return max((i.size for i in task.inputs
                if worker != task_to_worker[i.parent]),
               default=0)
