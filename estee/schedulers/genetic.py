import random
from typing import Tuple

from deap import algorithms, base, creator
from deap.gp import tools

from .scheduler import StaticScheduler
from .utils import compute_b_level_duration_size, get_size_estimate, estimate_schedule
from ..simulator import TaskAssignment

creator.create("FitnessMin", base.Fitness, weights=(-1.0,))
creator.create("Individual", list, fitness=creator.FitnessMin)


class GeneticScheduler(StaticScheduler):
    """
    Scheduler using a genetic algorithm with operators described in
    Genetic algorithms for task scheduling problem (2010).
    """
    def __init__(self):
        super().__init__("genetic", 0)
        self.best_individual = ()

    def init(self):
        toolbox = base.Toolbox()

        graph = self.task_graph
        workers = self.workers

        if not graph.tasks or not workers:
            return

        generator = self.generator_individual_alap(graph, workers, self._simulator.netmodel)

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
        hof = tools.HallOfFame(5)

        algorithms.eaSimple(pop, toolbox,
                            cxpb=0.8,
                            mutpb=0.05,
                            ngen=100,
                            halloffame=hof,
                            verbose=False)
        best = [item for item in hof.items if self.is_schedule_valid(item, graph, workers)]
        if not best:
            def get_worker(task):
                return random.choice([w for w in workers.values() if w.cpus >= task.cpus])
            self.best_individual = [TaskAssignment(get_worker(t), t) for t in graph.tasks]
        else:
            self.best_individual = self.create_schedule(best[0], graph.tasks, workers)
        assert self.is_schedule_valid(self.best_individual, graph, workers)

    def generator_individual_alap(self, graph, workers, netmodel):
        alap = compute_b_level_duration_size(graph, get_size_estimate, netmodel.bandwidth)

        def gen():
            yield from [random.randint(0, len(workers) - 1) for _ in range(graph.task_count)]
            yield from [t.id for t in sorted(graph.tasks.values(), key=lambda t: alap[t])]
        return gen

    def evaluate(self, individual) -> Tuple[float]:
        graph = self.task_graph
        workers = self.workers
        netmodel = self._simulator.netmodel

        if not self.is_schedule_valid(individual, graph, workers):
            return 10e10,

        tasks = {id: task.simple_copy() for (id, task) in self.task_graph.tasks.items()}
        workers = {id: worker.simple_copy() for (id, worker) in self.workers.items()}

        return estimate_schedule(self.create_schedule(individual, tasks, workers), netmodel),

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
        self.init()
        for assignment in self.best_individual:
            self.assign(assignment.worker, assignment.task)
