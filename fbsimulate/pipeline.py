import inspect
from datetime import timedelta

from prefect import Task, Flow
from prefect.schedules import IntervalSchedule, Schedule
from prefect.schedules.clocks import CronClock


class SimulatePipeline(object):

    def __init__(self, name):
        self._name = name
        self._flow = Flow(self._name)
        self._state = None
        self._tasks = {}

    def execute(self, **kwargs):
        schedule = None

        freq = kwargs.get('frequency', None)
        cron = kwargs.get('cron', None)
        if freq:
            schedule = IntervalSchedule(interval=timedelta(seconds=freq))
        elif cron:
            schedule = Schedule(clocks=[CronClock(cron)])

        self._flow.schedule = schedule
        self._state = self._flow.run()

    def register(self, task_instance, depends_on=None):

        self._tasks[task_instance.name] = task_instance
        self._flow.add_task(task_instance)

        if depends_on:
            if not isinstance(depends_on, list):
                raise RuntimeError('depends_on must be a list of tasks for upstream task outputs')

            depends_tasks = {}
            func = getattr(task_instance, 'execute')
            for arg, task in zip(inspect.signature(func).parameters.keys(), depends_on):
                task_name = task.name
                if task_name not in self._tasks:
                    raise ValueError(f'Invalid task_id {task_name}')

                depends_tasks[arg] = self._tasks[task_name]

            self._flow.set_dependencies(task_instance, keyword_tasks=depends_tasks)

    def get_output(self, task):
        if isinstance(task, Task):
            task = task.name
        return self._state.result[self._tasks[task]]._result.value

    def visualize(self):
        return self._flow.visualize()
