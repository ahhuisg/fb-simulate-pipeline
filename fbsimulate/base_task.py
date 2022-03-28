import time

from prefect import Task


class BaseTask(Task):

    def run(self, **kwargs):
        start_time = time.time()
        result = self.execute(**kwargs)
        end_time = time.time()
        self.logger.info(f'{self.name} takes {end_time - start_time} seconds')
        return result

    def execute(self, **kwargs):
        raise NotImplementedError("execute function of a Task class must be implemented!")
