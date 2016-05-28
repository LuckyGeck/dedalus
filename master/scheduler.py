from master.backend import MasterBackend
from master.engine import Engine
from threading import Thread, Event


class Scheduler(Thread):
    def __init__(self, backend: MasterBackend, engine: Engine):
        super().__init__(name='dedalus-master-scheduler')
        self.backend = backend
        self.engine = engine
        self.need_stop = Event()
        self.start()

    def schedule_graph(self, graph_name: str, schedule_rule: str):
        self.backend.write_schedule(graph_name, schedule_rule)

    def shutdown(self):
        self.need_stop.set()
        self.join()

    def run(self):
        while not self.need_stop.is_set():
            for schedule in self.backend.list_schedules():
                if schedule.should_be_triggered():
                    pass # self.engine.aa
            self.need_stop.wait(timeout=1)
