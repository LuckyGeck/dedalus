import datetime
from crontab import CronTab
from util.config import Config, BaseConfig, ConfigField, DateTimeField


class ScheduleRule(BaseConfig, CronTab):
    def __init__(self, crontab: str):
        super().__init__(crontab)
        self.crontab = crontab

    def __str__(self):
        return self.crontab

    def to_json(self):
        return str(self)

    def from_json(self, rule_str: str, skip_unknown_fields=False, path_to_node: str = ''):
        path_to_node = self._prepare_path_to_node(path_to_node)
        assert isinstance(rule_str, str), '{}: ScheduleRule can be constructed only from str'.format(path_to_node)
        self.matchers = self._make_matchers(rule_str)
        self.crontab = rule_str


class ScheduledGraph(Config):
    schedule = ScheduleRule('* * * * *')
    graph_name = ConfigField(type=str, required=True, default='')
    last_triggered = DateTimeField(0)
    schedule_created = DateTimeField()

    def __init__(self, graph_name: str, schedule: str):
        super().__init__()
        self.schedule.from_json(schedule)
        self.graph_name = graph_name
        self.schedule_created.set_to_now()

    def should_be_triggered(self) -> bool:
        unix_now = DateTimeField.datetime_to_unixtime(datetime.datetime.utcnow())
        return max(self.last_triggered.to_json(), self.schedule_created.to_json()) < unix_now + self.schedule.previous()
