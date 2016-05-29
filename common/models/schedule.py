import datetime
from crontab import CronTab
from util.config import Config, BaseConfig, ConfigField, DateTimeField


class ScheduleRule(BaseConfig, CronTab):
    def __init__(self, rule: str = '* * * * *', **kwargs):
        super().__init__(**kwargs)
        self.matchers = self._make_matchers(rule)
        self.crontab = rule

    def __str__(self):
        return self.crontab

    def to_json(self):
        return str(self)

    def from_json(self, rule_str: str, skip_unknown_fields=False):
        assert isinstance(rule_str, str), '{}: ScheduleRule can be constructed only from str'.format(self.path_to_node)
        self.matchers = self._make_matchers(rule_str)
        self.crontab = rule_str


class ScheduledGraph(Config):
    schedule = ScheduleRule()
    graph_name = ConfigField(type=str, required=True, default='')
    last_triggered = DateTimeField()
    schedule_created = DateTimeField()

    def init(self, graph_name: str, schedule: str):
        self.schedule.from_json(schedule)
        self.graph_name = graph_name
        self.schedule_created.set_to_now()
        self.verify()
        return self

    def should_be_triggered(self) -> bool:
        unix_now = DateTimeField.datetime_to_unixtime(datetime.datetime.utcnow())
        return max(self.last_triggered.to_json(), self.schedule_created.to_json()) < unix_now + self.schedule.previous()
