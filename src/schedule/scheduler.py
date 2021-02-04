import sys, logging, time, uwsgi, traceback
from schedule.dcard_crawler          import dc
from apscheduler.schedulers.blocking import BlockingScheduler
from settings.environment            import app
cron_log = logging.getLogger()

mmm = 13
sss = 0

func_name_dict = {
    'dc.dcard_forums_crawler':{
        'args'   : '',
        'day'    : '*',
        # 'hour'   : '{}'.format(app.config['MACHINE_CONFIG']['CRAWLER_SPAN']['hours']),
        # 'minute' : 0,
        # 'second' : 0
        'hour'   : '*',
        'minute' : mmm,
        'second' : sss
        },
    'dc.dcard_article_crawler':{
        'args'   : '',
        'day'    : '*',
        # 'hour'   : '{}'.format(app.config['MACHINE_CONFIG']['CRAWLER_SPAN']['hours']),
        # 'minute' : 0,
        # 'second' : 0
        'hour'   : '*',
        'minute' : mmm,
        'second' : sss
        },
    'dc.dcard_comment_crawler':{
        'args'   : '',
        'day'    : '*',
        # 'hour'   : '{}'.format(app.config['MACHINE_CONFIG']['CRAWLER_SPAN']['hours']),
        # 'minute' : 0,
        # 'second' : 0
        'hour'   : '*',
        'minute' : mmm,
        'second' : sss
        }
}

def job_creator(scheduler, func_name, job_dict):
    job = {
        'id'     : 'scheduler:{}'.format(func_name),
        'func'   : '{}:{}'.format(__name__, func_name),
        'trigger': 'cron'
    }
    job.update(job_dict)
    scheduler.add_job(**job)

aihub_scheduler = BlockingScheduler()
for k, v in func_name_dict.items():
    job_creator(aihub_scheduler, k, v)
aihub_scheduler.start()

try:
    while True:
        sig = uwsgi.signal_wait()
        print(sig)
except Exception as err:
    pass