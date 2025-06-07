from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

sched = BackgroundScheduler(timezone="Asia/Bangkok")
sched.start()

def schedule_daily_2am(func, cfg_id: int):
    trig = CronTrigger(hour=2, minute=0)
    sched.add_job(func, trig, args=[cfg_id],
                  id=f"cfg-{cfg_id}", replace_existing=True,
                  misfire_grace_time=3600)
