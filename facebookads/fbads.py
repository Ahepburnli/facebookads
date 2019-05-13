import os
import schedule
import time


def job():
    os.system("scrapy crawl fbads")


# schedule.every().day.at("00:00").do(job)
# schedule.every(15).minutes.do(job)

# schedule.every().hour.do(job)
schedule.every(12).hours.do(job)
#
while True:
    schedule.run_pending()
    time.sleep(1)

# 1.需要定时运行的函数job不应当是死循环类型的，也就是说，这个线程应该有一个执行完毕的出口。一是因为线程万一僵死，会是非常棘手的问题；二是下一次定时任务还会开启一个新的线程，执行次数多了就会演变成灾难。
# 2.如果schedule的时间间隔设置得比job执行的时间短，一样会线程堆积形成灾难，也就是说，我job的执行时间是1个小时，但是我定时任务设置的是5分钟一次，那就会一直堆积线程。
