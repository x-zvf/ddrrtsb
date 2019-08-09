from rq import Connection, Queue
import redis, time, click

from db import *
from processworker import *

rdb = redis.Redis()

q = Queue(connection=rdb, is_async=True)


@click.command()
def fetch_recent_rants():
    q.enqueue(fetch_all_from, "/devrant/rants", {
        "app": 3,
        "sort": "recent",
        "limit": 20
    }, "process_rant", 0, ["rants"], [True, False, False, False, False])


@click.command()
def fetch_top_rants():
    q.enqueue(fetch_all_from, "/devrant/rants", {
        "app": 3,
        "sort": "top",
        "range": "all",
        "limit": 20
    }, "process_rant", 0, ["rants"], [True, False, False, False, False])


@click.command()
def fetch_user_profiles():
    print("fetching started.")
    while True:
        i = 0
        njobs = len(q.jobs)
        while njobs > 15:
            print("waiting for queue [%d] to be smaller [%d]" % (njobs, i))
            i+=1
            time.sleep(2)
            njobs = len(q.jobs)

        u = con.execute(select([tbl_users.c.user_id]).where(
            ~ exists().where(tbl_users_checked.c.user_id == tbl_users.c.user_id))).fetchone()

        if u is () or u is None:
            break
        user_id = u[0]
        print("enqueueing user id %d" % user_id)
        q.enqueue(create_user_content, user_id, True, True, profile_fields_no_rfetch)

        con.execute(tbl_users_checked.insert().values(user_id=user_id, at_time=int(time.time())))

@click.group()
def cli():
    pass

cli.add_command(fetch_recent_rants)
cli.add_command(fetch_top_rants)
cli.add_command(fetch_user_profiles)

if __name__ == '__main__':
    cli()

