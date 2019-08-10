from rq import Connection, Queue
import redis, time, click

from db import *
from processworker import *

rdb = redis.Redis()


@click.command()
def fetch_recent_rants():
    new_queue.enqueue(fetch_all_from, "/devrant/rants", {
        "app": 3,
        "sort": "recent",
        "limit": 20
    }, "process_rant", 0, ["rants"], [True, False, False, False, False])


@click.command()
def fetch_top_rants():
    new_queue.enqueue(fetch_all_from, "/devrant/rants", {
        "app": 3,
        "sort": "top",
        "range": "all",
        "limit": 20
    }, "process_rant", 0, ["rants"], [True, False, False, False, False])


nj = lambda : rdb.llen("rq:queue:standard") + rdb.llen("rq:queue:comments") +rdb.llen("rq:queue:rants") +rdb.llen("rq:queue:new")

@click.command()
def fetch_user_profiles():
    print("fetching started.")
    while True:
        i = 0
        njobs = 2
        while njobs > 1:
            print("waiting for queue [%d] to be smaller [%d]" % (njobs, i))
            i+=1
            time.sleep(0.25)
            njobs = nj()

        u = con.execute(select([tbl_users.c.user_id]).where(and_(tbl_users.c.score == None, tbl_users.c.username != "/--DELETED--/"))).fetchone()

        user_id = u[0]
        con.execute(
        tbl_users.update()
            .values(
                username="/--FETCHING3--/",
            ).where(tbl_users.c.user_id == user_id))

        if u is () or u is None:
            break

        print("enqueueing user id %d" % user_id)
        new_queue.enqueue(create_user_content, user_id, True, True, profile_fields_no_rfetch)


@click.command()
def update_data():
    pass

@click.group()
def cli():
    pass

cli.add_command(fetch_recent_rants)
cli.add_command(fetch_top_rants)
cli.add_command(fetch_user_profiles)

if __name__ == '__main__':
    cli()

