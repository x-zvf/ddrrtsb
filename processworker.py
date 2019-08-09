import redis, requests, time, sqlalchemy
from rq import Worker, Queue, Connection
from db import *

listen = ['default']

rdb = redis.Redis()

rant_queue = Queue("rants", connection=rdb, is_async=True)
comment_queue = Queue("comments", connection=rdb, is_async=True)
new_queue = Queue("new", connection=rdb, is_async=True)
standard_queue = Queue("standard", connection=rdb)

is_async_fetch_all = False
is_async_check_rant = False
is_async_process_comments = False
is_async_fetch_comments = False
is_async_fetch_content = False

api_base_path = "https://devrant.com/api"

profile_fields = [
    {"name": "rants", "ppf": "process_rant", "args": [False]},
    {"name": "comments", "ppf": "process_comment", "args": [False]},
    {"name": "upvoted", "ppf": "process_rant", "args": [True]},
    {"name": "favorites", "ppf": "process_rant", "args": [True]}
]

profile_fields_no_rfetch = [
    {"name": "rants", "ppf": "process_rant", "args": [False, True, False, False, False]},
    {"name": "comments", "ppf": "process_comment", "args": [True, True, False, False, False]},
    {"name": "upvoted", "ppf": "process_rant", "args": [False, True, False, False, False]},
    {"name": "favorites", "ppf": "process_rant", "args": [False, True, False, False, False]}
]

fn_queues = {
    "process_rant":rant_queue,
    "process_comment":comment_queue,
    "process_comments_from_rant": comment_queue,
    "fetch_all_from": new_queue,
    "create_user_content": new_queue,
    "create_user": new_queue,
    "process_profile": standard_queue,
}

def get_queue(f):
    if f not in fn_queues:
        return standard_queue
    return fn_queues[f]


def wait_for_api_rate_limit():
    maxreq = rdb.get("maxreq")
    if maxreq is None:
        maxreq = 10
        rdb.set("maxreq", 10)
    else:
        maxreq = int(maxreq.decode())
    while True:
        nreq = rdb.incr("nreq")
        if nreq == 1:
            rdb.expire("nreq", 1)
        if nreq <= maxreq:
            break
        print("Waiting 50ms for API rate limit to clear.")
        time.sleep(0.05)


def get_request(subpath, parameters, postprocess_with=None, postprocess_args=[]):
    print("working on request for: %s" % subpath)
    url = api_base_path + subpath
    c = True
    fails = 0

    q = None

    if postprocess_with is not None:
        if postprocess_with not in globals():
            print("Can't find postprocess_with local function.")
            print("globals: " + str(globals()))
            return

        q = get_queue(postprocess_with)
        postprocess_function = globals()[postprocess_with]

    while fails < 4:
        print("request")
        wait_for_api_rate_limit()
        result = requests.get(url, params=parameters)
        if result.status_code > 299:
            print("Request failed.")
            fails += 1
            return
        rj = result.json()

        if postprocess_with is not None:
            q.enqueue(postprocess_function, rj, *postprocess_args)
        else:
            return rj
        break


def fetch_all_from(subpath, parameters, postprocess_with, skip=0, encapsulators=[], postprocess_args=[]):
    print("fetching all from subpath: %s, params=%s" % (subpath, str(parameters)))
    url = api_base_path + subpath
    c = True
    fails = 0

    if postprocess_with not in globals():
        print("Can't find postprocess_with local function.")
        print("globals: " + str(globals()))
        return

    q = get_queue(postprocess_with)

    postprocess_function = globals()[postprocess_with]

    while c and fails < 4:
        print("request with skip=%d" % skip)
        wait_for_api_rate_limit()
        parameters["skip"] = skip
        result = requests.get(url, params=parameters)
        if result.status_code > 299:
            print("Request failed.")
            fails += 1
            return
        rj = result.json()
        if not rj["success"]:
            print("Result success not True")
            return
        r = rj
        for e in encapsulators:
            r = r[e]

        if type(r) is not list:
            print("decapsulated request is not a list.")
            return

        for el in r:
            if is_async_fetch_all:
                q.enqueue(postprocess_function, el, *postprocess_args)
            else:
                postprocess_function(el, *postprocess_args)
        skip += 20
        fails = 0
        c = len(r) > 0

    print("finished task")


def create_user_content(user_id, fetch_profile=True, fetch_content=True, profile_flds=profile_fields):
    subpath = "/users/%d" % user_id

    if fetch_profile:
        new_queue.enqueue(get_request, subpath, {"app": 3}, "process_profile", [user_id, False])

    if fetch_content:
        for f in profile_flds:
            new_queue.enqueue(fetch_all_from, subpath, {
                "app": 3,
                "content": f["name"]
            }, f["ppf"], 0, ["profile", "content", "content"] + [f["name"]], f["args"])


def create_user(user_id, username, fetch_profile=True, fetch_content=True, profile_flds=profile_fields):
    r = con.execute(tbl_users.insert().values(user_id=user_id, username=username))
    assert r.inserted_primary_key == [user_id]
    create_user_content(user_id, fetch_profile, fetch_content, profile_flds)


def process_profile(profile, user_id, check_user=True, *args):
    if not profile["success"]:
        print("request not successful")
        return

    p = profile["profile"]
    username = p["username"]
    created_time = p["created_time"]
    avatar = p["avatar"]
    dpp = p["dpp"] is 1
    about = p["about"]
    github = p["github"]
    location = p["location"]
    score = p["score"]
    skills = p["skills"]
    website = p["website"]

    avatar_i = ""
    if "i" in avatar:
        avatar_i = avatar["i"]
    avatar_b = ""
    if "b" in avatar:
        avatar_b = avatar["b"]


    avatar_id = con.execute(select([tbl_avatars.c.avatar_id]).where(and_(tbl_avatars.c.i == avatar_i,
                                                                         tbl_avatars.c.b == avatar_b))).scalar()
    if avatar_id is None:
        r = con.execute(tbl_avatars.insert().values(b=avatar_b, i=avatar_i))
        avatar_id = r.inserted_primary_key

    if type(avatar_id) is list:
        avatar_id = avatar_id[0]

    if check_user and con.execute(select([func.count(tbl_users.c.user_id)])).scalar() == 0:
        create_user(user_id, username, False, False)

    con.execute(
        tbl_users.update()
            .values(
                username=username,
                created_time=created_time,
                avatar=avatar_id,
                dpp=dpp,
                about=about,
                github=github,
                location=location,
                score=score,
                skills=skills,
                website=website,
                last_fetched=int(time.time())
            ).where(tbl_users.c.user_id == user_id)
    )


def process_comment(comment, check_user=True, check_rant=True, update_if_exists=False, fetch_profile=True, fetch_content=True, *args):
    print("Processing rant: " + str(comment))
    comment_id = comment["id"]
    user_id = comment["user_id"]
    username = comment["user_username"]
    rant_id = comment["rant_id"]
    created_time = comment["created_time"]
    score = comment["score"]
    if "edited" in comment:
        edited = comment["edited"]
    else:
        edited = False

    attached_image = None
    attached_image_width = None
    attached_image_height = None
    if "attached_image" in comment:
        attached_image = comment["attached_image"]["url"]
        attached_image_width = comment["attached_image"]["width"]
        attached_image_height = comment["attached_image"]["height"]

    body = comment["body"]

    # check if comment already in DB
    if con.execute(select([func.count(tbl_comments.c.comment_id)]).where(tbl_comments.c.comment_id == comment_id)).scalar() != 0:
        print("comment id %d: already in db" % comment_id)
        if not update_if_exists:
            print("not updating it")
            return
        else:
            print("updating it.")
            con.execute(tbl_comments.update().values(
                score=score,
                attached_image_height=attached_image_height,
                attached_image_width=attached_image_width,
                attached_image=attached_image,
                edited=edited,
                body=body,
                last_fetched = int(time.time())
            ).where(tbl_comments.c.comment_id == comment_id))
            return

    # check if user with that userid exists, if not create it.
    if check_user and \
            con.execute(select(
            [func.count(tbl_users.c.user_id)]
            ).where(tbl_users.c.user_id == user_id)).scalar() == 0:
        create_user(user_id, username, fetch_profile, fetch_content)

    if check_rant and \
            con.execute(select([func.count(tbl_rants.c.rant_id)]).where(tbl_rants.c.rant_id == rant_id)).scalar() == 0:
        if is_async_check_rant:
            rant_queue.enqueue(get_request, "/devrant/rants/%d" % rant_id, {"app": 3}, "process_comments_from_rant", [True])
        else:
            process_comments_from_rant(get_request("/devrant/rants/%d" % rant_id, {"app": 3}), True)

        print("enqued the rant to fetch. The comment will be inserted when that rant is processed.")
        return

    # Everything OK for insert.
    r = con.execute(tbl_comments.insert().values(
        comment_id=comment_id,
        rant_id=rant_id,
        user_id=user_id,
        created_time=created_time,
        body=body,
        score=score,
        attached_image=attached_image,
        attached_image_width=attached_image_width,
        attached_image_height=attached_image_height,
        edited=False,
        last_fetched=int(time.time())
    ))
    assert r.inserted_primary_key == [comment_id]


def process_comments_from_rant(rant, process_rant_aswell=False,*args):
    if not rant["success"]:
        print("rant not fetched sucessfully")
        return
    if process_rant_aswell:
        if is_async_check_rant:
            rant_queue.enqueue(process_rant, rant["rant"], True, False, False, False, False)
        else:
            process_rant(rant["rant"], check_user=True, fetch_comments=False, fetch_profile=False, fetch_content=False)
    comments = rant["comments"]
    for c in comments:
        if is_async_process_comments:
            comment_queue.enqueue(process_comment, c, True, False)
        else:
            process_comment(c, True, False)


def process_rant(rant, check_user=True, fetch_comments=True, update_if_exists=False, fetch_profile=True, fetch_content=True, *args):
    print("Processing rant: " + str(rant))
    user_id = rant["user_id"]
    username = rant["user_username"]
    rant_id = rant["id"]
    tags = rant["tags"]

    created_time = rant["created_time"]
    attached_image = None
    attached_image_width = None
    attached_image_height = None
    if "attached_image" in rant:
        aimg = rant["attached_image"]
        if type(aimg) is dict:
            if "url" in aimg:
                attached_image = aimg["url"]
            if "height" in aimg:
                attached_image_height = aimg["height"]
            if "width" in aimg:
                attached_image_width = aimg["width"]
        elif type(aimg) is str:
            attached_image = aimg
    body = rant["text"]
    score = rant["score"]
    edited = rant["edited"]

    #check if rant already in DB
    if con.execute(select([func.count(tbl_rants.c.rant_id)]).where(tbl_rants.c.rant_id == rant_id)).scalar() != 0:
        print("rant id %d: already in db" % rant_id)
        if not update_if_exists:
            print("not updating it")
            return
        else:
            print("updating it.")
            con.execute(tbl_rants.update().values(
                tags=tags,
                score=score,
                attached_image=attached_image,
                edited=edited,
                body=body,
                last_fetched=int(time.time()),
                attached_image_width=attached_image_width,
                attached_image_height=attached_image_height
            ).where(tbl_rants.c.rant_id == rant_id))
            return

    # check if user with that userid exists, if not create it.
    if check_user \
            and con.execute(select([func.count(tbl_users.c.user_id)])
                                    .where(tbl_users.c.user_id == user_id)).scalar() == 0:
        create_user(user_id, username, fetch_profile, fetch_content)

    con.execute(tbl_rants.insert().values(
        user_id=user_id,
        rant_id=rant_id,
        created_time=created_time,
        body=body,
        tags=tags,
        score=score,
        attached_image=attached_image,
        edited=edited,
        last_fetched=int(time.time()),
        attached_image_width=attached_image_width,
        attached_image_height=attached_image_height
    ))
    if fetch_comments:
        if is_async_fetch_comments:
            comment_queue.enqueue(get_request, "/devrant/rants/%d" % rant_id, {"app": 3}, "process_comments_from_rant", [])
        else:
            process_comments_from_rant(get_request("/devrant/rants/%d" % rant_id, {"app": 3}))
