from sqlalchemy import *
from sqlalchemy.engine import Engine


def connect(user, password, db, host='localhost', port=5432):
    """Returns a connection and a metadata object"""
    # We connect with the help of the PostgreSQL URL
    url = 'postgresql://{}:{}@{}:{}/{}'
    url = url.format(user, password, host, port, db)

    # The return value of create_engine() is our connection object
    _con = create_engine(url, client_encoding='utf8')

    # We then bind the connection to MetaData()
    _meta = MetaData(bind=_con, reflect=False)

    return _con, _meta


con: Engine
con, meta = connect("ddrrtsb", "ddrrtsb", "drdata")

tbl_users = \
    Table("users", meta,
          Column("user_id", Integer, primary_key=True, autoincrement=False),
          Column("username", String, nullable=False),
          Column("score", Integer),
          Column("about", String),
          Column("location", String),
          Column("github", String),
          Column("website", String),
          Column("skills", String),
          Column("created_time", Integer),
          Column("dpp",  Boolean),
          Column("avatar", Integer, ForeignKey("avatars.avatar_id")),
          Column("last_fetched", Integer)
          )

tbl_avatars = \
    Table("avatars", meta,
          Column("avatar_id", Integer, primary_key=True, autoincrement=True),
          Column("i", String),
          Column("b", String)
          )

tbl_rants = \
    Table("rants", meta,
          Column("rant_id", Integer, primary_key=True, autoincrement=False),
          Column("user_id", Integer, ForeignKey("users.user_id"), nullable=False),
          Column("created_time", Integer),
          Column("body", Text),
          Column("tags", ARRAY(String)),
          Column("score", Integer),
          Column("attached_image", String),
          Column("attached_image_width", Integer),
          Column("attached_image_height", Integer),
          Column("edited", Boolean),
          Column("last_fetched", Integer)
          )

tbl_comments = \
    Table("comments", meta,
          Column("comment_id", Integer, primary_key=True, autoincrement=False),
          Column("rant_id", Integer, ForeignKey("rants.rant_id"), nullable=False),
          Column("user_id", Integer, ForeignKey("users.user_id"), nullable=False),
          Column("created_time", Integer),
          Column("body", TEXT),
          Column("score", Integer),
          Column("attached_image", String),
          Column("attached_image_width", Integer),
          Column("attached_image_height", Integer),
          Column("edited", Boolean),
          Column("last_fetched", Integer)
          )

tbl_users_checked = \
    Table("users_checked", meta,
          Column("user_id", Integer, primary_key=True),
          Column("at_time", Integer)
    )

meta.create_all(con)
