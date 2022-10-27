from curses.ascii import NUL
from wsgiref import headers
import click
import datetime
from pytz import timezone
from dotenv import load_dotenv
import os
import logging
import csv
import requests
import random
import time

TIMEZONE = timezone("UTC")


def save_to_csv(data: list, column_names: list, file_path: str) -> None:
    """Saves data in 2D-list to a CSV file with column_names"""

    try:
        with open(file_path, "w") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(column_names)
            for row in data:
                writer.writerow(row)
    except Exception as e:
        logging.error(f"Can't save data to {file_path}", e)


def setup_logging(logfile: str = "log.txt", loglevel: str = "DEBUG") -> None:
    """
    Sets up logging handlers and a format

    :param logfile:
    :param loglevel:
    """
    loglevel = getattr(logging, loglevel)

    logger = logging.getLogger()
    logger.setLevel(loglevel)
    fmt = (
        "%(asctime)s: %(levelname)s: %(filename)s: "
        + "%(funcName)s(): %(lineno)d: %(message)s"
    )
    formatter = logging.Formatter(fmt)

    fh = logging.FileHandler(logfile, encoding="utf-8")
    fh.setLevel(loglevel)
    fh.setFormatter(formatter)

    ch = logging.StreamHandler()
    ch.setLevel(loglevel)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)


def init_api():
    """Inits Reddit APIs and returns headers with auth token"""

    logging.info("Initializing API")

    load_dotenv()

    auth = requests.auth.HTTPBasicAuth(
        os.getenv("CLIENT_ID"), os.getenv("SECRET_TOKEN")
    )
    data = {
        "grant_type": "password",
        "username": os.getenv("USERNAME"),
        "password": os.getenv("PASSWORD"),
    }

    headers = {"User-Agent": os.getenv("USERAGENT")}
    res = requests.post(
        "https://www.reddit.com/api/v1/access_token",
        auth=auth,
        data=data,
        headers=headers,
    )

    # convert response to JSON and pull access_token value
    TOKEN = res.json()["access_token"]

    # add authorization to our headers dictionary
    headers = {**headers, **{"Authorization": f"bearer {TOKEN}"}}

    # while the token is valid (~2 hours) we just add headers=headers to our requests
    response = requests.get(
        "https://oauth.reddit.com/api/v1/me", headers=headers
    )
    logging.info(f"API is initialized with {response}")

    return headers


def parse_fields(kind: str, parent: str, message: list, fields: list) -> list:
    """Parse params of a row with post or comment data"""

    row = []
    for f in fields:
        if f == "kind":
            row.append(kind)
        elif f == "text":
            fname = "selftext" if kind == "post" else "body"
            row.append(message["data"].get(fname))
        elif f in message["data"]:
            if f == "parent_id":
                row.append(parent)
            elif f == "created_utc":
                row.append(
                    datetime.datetime.fromtimestamp(
                        message["data"][f], tz=TIMEZONE
                    )
                )
            else:
                row.append(message["data"][f])
        else:
            row.append(None)

    return row


def parse_comments(
    kind: str, parent: str, messages: list, fields: list, data: list
) -> list:
    """Parses messages and returns flat list"""

    for m in messages:
        row = parse_fields(kind=kind, parent=parent, message=m, fields=fields)
        data.append(row)

        if "replies" in m["data"] and len(m["data"]["replies"]) > 0:
            data = parse_comments(
                kind=kind,
                parent=parent,
                messages=m["data"]["replies"]["data"]["children"],
                fields=fields,
                data=data,
            )

    return data


def get_comments(
    subreddit: str, headers: dict, fields: list, post_id: str, data: list
) -> list:
    """Export 100 comments for a given post"""

    logging.info(f"Getting comments for r/{subreddit}/comments/{post_id}")
    url = f"https://oauth.reddit.com/r/{subreddit}/comments/{post_id}"

    response = requests.get(url, headers=headers, params={"limit": "100"})
    comments = response.json()

    if len(comments[1]["data"]["children"]) == 0:
        logging.info("Post has no comments")
        return data

    len_before = len(data)
    data = parse_comments(
        kind="comment",
        parent=post_id,
        messages=comments[1]["data"]["children"],
        fields=fields,
        data=data,
    )

    logging.info(f"Exported {len(data) - len_before} comments")

    return data


def get_posts(
    subreddit: str,
    headers: dict,
    fields: list,
    number: int = 50,
    comments: bool = False,
    data: list = None,
) -> list:
    """Exports number hot posts from subreddit with or without comments"""

    url = f"https://oauth.reddit.com/r/{subreddit}/hot"

    logging.info(f"Getting {number} posts for {url}")

    if data is None:
        logging.warn("Data was not given, set to an empty list")
        data = []

    number_to_load = number
    params = dict()

    while number_to_load > 0:
        params["limit"] = str(min(100, number_to_load))
        response = requests.get(url, headers=headers, params=params)
        logging.info(response)

        posts = response.json()["data"]["children"]
        for post in posts:
            row = parse_fields(
                kind="post", parent="", message=post, fields=fields
            )
            data.append(row)
            if comments:
                data = get_comments(
                    subreddit=subreddit,
                    headers=headers,
                    fields=fields,
                    post_id=post["data"]["id"],
                    data=data,
                )
                time.sleep(random.random() * 3 + random.random())

        if len(posts) < int(params["limit"]):
            number_to_load = 0
        else:
            number_to_load -= len(posts)
            params["after"] = posts[-1]["kind"] + "_" + posts[-1]["data"]["id"]
        logging.info(
            f"Exported {len(posts)} posts, rest {max(0, number_to_load)}"
        )

    return data


@click.command()
@click.argument("subreddits", type=click.STRING, nargs=-1)
@click.option(
    "-n", "--number", default=50, type=int, help="number of posts to export"
)
@click.option(
    "-c", "--comments", is_flag=True, help="export comments to each post"
)
def export_posts(subreddits: list, number: int, comments: bool) -> None:
    """Exports Number hot posts of given subreddits"""

    random.seed(15)

    fields = [
        "id",
        "kind",
        "parent_id",
        "subreddit",
        "author",
        "created_utc",
        "title",
        "text",
        "upvote_ratio",
        "ups",
        "downs",
        "crossposts",
        "link_flair_text",
        "url",
    ]

    setup_logging(logfile="data/log.txt", loglevel="INFO")

    logging.info(f"Starting export for subreddits {', '.join(subreddits)}")

    headers = init_api()

    posts = []
    for s in subreddits:
        posts = get_posts(
            s,
            headers=headers,
            fields=fields,
            data=posts,
            number=number,
            comments=comments,
        )
        time.sleep(random.random() * 5 + random.random() * 2)

    if len(posts) > 0:
        save_to_csv(posts, fields, "data/posts.csv")

    logging.info(f"Got {len(posts)} from subreddits {', '.join(subreddits)}")


if __name__ == "__main__":
    export_posts()
