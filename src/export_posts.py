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

TIMEZONE = timezone('UTC')

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
            os.getenv("CLIENT_ID"), 
            os.getenv("SECRET_TOKEN"))
    data = {'grant_type': 'password',
            'username': os.getenv("USERNAME"),
            'password': os.getenv("PASSWORD")}

    headers = {'User-Agent': os.getenv("USERAGENT")}
    res = requests.post('https://www.reddit.com/api/v1/access_token',
                    auth=auth, data=data, headers=headers)

    # convert response to JSON and pull access_token value
    TOKEN = res.json()['access_token']

    # add authorization to our headers dictionary
    headers = {**headers, **{'Authorization': f"bearer {TOKEN}"}}

    # while the token is valid (~2 hours) we just add headers=headers to our requests
    response = requests.get('https://oauth.reddit.com/api/v1/me', headers=headers)
    logging.info(f"API is initialized with {response}")

    return headers

def get_posts(subreddit: str, headers: dict, fields: list, data: list = None) -> list:
    """Imports 50 hot posts from subreddit"""

    url = f"https://oauth.reddit.com/r/{subreddit}/hot"

    logging.info(f"Getting posts for {url}") 

    if data is None: 
        logging.warn("Data was not given, set to an empty list")
        data = []

    response = requests.get(url, headers=headers, params={'limit': '50'})
    logging.info(response)

    for post in response.json()["data"]["children"]:
        row = []
        for f in fields:
            if f in post['data']:
                if f == 'created_utc':
                    row.append(datetime.datetime.fromtimestamp(post['data'][f], tz=TIMEZONE))
                else:
                    row.append(post['data'][f])
            else:
                row.append(None)

        data.append(row)
    
    logging.info(f"Exported {len(response.json()['data']['children'])} posts")
    return data
 

@click.command()
@click.argument("subreddits", type=click.STRING, nargs=-1)
def export_posts(subreddits) -> None:
    """Exports 50 hot posts of given subreddirts"""

    random.seed(15)
    
    fields = ['subreddit', 'author', 'created_utc', 
              'title', 'selftext', 'upvote_ratio', 'ups',
              'downs', 'crossposts', 'link_flair_text', 
               'id','kind', 'url' ]

    setup_logging(logfile="data/log.txt", loglevel="INFO")

    logging.info(f"Starting export for subreddits {', '.join(subreddits)}")
    
    headers = init_api()

    posts = []
    for s in subreddits:
        posts = get_posts(s, headers=headers, fields=fields, data=posts)
        time.sleep(random.random()*5 + random.random()*2)
        
    if len(posts) > 0:
        save_to_csv(posts, fields, "data/posts.csv")

    logging.info(f"Got {len(posts)} from subreddits {', '.join(subreddits)}")


if __name__ == "__main__":
    export_posts()