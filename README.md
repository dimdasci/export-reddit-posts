# export-reddit-posts

Based on [How to Use the Reddit API in Python](https://towardsdatascience.com/how-to-use-the-reddit-api-in-python-5e05ddfd1e5c) article by James Briggs.

## Installation

  make requirements

## Usage

  export_posts.py [OPTIONS] [SUBREDDITS]...

  Exports posts and comments of given subreddits

Options:
- `-n`, `--number` INTEGER — number of posts to export
- `-d`, `--days` INTEGER — number of days from now to to export posts
- `-f`, `--output-format` [`csv` or `parquet`] — an output file format: csv or parquet
- `-o`, `--output` TEXT — path to an output file
- `-l`, `--log` TEXT — path to a log file
-  `-c`, `--comments` — export comments to each post
- `--help` Show this message and exit
