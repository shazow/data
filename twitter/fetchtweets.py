#!/usr/bin/env python
# Download your tweets.
# Requires `tweepy`.

import tweepy
import json
import time
import logging


log = logging.getLogger(__name__)


api = tweepy.API(retry_count=3, retry_delay=5)


def prune_keys(d):
    "Remove extraneous fields (empty, redundant, unserializable)."
    prune_keys = ('author', 'user', '_api', 'created_at', 'retweeted_status', 'id')
    return dict((k, v) for k, v in d.iteritems() if k not in prune_keys and v and not k.endswith('_id'))


def serialize_status(s):
    "Return a JSONifiable dict. Removes extraneous fields."
    d = prune_keys(s.__dict__)
    d['created_at'] = time.mktime(s.created_at.timetuple())
    return d


def tweets_lookup_since(screen_name, tweet_id=None):
    "Stores results in memory until final return in order to reverse them chronologically."
    r = []
    for i, t in enumerate(tweepy.Cursor(api.user_timeline, screen_name=screen_name, count=200).items()):
        if tweet_id and t.id_str <= str(tweet_id):
            break

        r.append(serialize_status(t))
        log.info("[%d] Fetched (%d): %s", i, t.id, t.text)

    for status in reversed(r):
        yield status


def write_from_iterable(iter_tweets, out_fp):
    for status in iter_tweets:
        json.dump(status, out_fp, sort_keys=True)
        out_fp.write('\n')


def read_to_iterable(fp):
    for line in fp:
        yield json.loads(line)


def main(screen_name, out_filepath, tweet_id=None):
    if not tweet_id:
        try:
            # Find the last tweet
            line = None
            with open(out_filepath, 'r') as fp:
                for line in fp:
                    pass
                if line:
                    tweet_id = json.loads(line).get('id_str')
                    log.info("Last tweet_id found in %s: %s", out_filepath, tweet_id)
        except IOError:
            pass

    iter_tweets = tweets_lookup_since(screen_name, tweet_id)

    with open(out_filepath, 'a') as fp:
        write_from_iterable(iter_tweets, fp)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Download thy tweets.')

    parser.add_argument('screen_name', metavar='SCREEN_NAME',
                        help='Twitter account to fetch from.')

    parser.add_argument('out_filepath', metavar='JSONS_FILE', default='out.jsons', nargs='?',
                        help='File to resume writing to (will read the last tweet to set default of `tweet_id`). [default: %default]')

    parser.add_argument('tweet_id', metavar='TWEET_ID', type=int, default=None, nargs='?',
                        help='Start fetching after this tweet [default: last tweet in `out_filepath`]')

    parser.add_argument('--verbose', '-v', action='count')

    args = parser.parse_args()

    logging_level = logging.WARNING
    if args.verbose > 1:
        logging_level = logging.DEBUG
    elif args.verbose > 0:
        logging_level = logging.INFO

    logging.basicConfig(level=logging_level)

    main(args.screen_name, args.out_filepath, tweet_id=args.tweet_id)
