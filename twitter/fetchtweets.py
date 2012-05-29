#!/usr/bin/env python
# Copyright 2012 Andrey Petrov <andrey.petrov@shazow.net>
#
# This module is released under the MIT License:
# http://www.opensource.org/licenses/mit-license.php

"""Download thy tweets. (Requires tweepy)"""

__version__ = '1.0'

import tweepy
import json
import time
import logging

log = logging.getLogger(__name__)

api = tweepy.API(retry_count=3, retry_delay=5)

KEY_BLACKLIST = set([
    'author', # Deprecated property?
    'user', # Assume all tweets are by the same user.
    '_api', # Unserializable property from tweepy.
    'retweeted_status', # I forget why I'm blacklisting this. I'm sure I had my reasons at the time. :P
    'id', # We use id_str instead.
])


def prune_keys(d, blacklist=KEY_BLACKLIST):
    "Remove extraneous fields (empty, redundant, unserializable)."
    return dict((k, v) for k, v in d.iteritems() if k not in blacklist and v and not k.endswith('_id'))


def serialize_status(s):
    "Return a JSONifiable dict. Removes extraneous fields."
    d = prune_keys(s.__dict__)
    d['created_at'] = time.mktime(s.created_at.timetuple()) # Override
    return d


def tweets_lookup_since(screen_name, tweet_id=None):
    "Stores results in memory until final return in order to reverse them chronologically."
    r = []
    for i, status in enumerate(tweepy.Cursor(api.user_timeline, screen_name=screen_name, count=200).items()):
        if tweet_id and status.id_str <= str(tweet_id):
            break

        r.append(serialize_status(status))
        log.info("[%d] Fetched (%d): %s", i, status.id, status.text)

    for status in reversed(r):
        yield status


def write_from_iterable(iter_tweets, out_fp):
    for status in iter_tweets:
        json.dump(status, out_fp, sort_keys=True)
        out_fp.write('\n')


def read_to_iterable(fp):
    for line in fp:
        yield json.loads(line)


def reprocess_fp(fp_in, fp_out):
    write_from_iterable((prune_keys(d) for d in read_to_iterable(fp_in)), fp_out)


def verify_integrity(fp):
    current_id = 0
    for i, d in enumerate(read_to_iterable(fp)):
        id = int(d['id_str'])
        if id < current_id:
            print "Out-of-order id (%d) on line: %d" % (id, i)
        current_id = id


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

    parser = argparse.ArgumentParser(description=__doc__.strip())

    parser.add_argument('screen_name', metavar='SCREEN_NAME',
                        help='Twitter account to fetch from.')

    parser.add_argument('out_filepath', metavar='JSONS_FILE', default='out.jsons', nargs='?',
                        help='File to resume writing to (will read the last tweet to set default of TWEET_ID). '
                             '[default: %(default)s]')

    parser.add_argument('tweet_id', metavar='TWEET_ID', type=int, default=None, nargs='?',
                        help='Start fetching after this tweet. '
                             '[default: last tweet in JSONS_FILE]')

    parser.add_argument('--verbose', '-v', action='count')

    args = parser.parse_args()

    logging_level = logging.WARNING
    if args.verbose > 1:
        logging_level = logging.DEBUG
    elif args.verbose > 0:
        logging_level = logging.INFO

    logging.basicConfig(level=logging_level)

    main(args.screen_name, args.out_filepath, tweet_id=args.tweet_id)
