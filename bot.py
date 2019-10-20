import argparse
import collections
import datetime
import itertools
import json
import logging
import os
import os.path as osp

import requests

import cian_parser
from cian_parser import get_flats
from telegram.ext import CommandHandler, Updater

logger = logging.getLogger('cian_bot')
logger.setLevel(logging.DEBUG)

SAVE_FILE = 'save.json'


def price_per_person(flat):
    return flat.price / flat.n_rooms <= 35000


class CianBot:
    def __init__(self):
        self.flatlist = dict()
        self.flat_details = dict()
        self.viewed = collections.defaultdict(set)  # chat_id -> set[int]
        self.scheduled_messages = collections.deque()
        self.filters = []

    def save(self, basepath):
        if not osp.exists(basepath):
            os.makedirs(basepath)
        with open(osp.join(basepath, 'flatlist.json'), 'w') as f:
            json.dump(self.flatlist, f)
        with open(osp.join(basepath, 'flat_details.json'), 'w') as f:
            json.dump(self.flat_details, f)
        with open(osp.join(basepath, 'scheduled_messages.json'), 'w') as f:
            json.dump(list(self.scheduled_messages), f)
        with open(osp.join(basepath, 'user_viewed_flats.json'), 'w') as f:
            json.dump(list(self.viewed), f)

    @staticmethod
    def from_directory(basepath):
        self = CianBot()
        with open(osp.join(basepath, 'flatlist.json'), 'r') as f:
            self.flatlist.update(json.load(f))
        with open(osp.join(basepath, 'flat_details.json'), 'r') as f:
            self.flat_details.update(json.load(f))
        with open(osp.join(basepath, 'user_viewed_flats.json'), 'r') as f:
            self.viewed.update(json.load(f))
        with open(osp.join(basepath, 'scheduled_messages.json'), 'r') as f:
            self.scheduled_messages.extend(json.load(f))
        return self

    def start(self, update, context):
        self.viewed[update.message.chat_id]
        if 'job' not in context.chat_data:
            context.chat_data['job'] = context.job_queue.run_repeating

    def handle_new_flat(self, flat: cian_parser.FlatListItem):
        if flat.id not in self.flatlist:
            self.flatlist[flat.id] = flat
        if not all(f(flat) for f in self.filters):
            return
        text = '.\n'.join([
            f'{flat.pdf_link}',
            f'{flat.price}, deposit {flat.deposit}, fee {flat.fee}',
            f'{flat.metros}',
        ])
        for u in self.viewed:
            if flat.id in self.viewed[u]:
                continue
            msg = dict(chat_id=u, text=text)
            if len(flat.photos) > 0:
                msg['photo'] = flat.photos[0]
            self.scheduled_messages.append(msg)

    def send_messages(self, context):
        while len(self.scheduled_messages) > 0:
            msg = self.scheduled_messages.popleft()
            if 'photo' in msg:
                context.bot.send_photo(msg['chat_id'],
                                       msg['photo'],
                                       caption=msg['text'])
            else:
                context.bot.send_message(msg['chat_id'], msg['text'])

    def fetch_cian(self, context):
        for p in range(1, 4):
            with requests.Session() as s:
                html = cian_parser.get_flatlist_html(s, p, 300_000)
            flats = cian_parser.get_flatlist(html)
            for f in flats:
                self.handle_new_flat(f)
        self.send_messages(context)


if __name__ == '__main__':
    parser = argparse.ArgumentParser('cian_bot')
    parser.add_argument('--token-file', default='.token')
    parser.add_argument('--user', default=[], action='append')
    parser.add_argument('--state-dir', default='cian')

    args = parser.parse_args()
    with open(args.token_file, 'r') as f:
        token = f.readline().strip()

    updater = Updater(token, use_context=True)
    dp = updater.dispatcher
    dp.use_context = True
    if args.state_dir and osp.exists(args.state_dir):
        state = CianBot.from_directory(args.state_dir)
    else:
        state = CianBot()

    try:
        job = updater.job_queue
        job.run_repeating(state.fetch_cian, datetime.timedelta(minutes=30), 10)
        dp.add_handler(CommandHandler('start', state.start))
        updater.start_polling()
        updater.idle()
    finally:
        if args.state_dir is not None:
            if not osp.exists(args.state_dir):
                os.makedirs(args.state_dir)
            state.save(args.state_dir)
