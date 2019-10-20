import argparse
import collections
import datetime
import itertools
import json
import logging
import os
import os.path as osp
import logging
import copy

import attr
import requests

import cian_parser
from cian_parser import get_flats
from telegram.ext import CommandHandler, Updater

logger = logging.getLogger('cian_bot')
logger.setLevel(logging.DEBUG)
log_to_file = logging.FileHandler('cian_bot.log')
log_to_file.setLevel(logging.DEBUG)
logger.addHandler(log_to_file)
log_to_stdout = logging.StreamHandler()
log_to_stdout.setLevel(logging.INFO)
logger.addHandler(log_to_stdout)

SAVE_FILE = 'save.json'

METRO = [
    'Достоевская', 'Проспект Мира', 'Сухаревская', 'Цветной бульвар',
    'Трубная', 'Чеховская', 'Пушкинская', 'Кузнецкий мост', 'Лубянка',
    'Чистые пруды', 'Красные Ворота', 'Тургеневская', 'Сретенский бульвар',
    'Китай-город', 'Чкаловская', 'Маяковская', 'Белорусская', 'Менделеевская',
    'Новослободская'
]
METRO = [m.lower() for m in METRO]
METRO_BLACKLIST = [
    'Электрозаводская',
    'Бауманская',
    'Солнцево',
]
METRO_BLACKLIST = [m.lower() for m in METRO_BLACKLIST]


def filter_price_per_person(flat):
    ppp = flat.price / flat.rooms
    if ppp > 35000:
        logger.debug(
            f'Flat {flat.id} failed price test: price={flat.price}, rooms={flat.rooms}'
        )
    return ppp <= 35000


def filter_metro(flat):
    ok = any(m.lower() in METRO
                 for m in flat.metros) and not any(m.lower() in METRO_BLACKLIST
                                                   for m in flat.metros)
    if not ok:
        logger.debug(
            f'Flat {flat.id} failed metro test. Metros: {flat.metros}')
    return ok


@attr.s
class CianStateSerializable:
    flatlist = attr.ib(type=dict)
    flat_details = attr.ib(type=dict)
    viewed = attr.ib(type=dict)
    scheduled_messages = attr.ib(type=list)


class CianBot:
    def __init__(self):
        self.flatlist = dict()
        self.flat_details = dict()
        self.viewed = collections.defaultdict(set)  # chat_id -> set[int]
        self.scheduled_messages = collections.deque()

    @property
    def filters(self):
        return [filter_price_per_person, filter_metro]

    def save(self, basepath):
        if not osp.exists(basepath):
            os.makedirs(basepath)
        with open(osp.join(basepath, 'state.json'), 'w') as f:
            json.dump(
                attr.asdict(
                    CianStateSerializable(flatlist=self.flatlist,
                                          flat_details=self.flat_details,
                                          viewed=self.viewed,
                                          scheduled_messages=list(
                                              self.scheduled_messages))), f)

    @staticmethod
    def from_directory(basepath):
        self = CianBot()
        with open(osp.join(basepath, 'state.json'), 'r') as f:
            state = json.load(f)
        self.flatlist.update(state['flatlist'])
        self.flat_details.update(state['flat_details'])
        self.viewed.update({a: set(b) for a, b in state['viewed'].items()})
        self.scheduled_messages.extend(state['scheduled_messages'])
        return self

    def start(self, update, context):
        self.viewed[update.message.chat_id] = set()
        logger.info(f'{update.message.chat_id} connected')

    def flat_to_message(self, flat):
        text = '.\n'.join([
            f'{flat.pdf_link}',
            ', '.join([
                f'{k} {getattr(flat, k.lower())}'
                for k in ['Price', 'deposit', 'fee', 'bonus']
                if getattr(flat, k.lower())
            ]),
            f'{flat.bedrooms} rooms',
            f'{flat.metros}',
        ])
        msg = dict(text=text)
        if len(flat.photos) > 0:
            msg['photo'] = flat.photos[0]
        return msg

    def flat_ok(self, flat):
        for f in self.filters:
            if not f(flat):
                logger.debug(f'Flat {flat.id} couldn\'t pass {f.__name__}.')
                return False
        return True

    def handle_new_flat(self, flat: cian_parser.FlatListItem):
        if flat.id in self.flatlist:
            return
        self.flatlist[flat.id] = flat
        if not self.flat_ok(flat):
            return
        msg = self.flat_to_message(flat)
        for u in self.viewed:
            if flat.id in self.viewed[u]:
                continue
            msg = copy.deepcopy(msg)
            msg['chat_id'] = u
            self.scheduled_messages.append(msg)
            self.viewed[u].add(flat.id)

    def send_messages(self, context):
        while len(self.scheduled_messages) > 0:
            logger.debug(f'Notifying {msg["chat_id"]} about: {msg["text"]}')
            msg = self.scheduled_messages.popleft()
            if 'photo' in msg:
                context.bot.send_photo(msg['chat_id'],
                                       msg['photo'],
                                       caption=msg['text'])
            else:
                context.bot.send_message(msg['chat_id'], msg['text'])

    def fetch_messages(self, update, context):
        logger.info(f'{update.message.chat_id} asks for messages')
        for f in self.flatlist:
            if f.id in self.viewed[update.message.chat_id]:
                continue
            if not self.flat_ok(flat):
                continue
            msg = self.flat_to_message(f)
            msg['chat_id'] = update.message.chat_id
            self.scheduled_messages.append(msg)
        logger.info('Sending messages as requested')
        self.send_messages()
        logger.info('Messages sent')

    def fetch_cian(self, context):
        logger.info('Fetching cian')
        for p in range(1, 20):
            with requests.Session() as s:
                html = cian_parser.get_flatlist_html(s, p, 100_000)
            flats = cian_parser.get_flatlist(html)
            for f in flats:
                self.handle_new_flat(f)
            logger.info(f'Fetched page {p}')
            logger.info('Sending messages after fetch')
            self.send_messages(context)
            logger.info('Messages sent')
        logger.info('Saving backup')
        self.save('.cian-backup')
        logger.info('Saved backup')
    def fetch_cian_url(self, context):
        with requests.Session() as s:
            html = s.get(url)
        flats = cian_parser.get_flatlist(html)
        for f in flats:
            self.handle_new_flat(f)
        logger.info(f'Fetched page {p}')
        logger.info('Sending messages after fetch')
        self.send_messages(context)
        logger.info('Messages sent')



if __name__ == '__main__':
    parser = argparse.ArgumentParser('cian_bot')
    parser.add_argument('--token-file', default='.token')
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
        job.run_repeating(state.fetch_cian, datetime.timedelta(minutes=180), 10)
        dp.add_handler(CommandHandler('start', state.start))
        dp.add_handler(CommandHandler('fetch', state.fetch_messages))
        updater.start_polling()
        updater.idle()
    finally:
        if args.state_dir is not None:
            if not osp.exists(args.state_dir):
                os.makedirs(args.state_dir)
            state.save(args.state_dir)
