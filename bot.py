import argparse
import collections
import copy
import datetime
import hashlib
import io
import itertools
import json
import logging
import os
import os.path as osp
import pprint
from contextlib import ExitStack

import attr
import requests

import cian_parser
from cian_parser import get_flats
from telegram import InputFile, InputMediaPhoto
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
N_PHOTOS_MAX = 4

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


def fetch_file(url):
    # aye, it doesnt depend on basedir, i know
    for i in range(5):
        try:
            if not osp.exists('photos'):
                os.mkdir('photos')
            filename = hashlib.sha256(url.encode('utf8')).hexdigest()
            filename = osp.join('photos', filename)
            if not osp.exists(filename):
                res = requests.get(url)
                with open(filename, 'wb') as out:
                    for chunk in res.iter_content(chunk_size=1024):
                        out.write(chunk)
            return filename
        except Exception as e:
            logger.error(f'fetch_file: url={url} e={e}')


@attr.s
class CianStateSerializable:
    """CianStateSerializable:
    TODO: just add in fucking mongo you lazy hog, it'll be more usable and reusable"""
    flatlist = attr.ib(type=dict)
    flat_details = attr.ib(type=dict)
    viewed = attr.ib(type=dict)
    observed_urls = attr.ib(type=list)
    scheduled_messages = attr.ib(type=list)


class CianBot:
    def __init__(self):
        self.flatlist = dict()
        self.flat_details = dict()
        self.viewed = collections.defaultdict(set)  # chat_id -> set[int]
        self.scheduled_messages = collections.deque()
        self.observed_urls = list()

    @property
    def filters(self):
        return [filter_price_per_person, filter_metro]

    def save(self, basepath):
        if not osp.exists(basepath):
            os.makedirs(basepath)
        with open(osp.join(basepath, 'state.json'), 'w') as f:
            json.dump(
                attr.asdict(
                    CianStateSerializable(
                        flatlist=self.flatlist,
                        flat_details=self.flat_details,
                        viewed=self.viewed,
                        scheduled_messages=list(self.scheduled_messages),
                        observed_urls=self.observed_urls)), f)

    @staticmethod
    def from_directory(basepath):
        self = CianBot()
        with open(osp.join(basepath, 'state.json'), 'r') as f:
            state = json.load(f)
        self.flatlist.update(state['flatlist'])
        self.flat_details.update(state['flat_details'])
        self.viewed.update({a: set(b) for a, b in state['viewed'].items()})
        self.scheduled_messages.extend(state['scheduled_messages'])
        self.observed_urls.extend(state['observed_urls'])
        return self

    def start(self, update, context):
        self.viewed[update.message.chat_id] = set()
        logger.info(f'{update.message.chat_id} connected')

    def flat_to_message(self, flat):
        try:
            text = '.\n'.join([
                f'{flat.href}',
                ', '.join([
                    f'{k} {getattr(flat, k.lower())}'
                    for k in ['Price', 'deposit', 'fee', 'bonus']
                    if getattr(flat, k.lower())
                ]),
                f'{flat.bedrooms} rooms',
                f'{flat.metros}',
                f'{flat.address}',
                ' '.join(cian_parser.js_offer_to_phones(flat.json)),
            ])
            msg = dict(text=text)
            if len(flat.photos) > 0:
                msg['photos'] = flat.photos
                msg['photo'] = flat.photos[0]
                msg['document'] = flat.pdf_link
            return msg
        except Exception as e:
            logger.error(f'flat_to_msg: {e}')
            raise e

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
        logger.info(
            f'send_message: about to send {len(self.scheduled_messages)} messages'
        )
        while len(self.scheduled_messages) > 0:
            try:
                msg = self.scheduled_messages.popleft()
                logger.debug(
                    f'Notifying {msg["chat_id"]} about: {msg["text"]}')

                sent_msg = None
                # Aye, that's a ton of shitcode
                if 'photo' in msg:
                    sent_msg = context.bot.send_photo(msg['chat_id'],
                                                      msg['photo'],
                                                      caption=msg['text'])
                else:
                    sent_msg = context.bot.send_message(
                        msg['chat_id'], msg['text'])
            except Exception as e:
                logger.error(f'send_messages: {e}')
                self.scheduled_messages.append(msg)
            else:
                if 'document' in msg and sent_msg is not None:
                    sent_msg.reply_text(msg['document'])
                if 'photos' in msg and len(
                        msg['photos']) >= 2 and sent_msg is not None:
                    with ExitStack() as stack:
                        photos = msg['photos'][:N_PHOTOS_MAX]
                        photos = [fetch_file(p) for p in photos]
                        photos = [
                            stack.enter_context(open(p, 'rb')) for p in photos
                        ]
                        context.bot.send_media_group(
                            msg['chat_id'],
                            [InputMediaPhoto(p) for p in photos],
                            timeout=120 * len(photos),
                            reply_to_message_id=sent_msg.message_id)
                if sent_msg is None:
                    logger.error(
                        f'Failed to send message to {msg["chat_id"]} with content {msg["text"]}'
                    )

    def get_json(self, update, context):
        logger.info(f'get_json {context.args}')
        flatid = context.args[0]
        flatid = int(flatid)
        if not flatid in self.flatlist:
            pass  # TODO: schedule querying the flat page
        flat = self.flatlist[flatid]
        logger.debug(f'get_json {context.args}: flat found')
        js = flat.json
        logger.debug(f'get_json {context.args}: extracted json')
        js = pprint.pformat(js).encode('utf8')
        logger.debug(f'get_json {context.args}: encoded into bytes')
        doc = InputFile(input_file_content=js, filename=f'{flatid}.json')
        logger.debug(f'get_json {context.args}: created InputFile')
        update.message.reply_document(doc)
        logger.debug(f'get_json {context.args}: send a reply')

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
        if len(self.observed_urls) == 0:
            logger.info('fetch_cian: no URLs to fetch')
            return
        with requests.Session() as s:
            for url in self.observed_urls:
                try:
                    logger.info(f'fetch_cian: fetching {url}')
                    res = s.get(url)
                    logger.info(f'fetch_cian: status {res.status_code}')
                    html = res.text
                    flats = cian_parser.get_flatlist(html)
                    logger.info(
                        f'fetch_cian: fetched {len(flats)} flats from {url}')
                    for f in flats:
                        self.handle_new_flat(f)
                    self.send_messages(context)
                except Exception as e:
                    logger.fatal(
                        f'fetch_cian: failed fetching flats from {url}; error: {e}'
                    )
        logger.info('Saving backup')
        self.save('.cian-backup')
        logger.info('Saved backup')

    def observe_url(self, update, context):
        if len(context.args) != 1:
            update.message.reply('Synopsis: /observe https://cian.ru/...')
            logger.error(
                f'observe_url: invalid number of arguments; arguments are: {context.args}'
            )
            return
        url = context.args[0]
        self.observed_urls = sorted(set(self.observed_urls + [url]))
        logger.info('observe_url: scheduled cian_fetch')
        due = 5
        context.job_queue.run_once(self.fetch_cian,
                                   due,
                                   context=update.message.chat_id)
        update.message.reply(f'Observing {url}')
        logger.info(f'observe_url: Observing {url}')


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
        job.run_repeating(state.fetch_cian, datetime.timedelta(minutes=180),
                          10)
        dp.add_handler(CommandHandler('start', state.start))
        dp.add_handler(
            CommandHandler('observe',
                           state.observe_url,
                           pass_args=True,
                           pass_job_queue=True,
                           pass_chat_data=True))
        dp.add_handler(CommandHandler('fetchMessages', state.fetch_messages))
        dp.add_handler(
            CommandHandler('json',
                           state.get_json,
                           pass_args=True,
                           pass_job_queue=True,
                           pass_chat_data=True))
        updater.start_polling()
        updater.idle()
    finally:
        if args.state_dir is not None:
            if not osp.exists(args.state_dir):
                os.makedirs(args.state_dir)
            state.save(args.state_dir)
