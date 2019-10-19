from telegram.ext import Updater
from cian_parser import get_flats
import datetime
import logging
import argparse

logger = logging.getLogger('cian_bot')
logger.setLevel(logging.DEBUG)

URL = 'https://www.cian.ru/cat.php?currency=2&deal_type=rent&engine_version=2' \
      '&foot_min=15&maxprice={maxprice}&metro%5B0%5D=77&metro%5B1%5D=85&metro%5B2%5D=129' \
      '&offer_type={offer_type}&only_foot=2&room1=1&room2=1&type=4'
SAVE_FILE = 'save.json'


if __name__ == '__main__':
    parser = argparse.ArgumentParser('cian_bot')
    parser.add_argument('--maxprice', default=1_000_000, type=int)
    parser.add_argument('--offer-type', default='flat')
    parser.add_argument('--token-file', default='.token')
    parser.add_argument('--user', default=[], action='append')

    args = parser.parse_args()
    url = URL.format(**args.__dict__)
    with open(args.token_file, 'r') as f:
        token = f.readline().strip()

    def update(bot, job):
        flats = get_flats(url, SAVE_FILE)
        user_ids = list(args.user)
        if flats:
            text = str(flats)
            for id in user_ids:
                bot.send_message(chat_id=id, text=text)
        else:
            for id in user_ids:
                bot.send_message(chat_id=id, text='Nothing new, sorry.')
                
    updater = Updater(token)
    job = updater.job_queue
    job.run_repeating(update, datetime.timedelta(minutes=30), 10)
    updater.start_polling()
    updater.idle()
