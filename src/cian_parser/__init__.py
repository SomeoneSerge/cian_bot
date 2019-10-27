import collections
import copy
import itertools
import json
import pprint
import re
import logging
from urllib.parse import urljoin, urlparse

import attr
import requests

import pyjsparser
from bs4 import BeautifulSoup

logger = logging.getLogger('cian_bot.cian_parser')

OFFER_ID_PATTERN = re.compile(r'\bID (?P<id>[a-zA-Z0-9]+)\b')
EXAMPLE_URL = 'https://www.cian.ru/cat.php?deal_type=rent&maxprice={maxprice}&engine_version=2&foot_min=45&metro%5B0%5D=54&metro%5B10%5D=132&metro%5B11%5D=145&metro%5B12%5D=148&metro%5B13%5D=149&metro%5B14%5D=237&metro%5B1%5D=58&metro%5B2%5D=68&metro%5B3%5D=71&metro%5B4%5D=78&metro%5B5%5D=103&metro%5B6%5D=105&metro%5B7%5D=119&metro%5B8%5D=121&metro%5B9%5D=130&offer_type=flat&only_foot=2&room1=1&room2=1&room3=1&room4=1&room5=1&room6=1&type=4&p={page}'
BASE_URL = 'https://www.cian.ru/cat.php'

URL_DEFAULTS = dict(
    deal_type='rent',
    maxprice=1_000_000,
    engine_version=2,
    foot_min=20,
    offer_type='flat',
    room2=1,
    room3=1,
    minprice=15_000,
    # room4=1,
    # room5=1,
    # room6=1,
    sort='price_object_order',
    # metro=[
    #     '47', '121', '130', '132', '145', '148', '149', '150', '237', '54',
    #     '58', '64', '68', '71', '78', '103', '105', '119'],
    p=1)
URL_DEFAULTS.update({
    'metro[0]': '47',
    'metro[10]': '121',
    'metro[11]': '130',
    'metro[12]': '132',
    'metro[13]': '145',
    'metro[14]': '148',
    'metro[15]': '149',
    'metro[16]': '150',
    'metro[17]': '237',
    'metro[1]': '54',
    'metro[2]': '58',
    'metro[3]': '64',
    'metro[4]': '68',
    'metro[5]': '71',
    'metro[6]': '78',
    'metro[7]': '103',
    'metro[8]': '105',
    'metro[9]': '119'
})

# Within the ring only
# URL_DEFAULTS.update({
#     'metro[0]': '4',
#     'metro[10]': '54',
#     'metro[11]': '56',
#     'metro[12]': '58',
#     'metro[13]': '61',
#     'metro[14]': '64',
#     'metro[15]': '66',
#     'metro[16]': '68',
#     'metro[17]': '71',
#     'metro[18]': '77',
#     'metro[19]': '78',
#     'metro[1]': '8',
#     'metro[20]': '80',
#     'metro[21]': '84',
#     'metro[22]': '85',
#     'metro[23]': '86',
#     'metro[24]': '96',
#     'metro[25]': '98',
#     'metro[26]': '103',
#     'metro[27]': '105',
#     'metro[28]': '114',
#     'metro[29]': '115',
#     'metro[2]': '12',
#     'metro[30]': '119',
#     'metro[31]': '121',
#     'metro[32]': '123',
#     'metro[33]': '124',
#     'metro[34]': '125',
#     'metro[35]': '129',
#     'metro[36]': '130',
#     'metro[37]': '132',
#     'metro[38]': '145',
#     'metro[39]': '148',
#     'metro[3]': '15',
#     'metro[40]': '149',
#     'metro[41]': '150',
#     'metro[42]': '159',
#     'metro[4]': '18',
#     'metro[5]': '20',
#     'metro[6]': '38',
#     'metro[7]': '46',
#     'metro[8]': '47',
#     'metro[9]': '50'
# })


def offer_container_to_id_href(offer_soup):
    hrefs = offer_soup.find_all('a', class_=True)
    for h in hrefs:
        if not any('header' in c for c in h.attrs['class']):
            continue
        if not urlparse(h.attrs['href']).path.startswith('/rent/flat/'):
            continue
        href = h.attrs['href']
        id = urlparse(h.attrs['href']).path.split('/')[-2]
        return id, href


def offer_container_to_metro(offer_soup):
    divs = offer_soup.find_all('div', class_=True)
    divs = [
        div.text for div in divs
        if any('underground-name' in c for c in div.attrs['class'])
    ]
    return divs


@attr.s
class FlatListItem:
    id = attr.ib(type=int)
    href = attr.ib()
    pdf_link = attr.ib()
    price = attr.ib(type=float)
    deposit = attr.ib(type=float)
    fee = attr.ib(type=float)
    bonus = attr.ib()
    metros = attr.ib(type=list)
    rooms = attr.ib(type=int)
    bedrooms = attr.ib(type=int)
    description = attr.ib()
    address = attr.ib()
    photos = attr.ib()
    json = attr.ib()


def get_params(**params):
    pp = copy.deepcopy(URL_DEFAULTS)
    pp = pp.update(params)
    return pp


def get_flatlist_html(req, page, maxprice):
    res = req.get(BASE_URL, params=get_params(p=page, maxprice=maxprice))
    logger.debug(f'Finished querying {res.url}. Status {res.status_code}')
    res = res.text
    return res


def get_flatlist(html):
    res = BeautifulSoup(html, 'lxml')
    js = pyjsparser.parse(
        next(s for s in res.find_all('script') if '"priceRur"' in s.text).text)
    offers = next(o['value'] for t, o in js_traverse(js)
                  if t == 'Property' and o['key']['value'] == 'offers')
    offers = [js_parse_object_expression(o) for o in offers['elements']]
    return [
        FlatListItem(
            int(o['id']), o['fullUrl'],
            urljoin('https://cian.ru/export/pdf/',
                    urlparse(o['fullUrl']).path[1:]),
            (o['bargainTerms']['priceRur'] if o['bargainTerms']['priceRur'] >
             5000 else o['bargainTerms']['priceRur'] * 65),
            o['bargainTerms']['deposit'], o['bargainTerms']['clientFee'],
            (o['bargainTerms'].get('agentBonus', 0) or 0),
            [ug['name'] for ug in o['geo']['undergrounds']],
            int(o['roomsCount'] or 1),
            int(o['bedroomsCount']
                or max(1,
                       int(o['roomsCount'] or 0) - 1)),
            o['description'],
            o['geo']['userInput'],
            [p['fullUrl'] for p in o['photos']],
            o)
        for o in offers
    ]


@attr.s
class Flat:
    offer_id = attr.ib()
    price = attr.ib()
    gist = attr.ib()
    address = attr.ib()
    text = attr.ib()
    feats = attr.ib(type=list)
    add_feats = attr.ib(type=dict)
    numbers = attr.ib(type=dict)
    text = attr.ib()

def js_offer_to_phones(js):
    try:
        return [f'+{o["countryCode"]}{o["number"]}' for o in js['phones']]
    except Exception as e:
        logger.error("js_offer_to_phones: error {e} while extracting phones from: {}".format(pprint.pformat(js)))
        return list()


def js_is_node(subtree):
    return isinstance(subtree, dict) and 'type' in subtree


def js_traverse(js, filter=None):
    q = collections.deque()
    if filter is None or not filter(js):
        q.append(js)
    while len(q) > 0:
        # r = q.popleft()
        r = q.pop()
        yield r['type'], r
        for child_name, child in r.items():
            if filter is not None and filter(child):
                continue
            if js_is_node(child):
                q.append(child)
            elif isinstance(child, list):
                for item in child:
                    if js_is_node(item):
                        q.append(item)


def js_parse_object_expression(expr):
    if expr['type'] == 'Literal':
        return expr['value']
    elif expr['type'] == 'ArrayExpression':
        return [js_parse_object_expression(e) for e in expr['elements']]
    elif expr['type'] == 'ObjectExpression':
        return {
            p['key']['value']: js_parse_object_expression(p['value'])
            for p in expr['properties']
        }
    else:
        raise Exception(
            f'Unknown expression type: {expr["type"]}. Key={expr["key"]["value"]}'
        )


def js_findall_offer_data(js):
    for t, r in traverse(js):
        if t != 'Property': continue
        if r['key']['type'] != 'Literal': continue
        if r['key']['value'] != 'offerData': continue
        offer = js_parse_object_expression(r['value'])
        offer['offer']['id'] = int(offer['offer']['id'])
        yield offer


def get_flats(html):
    page = BeautifulSoup(html, 'lxml')
    js = next(s for s in page.find_all('script') if '"offerId"' in s.text).text
    js = pyjsparser.parse(js)
    yield from js_findall_offer_data(js)


def _get_flats(url, save_file):
    result = []
    new_flats = _get_new(url, save_file)
    if new_flats:
        for i, flat in enumerate(new_flats):
            agency = 'Agency' if flat['owner'] == False else 'Owner'
            price = flat['price']
            link = flat['url']
