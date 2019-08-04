# encoding=utf-8
# vim: sw=4 ts=4 expandtab ai
import sys
import os
import pickle
import pymysql.cursors
from pymysql.cursors import Cursor
import re
from datetime import datetime
import mwclient
import configparser
import logging


class ForceUnicodeCursor(Cursor):

    def _ensure_unicode_value(self, value):
        if isinstance(value, bytes) or isinstance(value, bytearray):
            return value.decode('utf-8')
        return value

    def _ensure_unicode(self, row):
        return [self._ensure_unicode_value(value) for value in row]

    def _do_get_result(self):
        conn = self._get_db()

        self._result = result = conn._result

        self.rowcount = result.affected_rows
        self.description = result.description
        self.lastrowid = result.insert_id
        self._rows = [self._ensure_unicode(row) for row in result.rows]


class MyFormatter(logging.Formatter):
    def format(self, record):
        record.relativeSecs = record.relativeCreated / 1000
        return super(MyFormatter, self).format(record)


logger = logging.getLogger()
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = MyFormatter("%(relativeSecs).1fs [%(levelname)s] %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)


    
def tprint(line):
    print(line, end='\r', flush=True)

def fprint(line):
    logger.info(line)

db = pymysql.connect(
    cursorclass=ForceUnicodeCursor,
    host='nowiki.labsdb',
    db='nowiki_p',
    read_default_file=os.path.expanduser('~/replica.my.cnf')
)

db.ping(True)


def get_config(fname):
    config = {}
    parser = configparser.ConfigParser()
    parser.read([fname])
    return parser


def get_subcats(cat_name, exceptions, hidden, path=[]):
    cats = [cat_name]
    with db.cursor() as cursor:
        cursor.execute(
            'SELECT page.page_title FROM page, categorylinks WHERE categorylinks.cl_to=%s '
            'AND categorylinks.cl_type="subcat" AND categorylinks.cl_from=page.page_id',
            [cat_name]
        )
        for row in cursor:
            subcat = row[0]
            # fprint(subcat)
            if subcat in hidden and subcat not in exceptions:
                if len(path) > 12:
                    fprint("WARN: cat path exceeds max depth: {}".format(' > '.join(path)))
                    # sys.exit(1)
                else:
                    subcat_members = get_subcats(subcat, exceptions, hidden, path + [cat_name])
                    cats.extend(subcat_members)
                    # fprint('%s: %d categories             ' % (subcat, len(subcat_members)), end='\r', flush=True)
                    #if len(path) < 1:
                    #    fprint('')
    return list(set(cats))


def get_memberpages(msg, patterns, truncate=True):
    # Returns a set of page IDs as ints, not namespace-filtered
    tprint(msg)
    if truncate:
        qargs = ['{}%'.format(x) for x in patterns]
        qstr = ' OR '.join(['cl_to LIKE %s' for x in patterns])
        qstr = 'SELECT cl_from FROM categorylinks WHERE ({}) AND cl_type="page" GROUP BY cl_from'.format(qstr)
    else:
        qargs = ['{}'.format(x) for x in patterns]
        qstr = 'cl_to IN ({})'.format( ('%s,' * len(patterns)).rstrip(','))
        qstr = 'SELECT cl_from FROM categorylinks WHERE ({}) AND cl_type="page" GROUP BY cl_from'.format(qstr)
    with db.cursor() as cursor:
        # fprint(qstr)
        # fprint(qargs)
        cursor.execute(qstr, qargs)
        pages = [row[0] for row in cursor]
    return list(set(pages))


def get_pagecats(msg, pageids):
    chunksize = 10000
    cats = {}
    tprint(msg)
    with db.cursor() as cursor:
        for n in range(0, len(pageids), chunksize):
            tp = pageids[n:n+chunksize]
            cursor.execute('SELECT cl_from, cl_to FROM categorylinks WHERE cl_from IN ({})'.format(('%s,' * len(tp)).rstrip(',')), tp)
            for row in cursor:
                if row[0] not in cats:
                    cats[row[0]] = []
                cats[row[0]].append(row[1])
            tprint(msg + ': %d of %d' % ( n + chunksize, len(pageids)))
    return cats


def get_hidden_cats():
    chunksize = 1000
    cats = []
    with db.cursor() as cursor:
        cursor.execute('SELECT page.page_title FROM page, page_props WHERE page.page_id=page_props.pp_page AND page.page_namespace=14 AND page_props.pp_propname="hiddencat"')
        for row in cursor:
            cats.append(row[0])
    return cats


def only_trivial_cats(cats, matcher, maintenance_cats):
    for cat in cats:
        if cat not in maintenance_cats and not matcher.match(cat):
            return False
    return True

def main(maintenance_cats):
    bio_cats = ['Personer_fra_', 'Fødsler_i_', 'Dødsfall_i_']
    ignorepages = 'Dødsfall_i_'

    pageids = get_memberpages('Getting list of biography pages', bio_cats)
    fprint('Found %s biographies' % len(pageids))

    pagecats = get_pagecats('Getting categories for the biography pages', pageids)
    fprint('Found cats for %s pages' % len(list(pagecats.keys())))

    tprint('Testing the biography pages against the list of maintenance categories')
    r1 = re.compile(r'(' + '|'.join(bio_cats) + ')')
    plist = []
    for pageid, cats in list(pagecats.items()):
        if only_trivial_cats(cats, r1, maintenance_cats):
            plist.append(pageid)
    fprint('Found %s biography pages lacking non-trivial categories' % len(plist))

    tprint('Getting titles for the matched pages')
    titles = []
    with db.cursor() as cursor:
        for page_id in plist:
            cursor.execute('SELECT page_title FROM page WHERE page_id=%s AND page_namespace=0 LIMIT 1', [int(page_id)])
            page = cursor.fetchone()
            if page is not None:
                page_title = page[0]
                if re.match(ignorepages, page_title) is None:
                    titles.append(page_title.replace('_', ' '))
    fprint('Found %s matching titles' % len(titles))

    return titles


def main2(maintenance_cats):
    pageids = get_memberpages('Getting list of category members for the maintenance cats', maintenance_cats, False)
    fprint('Found {} pages'.format(len(pageids)))

    pagecats = get_pagecats('Getting categories for the pages', pageids)
    fprint('Found cats for {} pages'.format(len(list(pagecats.keys()))))

    tprint('Testing %d pages' % len(pagecats.keys()))
    plist = []
    for pageid, cats in list(pagecats.items()):
        others = [x for x in cats if x not in maintenance_cats]
        if len(others) == 0:
            plist.append(pageid)

    tprint('Checking {} matching pages'.format(len(plist)))
    titles = []
    with db.cursor() as cursor:
        for page_id in plist:
            cursor.execute('SELECT page_title FROM page WHERE page_id=%s AND page_namespace=0 LIMIT 1', [int(page_id)])
            page = cursor.fetchone()
            if page is not None:
                page_title = page[0]
                titles.append(page_title.replace('_', ' '))

    fprint('Found {} matching titles'.format(len(titles)))
    return titles


def update_page(site, pagename, titles):
    if titles is None or len(titles) == 0:
        return
    page = site.pages[pagename]
    origtext = page.text()
    beginlistmarker = '<!--BegynnListe-->'
    p = origtext.find(beginlistmarker)
    if p == -1:
        raise Exception('Uh oh, begin list marker not found')
    titles = sorted(titles)
    text = '\n'.join('* [[{}]]'.format(title) for title in titles)
    dt = datetime.now().strftime('%Y-%m-%d')
    newtext = '{}{}{:d} sider (oppdatert {}):\n\n{}'.format(origtext[0:p], beginlistmarker, len(titles), dt, text)
    fprint('New text ready')
    # fprint(newtext)
    page.save(newtext, 'Bot: Oppdaterer liste')
    fprint('New text saved')


conf = get_config('config.cnf')
mwsite = dict(conf.items('site'))
mwoptions = dict(conf.items('options'))
site = mwclient.Site(**mwsite)

maintenance_exceptions = [
    'Mangler_interwiki',
    'Kategorier_som_trenger_diffusjon',
    'Artikler_som_bør_flettes',
    'Artikler_som_bør_flyttes',
    'Sider_som_er_foreslått_slettet',
    'Kategorier_som_trenger_diffusjon',
    'Kvinner',
    'Menn',
]

tprint('Getting list of hidden cats')
hidden_cats = get_hidden_cats()
fprint('Found %d hidden cats' % len(hidden_cats))

tprint('Getting list of maintenance categories')
maintenance_cats = get_subcats('Wikipedia-vedlikehold', maintenance_exceptions, hidden_cats)
fprint('Found %s maintenance categories' % len(maintenance_cats))

update_page(site, 'Wikipedia:Kategorifattige biografier', main(maintenance_cats))
update_page(site, 'Wikipedia:Artikler med kun vedlikeholdskategorier', main2(maintenance_cats))

fprint('Exiting')
