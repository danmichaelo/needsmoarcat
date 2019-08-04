# encoding=utf-8
# vim: sw=4 ts=4 expandtab ai
import itertools
import os
import re
from datetime import datetime
import configparser
import logging

import pymysql.cursors
from pymysql.cursors import SSCursor
import mwclient
from tqdm import tqdm


# ---------------------------------------------------------------------------------------
# Bootstrapping


class ForceUnicodeCursor(SSCursor):

    @staticmethod
    def _ensure_unicode_value(value):
        if isinstance(value, bytes) or isinstance(value, bytearray):
            return value.decode('utf-8')
        return value

    def _conv_row(self, row):
        if row is None:
            return None
        return [self._ensure_unicode_value(value) for value in row]


class MyFormatter(logging.Formatter):
    def format(self, record):
        record.relativeSecs = record.relativeCreated / 1000
        return super(MyFormatter, self).format(record)


def chunked_iterable(iterable, size):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, size))
        if not chunk:
            break
        yield chunk


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
    logger.info(line + '                                      ')


def get_config(fname):
    parser = configparser.ConfigParser()
    parser.read([fname])
    return parser


def dump_sorted(lines, filename):
    with open(filename, 'w') as fp:
        for line in sorted(list(lines)):
            fp.write('%s\n' % line)


# ---------------------------------------------------------------------------------------
# Database query functions


def get_hidden_categories(db, msg):
    # Get a list of all hidden categories
    tprint(msg)
    with db.cursor() as cursor:
        cursor.execute(
            "SELECT page.page_title FROM page, page_props"
            " WHERE page.page_id=page_props.pp_page AND page.page_namespace=14"
            " AND page_props.pp_propname='hiddencat'"
        )
        return set([row[0] for row in cursor])


def get_category_memberships(db, msg):
    # Get category memberships for all articles (pages with namespace 0)
    # Returns dict on the format {page_id: int: [page_title: str, category_names: set)
    out = {}
    tprint(msg)
    with tqdm(desc=msg) as pbar:
        with db.cursor() as cursor:
            cursor.execute(
                "SELECT cl.cl_from, page.page_title, cl.cl_to "
                " FROM categorylinks AS cl, page"
                " WHERE cl.cl_type='page'"
                " AND cl.cl_from=page.page_id"
                " AND page.page_namespace=0"
                # " LIMIT 10000"
            )
            for row in cursor:
                if row[0] not in out:
                    out[row[0]] = [row[1], set([row[2]])]
                else:
                    out[row[0]][1].add(row[2])
                pbar.update()
    return out


# ---------------------------------------------------------------------------------------
# Filter functions


def any_matching(cats, matcher):
    for cat in cats:
        if matcher.match(cat):
            return True
    return False


def all_from_set_or_matching(cats, hidden_categories, matcher):
    for cat in cats:
        if cat not in hidden_categories and not matcher.match(cat):
            return False
    return True


def all_from_set(page_categories, hidden_categories):
    for cat in page_categories:
        if cat not in hidden_categories:
            return False

    return True

# ---------------------------------------------


def check_page_categories(page_categories, hidden_categories):
    msg = 'Checking the %d pages' % len(page_categories.keys())
    tprint(msg)
    out = []
    for page_id in page_categories.keys():
        if all_from_set(page_categories[page_id], hidden_categories):
            out.append(page_id)
    return out


# ---------------------------------------------------------------------------------------
# Job functions


def kategorifattige_biografier(category_links, hidden_cats):
    # Wikipedia:Kategorifattige biografier

    cat_patterns = ['Personer_fra_', 'Fødsler_i_', 'Dødsfall_i_']
    matcher = re.compile(r'(' + '|'.join(cat_patterns) + ')')

    titles = set()
    for page_id, page in category_links.items():
        if any_matching(page[1], matcher) and all_from_set_or_matching(page[1], hidden_cats, matcher):
            titles.add(page[0])

    fprint('Wikipedia:Kategorifattige biografier: Found %d matching pages' % len(titles))
    dump_sorted(titles, 'kategorifattige_biografier.txt')

    return titles


def kun_vedlikeholdskategorier(category_links, hidden_cats):
    # Wikipedia:Artikler med kun vedlikeholdskategorier

    titles = set()
    for page_id, page in category_links.items():
        if all_from_set(page[1], hidden_cats):
            titles.add(page[0])

    fprint('Wikipedia:Artikler med kun vedlikeholdskategorier: Found %d matching pages' % len(titles))
    dump_sorted(titles, 'kun_vedlikeholdskategorier.txt')

    return titles


# ---------------------------------------------------------------------------------------
# Formatting and page updating

def update_page(site, pagename, titles):
    if titles is None or len(titles) == 0:
        return
    titles = [title.replace('_', ' ') for title in titles]
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
    # fprint(newtext)
    page.save(newtext, 'Bot: Oppdaterer liste')
    fprint('New text saved')

# ---------------------------------------------------------------------------------------
# Main


def main():

    conf = get_config('config.cnf')
    db_conf = dict(conf.items('db'))
    if 'port' in db_conf:
        db_conf['port'] = int(db_conf['port'])

    db = pymysql.connect(cursorclass=ForceUnicodeCursor, **db_conf)
    db.ping(True)

    site_conf = dict(conf.items('site'))
    site = mwclient.Site(**site_conf)

    hidden_cats = get_hidden_categories(db, 'Getting list of hidden categories')
    hidden_cats.remove('Artikler_som_bør_flettes')
    hidden_cats.remove('Sider_som_er_foreslått_slettet')
    dump_sorted(hidden_cats, 'hidden_cats.txt')
    fprint('Found %d hidden cats' % len(hidden_cats))

    category_links = get_category_memberships(db, 'Reading category memberships')
    fprint('Read category memberships for %d pages' % len(category_links))

    update_page(
        site,
        'Wikipedia:Kategorifattige biografier',
        kategorifattige_biografier(category_links, hidden_cats)
    )

    update_page(
        site,
        'Wikipedia:Artikler med kun vedlikeholdskategorier',
        kun_vedlikeholdskategorier(category_links, hidden_cats)
    )

    fprint('Exiting')


if __name__ == '__main__':
    main()


# ---------------------------------------------------------------------------------------
# Not in use anymore

# def get_subcats(cat_name, exceptions, hidden, path=[]):
#     cats = [cat_name]
#     with db.cursor() as cursor:
#         cursor.execute(
#             "SELECT page.page_title FROM page, categorylinks"
#             " WHERE categorylinks.cl_to=%s"
#             " AND categorylinks.cl_type='subcat'"
#             " AND categorylinks.cl_from=page.page_id",
#             [cat_name]
#         )
#         for row in cursor:
#             subcat = row[0]
#             # fprint(subcat)
#             if subcat in hidden and subcat not in exceptions:
#                 if len(path) > 12:
#                     fprint("WARN: cat path exceeds max depth: {}".format(' > '.join(path)))
#                 else:
#                     subcat_members = get_subcats(subcat, exceptions, hidden, path + [cat_name])
#                     cats.extend(subcat_members)
#     return list(set(cats))
#
#
# def get_page_categories(msg, page_ids):
#     chunksize = 10000
#     out = {}
#     tprint(msg)
#     cur = 0
#     tot = len(page_ids)
#     with db.cursor() as cursor:
#         for chunk in chunked_iterable(page_ids, chunksize):
#             cursor.execute(
#                 "SELECT cl_from, cl_to FROM categorylinks"
#                 " WHERE cl_from IN (%s)" % ('%s,' * len(chunk)).rstrip(','),
#                 chunk
#             )
#             for row in cursor:
#                 cur += 1
#                 if row[0] not in out:
#                     out[row[0]] = set()
#                 out[row[0]].add(row[1])
#             tprint(msg + ': %d of %d' % (cur, tot))
#     return out


# def get_page_titles(page_ids):
#     # Given a set of page IDs, fetch the corresponding page titles
#     chunksize = 100
#     tprint('Fetching page titles for %d pages' % len(page_ids))
#     titles = set()
#     with db.cursor() as cursor:
#         for chunk in chunked_iterable(page_ids, chunksize):
#             cursor.execute(
#                 'SELECT page_title FROM page WHERE page_id IN (%s)' % ('%s,' * len(chunk)).rstrip(','),
#                 chunk
#             )
#             titles.update([row[0] for row in cursor])
#
#     return titles
