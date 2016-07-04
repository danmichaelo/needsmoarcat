# encoding=utf-8
from __future__ import unicode_literals
from __future__ import print_function

import sys
import os
import pickle
import oursql
import re
from datetime import datetime
import mwclient
import ConfigParser

db = oursql.connect(db='nowiki_p',
                    host='nowiki.labsdb',
                    read_default_file=os.path.expanduser('~/replica.my.cnf'),
                    charset=None,
                    use_unicode=False,
                    autoreconnect=True
                    )
# db.ping(True)
cur = db.cursor()


def get_config(fname):
    config = {}
    parser = ConfigParser.SafeConfigParser()
    parser.read([fname])
    return parser


def sout(m):
    sys.stdout.write(m)
    sys.stdout.flush()


def get_subcats(cat_name, exceptions, path=[]):
    sout('.')
    cats = [cat_name]
    cur.execute('SELECT page.page_title FROM page, categorylinks WHERE categorylinks.cl_to=? AND categorylinks.cl_type="subcat" AND categorylinks.cl_from=page.page_id', [cat_name.encode('utf-8')])
    for row in cur.fetchall():
        subcat = row[0].decode('utf-8')
        if subcat not in exceptions:
            if len(path) > 20:
                print("WARN: cat path exceeds max depth: {}".format(' > '.join(path)))
                sys.exit(1)
            cats.extend(get_subcats(subcat, exceptions, path + [cat_name]))
    return list(set(cats))


def get_memberpages(patterns, truncate=True):
    # Returns a set of page IDs as ints
    sout('.')
    if truncate:
        qargs = ['{}%'.format(x).encode('utf-8') for x in patterns]
        qstr = ' OR '.join(['cl_to LIKE ?' for x in patterns])
        qstr = 'SELECT cl_from FROM categorylinks WHERE ({}) AND cl_type="page" GROUP BY cl_from'.format(qstr)
    else:
        qargs = ['{}'.format(x).encode('utf-8') for x in patterns]
        qstr = 'cl_to IN ({})'.format(','.join('?'*len(patterns)))
        qstr = 'SELECT cl_from FROM categorylinks WHERE ({}) AND cl_type="page" GROUP BY cl_from'.format(qstr)
    cur.execute(qstr, qargs)
    pages = []
    for row in cur.fetchall():
        pages.append(row[0])
    return list(set(pages))


def get_pagecats(pageids):
    chunksize = 10000
    cats = {}
    for n in range(0, len(pageids), chunksize):
        sout('.')
        tp = pageids[n:n+chunksize]
        cur.execute('SELECT cl_from, cl_to FROM categorylinks WHERE cl_from IN ({})'.format(', '.join('?' * len(tp))), tp)
        for row in cur.fetchall():
            if row[0] not in cats:
                cats[row[0]] = []
            cats[row[0]].append(row[1].decode('utf-8'))
    return cats


def hiddenonly(catnames):
    chunksize = 1000
    filtered = []
    for n in range(0, len(catnames), chunksize):
        sout('.')
        tp = [x.encode('utf-8') for x in catnames[n:n+chunksize]]

        cur.execute('SELECT page_title FROM page, page_props WHERE page_id=pp_page AND page_namespace=14 AND pp_propname="hiddencat" AND page_title IN ({})'.format(', '.join('?' * len(tp))), tp)
        for row in cur.fetchall():
            filtered.append(row[0].decode('utf-8'))
    return filtered

def main(maintenance_exceptions):
    ignorecats = ['Personer_fra_', 'Fødsler_i_', 'Dødsfall_i_']
    ignorepages = 'Dødsfall_i_'

    sout('Reading category tree')
    if os.path.exists('subcats.cache'):
        sout(' from cache')
        mcats = pickle.load(open('subcats.cache', 'r'))
    else:
        mcats = get_subcats('Wikipedia-vedlikehold', maintenance_exceptions)
        pickle.dump(mcats, open('subcats.cache', 'w'))
    sout('\n')
    print('Read {} maintenance categories'.format(len(mcats)))


    sout('Reading member pages')
    if os.path.exists('pageids.cache'):
        sout(' from cache')
        pageids = pickle.load(open('pageids.cache', 'r'))
    else:
        pageids = get_memberpages(ignorecats)
        pickle.dump(pageids, open('pageids.cache', 'w'))
    sout('\n')
    print('Read {} member pages'.format(len(pageids)))

    sout('Getting page categories')
    if os.path.exists('pagecats.cache'):
        sout(' from cache')
        pagecats = pickle.load(open('pagecats.cache', 'r'))
    else:
        sout('.')
        pagecats = get_pagecats(pageids)
        pickle.dump(pagecats, open('pagecats.cache', 'w'))
    sout('\n')
    print('Got cats for {} pages'.format(len(pagecats.keys())))


    sout('Testing pages')
    r1 = re.compile(r'(' + '|'.join(ignorecats) + ')')
    plist = []
    for pageid, cats in pagecats.items():
        others = [x for x in cats if r1.match(x) is None and x not in mcats]
        if len(others) == 0:
            plist.append(pageid)

    sout('\n')
    print('Found {} matching pages'.format(len(plist)))

    titles = []
    for page_id in plist:
        cur.execute('SELECT page_namespace, page_title FROM page WHERE page_id=? LIMIT 1', [int(page_id)])
        page = cur.fetchall()[0]
        page_title = page[1].decode('utf-8')
        if re.match(ignorepages, page_title) is None:
            titles.append(page_title.replace('_', ' '))

    print('Found {} matching titles'.format(len(titles)))
    return titles


def main2(maintenance_exceptions):
    sout('Reading category tree')
    if os.path.exists('subcats.cache'):
        sout(' from cache')
        mcats = pickle.load(open('subcats.cache', 'r'))
        sout('\n')
        print('Read {} maintenance categories'.format(len(mcats)))
    else:
        mcats = get_subcats('Wikipedia-vedlikehold', maintenance_exceptions)
        sout('\n')
        # pickle.dump(mcats, open('subcats.raw.cache', 'w'))
        # mcats = pickle.load(open('subcats.raw.cache', 'r'))
        print('Read {} maintenance categories'.format(len(mcats)))
        mcats = hiddenonly(mcats)
        print('... of which {} hidden'.format(len(mcats)))
        pickle.dump(mcats, open('subcats.cache', 'w'))
        # sys.exit(1)

    sout('Reading member pages')
    if os.path.exists('pageids2.cache'):
        sout(' from cache')
        pageids = pickle.load(open('pageids2.cache', 'r'))
    else:
        pageids = get_memberpages(mcats, False)
        pickle.dump(pageids, open('pageids2.cache', 'w'))
    sout('\n')
    print('Read {} member pages'.format(len(pageids)))

    sout('Getting page categories')
    if os.path.exists('pagecats2.cache'):
        sout(' from cache')
        pagecats = pickle.load(open('pagecats2.cache', 'r'))
    else:
        sout('.')
        pagecats = get_pagecats(pageids)
        pickle.dump(pagecats, open('pagecats2.cache', 'w'))
    sout('\n')
    print('Got cats for {} pages'.format(len(pagecats.keys())))

    sout('Testing pages')
    plist = []
    for pageid, cats in pagecats.items():
        others = [x for x in cats if x not in mcats]
        if len(others) == 0:
            plist.append(pageid)

    sout('\n')
    print('Found {} matching pages'.format(len(plist)))

    titles = []
    for page_id in plist:
        cur.execute('SELECT page_namespace, page_title FROM page WHERE page_id=? LIMIT 1', [int(page_id)])
        try:
            page = cur.fetchall()[0]
        except IndexError:
            print('ERROR: Failed to lookup page {}'.format(page_id))
        page_title = page[1].decode('utf-8')
        if page[0] == 0:
            titles.append(page_title.replace('_', ' '))
        
    print('Found {} matching titles'.format(len(titles)))
    return titles


def update_page(site, pagename, titles):
    page = site.pages[pagename]
    origtext = page.text()
    beginlistmarker = '<!--BegynnListe-->'
    p = origtext.find(beginlistmarker)
    if p == -1:
        raise StandardError('Uh oh, begin list marker not found')
    titles = sorted(titles)
    text = '\n'.join('* [[{}]]'.format(title) for title in titles)
    dt = datetime.now().strftime('%Y-%m-%d')
    newtext = '{}{}{:d} sider (oppdatert {}):\n\n{}'.format(origtext[0:p], beginlistmarker, len(titles), dt, text)
    print('New text ready')
    # print(newtext)
    page.save(newtext, 'Bot: Oppdaterer liste')
    print('New text saved')


conf = get_config('config.cnf')
mwsite = dict(conf.items('site'))
mwoptions = dict(conf.items('options'))
site = mwclient.Site(**mwsite)

maintenance_exceptions = ['Mangler_interwiki', 'Kategorier_som_trenger_diffusjon', 'Artikler_som_bør_flettes', 'Artikler_som_bør_flyttes', 'Sider_som_er_foreslått_slettet', 'Kategorier_som_trenger_diffusjon', 'Kvinner', 'Menn']

update_page(site, 'Wikipedia:Kategorifattige biografier', main(maintenance_exceptions))
#update_page(site, 'Wikipedia:Artikler med kun vedlikeholdskategorier', main2(maintenance_exceptions))

print('Exiting')

