#!/usr/bin/env python3

#
# Multi-process and Multi-threaded fast scraper for phpBB forums
#
# Author: 2020, Anton D. Kachalov <mouse@ya.ru>
# License: MIT
#
# TODO:
#  - Support for password-protected forums/topics
#

import os
import re
import sys
import time
import orjson
import getopt
import locale
import logging
import urllib3
import requests
import datetime
import dateutil.parser
from queue import Empty
import multiprocessing as mp
from bs4 import BeautifulSoup


scraper_opts = {
  'parser': 'lxml',
#  'parser': 'html.parser',
  'headers': {},
  'output': None,
  'log_level': logging.WARNING,
  'lc_time': ('ru_RU', 'utf-8'),
  'parse_date': False,
  'save_media': False,
  'save_attachments': False,
  'save_users': False,
  'force': False,
  'max_workers': 10,
  'pool_size': 1000,
  'max_retries': 3,
  'timeout': 30.0,
}


def send_worker(r):
  retry = 0
  while retry < scraper_opts['max_retries']:
    try:
      req = r.request()
      return req, req.obj
    except requests.exceptions.ConnectionError as e:
      retry += 1

  logging.warning('Failed to request {}'.format(r))
  return None, None


def scrape_worker(args):
  ok, obj, r, page_merger = args
  if not ok or obj is None:
    return []
  pages = obj.scrape(r, page_merger)
  return pages


class PageMerger:
  def __init__(self):
    self._pages = mp.Manager().dict()
    self._lock = mp.Manager().Lock()


  def add(self, opts, session, paths, key, cls, data, size):
    self._lock.acquire()
    self._pages[key] = {
      'data': [None]*size,
      'remain': size,
      'opts': opts,
      'paths': paths,
      'session': session,
      'class': cls,
    }
    self._lock.release()
    return self.append(key, data, 0)


  def append(self, key, data, offset):
    self._lock.acquire()
    v = self._pages[key]
    v['data'][offset:offset+len(data)] = data
    v['remain'] -= len(data)
    # To induce shared dict update need an assignment operation
    self._pages[key] = v
    media = []
    if v['remain'] <= 0:
      media = v['class'].save(
        session=v['session'], id=key,
        data=v['data'], paths=v['paths'],
        opts=v['opts'])
      del self._pages[key]

    self._lock.release()
    return media


  def stats(self):
    self._lock.acquire()
    stats = []
    for k, v in self._pages.items():
      stats.append({
        'remain': v['remain'],
        'size': len(v['data']),
        'paths': ' / '.join([p[0] for p in v['paths']] + [str(k)]),
      })

    self._lock.release()
    return stats


  def force_save(self):
    self._lock.acquire()
    media = []
    if len(self._pages):
      logging.info('FORCE: Saving unmerged pages')
    for key, v in self._pages.items():
      v['data'] = list(filter(None, v['data']))
      media.extend(v['class'].save(
        session=v['session'], id=key,
        data=v['data'], paths=v['paths'],
        opts=v['opts']))

    self._lock.release()
    return media


class FileSaver:
  def __init__(self, opts, session, paths, fname, url, use_session=False):
    self._opts = opts
    self._session = session
    self._paths = paths
    self._fname = fname
    self._url = url
    if not use_session:
      self._session = requests.Session()
      a = requests.adapters.HTTPAdapter(max_retries=self._opts['max_retries'], )
      self._session.mount('http://', a)
      self._session.mount('https://', a)
      self._session.verify = False
      self._opts['headers'] = {}


  def __str__(self):
    return r'FileSaver<{} @ {}>'.format(self._fname, self._url)


  def __repr__(self):
    return self.__str__()


  def request(self):
    r = self._session.get(self._url, timeout=self._opts['timeout'])
    r.obj = self
    return r


  @staticmethod
  def full_path(output, paths, fname):
    fname = re.sub(r'[/\\|:\s]', '_', fname).encode('utf-8', 'ignore').decode('utf-8')

    ospaths = [p[0] for p in paths]
    dname = os.path.join(output, *ospaths)
    ext = fname.rsplit('.', 1)
    if len(ext) != 2:
      ext = [ext[0], 'unk']
    return '{}.{}'.format(os.path.join(dname, ext[0]), ext[1])


  def scrape(self, resp, page_merger):
    fname = FileSaver.full_path(self._opts['output'], self._paths, self._fname)
    dname = os.path.dirname(fname)
    logging.info('Saving {} [{}]'.format(fname, resp.headers['Content-Type']))

    try:
      os.makedirs(dname, exist_ok=True)
    except Exception as e:
      logging.error('Error creating directory {}: {}'.format(dname, e))
      return []

    try:
      with open(fname, 'wb+') as f:
        f.write(resp.content)
    except Exception as e:
      logging.error('Unable to save file {}: {}'.format(fname, e))
      return []

    return []


class PhpBBElement:
  def __init__(self, opts):
    self._opts = opts


  def __str__(self):
    return r'<{}>'.format(self.__class__)


  def __repr__(self):
    return self.__str__()


  def _get_url_query(self, url):
    if isinstance(url, str):
      url = requests.utils.urlparse(url)

    if '=' not in url.query:
      return {}

    return dict(x.split('=') for x in url.query.split('&'))


  def _pagination(self):
    self._path = []

    breadcrumbs = self._page.select('#nav-breadcrumbs > li.breadcrumbs > span.crumb[data-forum-id]')
    for b in breadcrumbs:
      self._path.append((b['data-forum-id'], b.a.span.string.strip()))

    pagination = self._page.select('div.action-bar.bar-top > div.pagination > ul > li:not(.next):not(.page-jump) > a.button')
    url = ''
    pages_count = 1
    start_value = 0
    if pagination:
      pages_count = int(pagination[-1].string.strip())
      url = self._get_url_query(pagination[-1]['href'])
      starts = set([0])
      for p in pagination:
        try:
          u = self._get_url_query(p['href'])
        except Exception as e:
          logging.info('Unable to parse URL {} : {}'.format(p['href'], e))
          continue
        if 'start' in u:
          starts.add(int(u['start']))
      starts = sorted(starts)
      if len(starts) > 1:
        deltas = set()
        for i in range(1, len(starts)):
          deltas.add(starts[i] - starts[i-1])
        start_value = min(deltas)

    pgs = self._page.select('div.action-bar.bar-top > div.pagination')[0]
    try:
      pgs.ul.decompose()
      pgs.a.decompose()
    except:
      pass
    m = re.search(r'^[^\d+]*(\d+)', pgs.text.strip())
    self._elements_count = int(m.group(1))
    if pages_count == 1:
      start_value = self._elements_count

    return url, start_value, pages_count


class PhpBBForum(PhpBBElement):
  def __init__(self, opts, session, forum_id, start):
    super().__init__(opts)
    self._session = session
    self._forum_id = int(forum_id)
    self._start = int(start)


  def __str__(self):
    return r'PhpBBForum<id={} @ {}>'.format(self._forum_id, self._start)


  def request(self):
    r = self._session.get(self._url(forum_id=self._forum_id, start=self._start), timeout=self._opts['timeout'])
    r.obj = self
    return r


  def scrape(self, resp, page_merger):
    self._page = BeautifulSoup(resp.content, self._opts['parser'])

    msg = self._page.select('#message p')
    if msg:
      logging.error('Fetch forum {} failed: {}'.format(self._forum_id, msg[0].string.strip()))
      return []

    msg = self._page.select('form#login')
    if msg:
      logging.error('Login required to scrape forum {}'.format(self._forum_id))
      return []

    pages = []
    if not self._forum_id:
      # Queue forums list only
      for f in self._page.select('div#page-body > div.forabg li.header a'):
        fid = self._get_url_query(f['href'])['f']
        pages.append(PhpBBForum(self._opts, self._session, fid, 0))
      return pages

    for f in self._page.select('div#page-body > div.forabg li.row > dl > dt a.forumtitle'):
      fid = self._get_url_query(f['href'])['f']
      pages.append(PhpBBForum(self._opts, self._session, fid, 0))

    for f in self._page.select('div#page-body > div.forumbg li.row > dl > dt a.topictitle'):
      tid = int(self._get_url_query(f['href'])['t'])
      if tid not in self._opts['scraped_topics']:
        pages.append(PhpBBTopic(self._opts, self._session, tid, 0))

    if self._start != 0:
      return pages

    url, start_value, pages_count = self._pagination()

    if not 'start' in url:
      return []

    last = int(url['start'])
    for i in range(1, pages_count):
      pages.append(PhpBBForum(self._opts, self._session,
                              forum_id=self._forum_id,
                              start=i * start_value))

    return pages


  def _url(self, forum_id = 0, start = 0):
    args = []
    if forum_id:
      args.append('f={}'.format(forum_id))
    if start:
      args.append('start={}'.format(start))

    if args:
      return '{}/viewforum.php?{}'.format(self._opts['url'], '&'.join(args))
    return '{}'.format(self._opts['url'])


class PhpBBTopic(PhpBBElement):
  def __init__(self, opts, session, topic_id, start):
    super().__init__(opts)
    self._session = session
    self._posts = []
    self._forum_id = None
    self._topic_id = int(topic_id)
    self._start = int(start)    


  def __str__(self):
    return r'PhpBBTopic<id={} @ {}>'.format(self._topic_id, self._start)


  def request(self):
    r = self._session.get(self._url(topic_id=self._topic_id, start=self._start), timeout=self._opts['timeout'])
    r.obj = self
    return r


  def _parse_page(self):
    for p in self._page.select('div.post'):
      pid = p['id'][1:]
      post = {
       'msg_id': int(pid)
      }
      member = p.select('div > dl.postprofile > dt > a')
      if member:
        post['uid'] = int(self._get_url_query(member[0]['href'])['u'])
        post['user'] = member[0].string.strip()
      date = p.select('#post_content{} > p.author'.format(pid))[0]
      # HACK: find(recursive=False, text=True) doesn't work
      date.a.decompose()
      date.span.decompose()
      post['date'] = date.text.strip()
      if self._opts['parse_date']:
        post['date'] = int(dateutil.parser.parse(post['date']).timestamp())

      content = p.select('#post_content{} > div.content'.format(pid))[0]
      post.update(self._html2bb(content))

      for img in p.select('dl.attachbox img.postimage'):
        try:
          post['files'].append((img['alt'], '{}/{}'.format(self._opts['url'], img['src'])))
        except:
          print(img)
          sys.exit(0)
        img.decompose()

      self._posts.append(post)


  def _unwrap_tag(self, tag, before, after, extract=None, cleanup=None):
    if extract:
      if not isinstance(extract, list):
        extract = [extract]
      v = [x(tag) for x in extract]
      if '{}' in before:
        before = before.format(*v)
      if '{}' in after:
        after = after.format(*v)

    if before:
      tb = self._page.new_tag('span')
      tb.string = before
      tag.insert_before(tb)
      tb.unwrap()

    if after:
      ta = self._page.new_tag('span')
      ta.string = after
      tag.insert_after(ta)
      ta.unwrap()

    if cleanup:
      cleanup(tag)

    tag.unwrap()


  def _replace_tags(self, tags, before, after, extract=None, cleanup=None):
    for t in tags:
      self._unwrap_tag(t, before, after, extract, cleanup)


  def _extract_style(self, tag, name):
    style = tag['style']
    if ';' in style:
      style = style.split(';')
    else:
      style = [style]
    for s in style:
      k,v = s.strip().split(':', 1)
      if k.strip() == name:
        return v.strip()

    return None      


  def _html2bb(self, content):
    res = {'files': []}
    if self._opts['save_media']:
      res['media'] = []

    for img in content.select('div.inline-attachment > dl.thumbnail img.postimage'):
      res['files'].append((img['alt'], '{}/{}'.format(self._opts['url'], img['src'])))
      img.decompose()

    for a in content.select('div.inline-attachment > dl.file a.postlink'):
      res['files'].append((a.string.strip(), '{}/{}'.format(self._opts['url'], a['href'])))
      a.decompose()

    # Drop "Edit by" notice message
    for c in content.select('div.notice'):
      c.decompose()

    # Drop buttons from spoiler div
    for c in content.select('div > input.button2'):
      c.decompose()

    # Drop signatures
    for c in content.select('div.signature'):
      c.decompose()

    # Unwrap [code] section
    for c in content.select('div.codebox'):
      c.p.decompose()
      c.pre.code.unwrap()
      c.pre.unwrap()
      cp = self._page.new_tag('pre')
      c.wrap(cp)
      c.unwrap()
      cp.string = cp.string.replace('\n', '&#10;')


    self._replace_tags(content.find_all('pre'), '[code]', '[/code]')

    self._replace_tags(content.find_all('br'), '&#10;', '')
    self._replace_tags(content.select('span[style*="text-decoration:underline"]'), '[u]', '[/u]')
    self._replace_tags(content.select('span[style*="color:"]'), '[color={}]', '[/color]',
      lambda tag: self._extract_style(tag, 'color'))
    self._replace_tags(content.find_all('strong'), '[b]', '[/b]')
    self._replace_tags(content.select('em.text-italics'), '[i]', '[/i]')

    self._replace_tags(content.find_all('li'), '[*]', '')
    self._replace_tags(content.select('ol[style*="list-style-type:lower-alpha"]'), '[list=a]', '[/list]')
    self._replace_tags(content.select('ol[style*="list-style-type:decimal"]'), '[list=1]', '[/list]')
    self._replace_tags(content.select('a.postlink'), '[url{}]', '[/url]',
      lambda tag: '' if tag['href'] == tag.string else '={}'.format(tag['href']))
    self._replace_tags(content.select('img.smilies'), '{}', '',
      lambda tag: tag['alt'])

    if self._opts['save_media']:
      for i in content.select('img.postimage'):
        if i['src'][0] == '.':
          i['src'] = '{}/{}'.format(self._opts['url'], i['src'])
        res['media'].append((os.path.basename(i['src']), i['src']))

      for i in content.select('img[height]'):
        if i['src'][0] == '.':
          i['src'] = '{}/{}'.format(self._opts['url'], i['src'])
        res['media'].append((os.path.basename(i['src']), i['src']))

    self._replace_tags(content.select('img.postimage'), '[img]{}', '[/img]',
      lambda tag: tag['src'])
    self._replace_tags(content.select('img[height]'), '[fimg={}]{}', '[/fimg]',
      [lambda tag: tag['height'], lambda tag: tag['src']])

    self._replace_tags(content.find_all('ul'), '[list]', '[/list]')
    self._replace_tags(content.select('div[style*="padding:"]'), '[spoiler={}]', '[/spoiler]',
      lambda tag: tag.span.text.strip().replace(']', '\\]'),
      lambda tag: tag.span.decompose())
    self._replace_tags(content.select('span[style*="font-size:"]'), '[size={}]', '[/size]',
      lambda tag: self._extract_style(tag, 'font-size')[:-1])

    quotes = content.find_all('blockquote')
    quotes.reverse()
    for quote in quotes:
      q = {'who': ''}
      if quote.cite:
        if not quote.cite.a:
          q['who'] = '={}'.format(quote.cite.string.strip().rsplit(' ', 1)[0].replace(']', '\\]'))
        else:
          q['who'] = '={} user_id={}'.format(
            quote.cite.a.string.strip().replace(']', '\\]'),
            self._get_url_query(quote.cite.a['href'])['u'])
        pt = quote.cite.select('div.responsive-hide')
        if pt:
          qdate = pt[0].string.strip()
          if self._opts['parse_date']:
            qdate = int(dateutil.parser.parse(qdate).timestamp())
          q['who'] += ' time={}'.format(qdate)
        quote.cite.decompose()

      quote.div.unwrap()
      self._unwrap_tag(quote, '[quote{}]'.format(q['who']), '[/quote]')

    res['content'] = content.text.replace('\n', '').replace('&#10;', '\n')
    return res


  def _url(self, topic_id = 0, start = 0):
    args = []
    if topic_id:
      args.append('t={}'.format(topic_id))
    if start:
      args.append('start={}'.format(start))

    return '{}/viewtopic.php?{}'.format(self._opts['url'], '&'.join(args))


  def scrape(self, resp, page_merger):
    self._page = BeautifulSoup(resp.content, self._opts['parser'])
#    self._page = BeautifulSoup('<html><body><blockquote><cite>asd]bbb</cite><div>LLLL</div></blockquote><div class="codebox"><p>KOD</p><pre><code>TEST CODE\nFFFF</code></pre></div><br/><span style="text-decoration:underline">UNDERLINE</span>\n\n<ul><li>ITEM1</li></ul><ol style="list-style-type:lower-alpha"><li>ITEM1</li></ol><ol style="list-style-type:decimal"><li>ITEM2</li></ol><img src="https://avatars.mds.yandex.net/get-banana/55914/x25Bic0D9kVbOCgUbeLnDbwof_banana_20161021_22_ret.png/optimize" height="200"><img src="https://avatars.mds.yandex.net/get-banana/55914/x25Bic0D9kVbOCgUbeLnDbwof_banana_20161021_22_ret.png/optimize"><a href="http://localhost/" class="postlink">http://localhost/</a><span style="color:red">RED</span><span style="font-size: 50%; line-height: normal">SMALL</span>&lt;aaa;&gt;&eacute;</body></html>', self._opts['parser'])
#    self._html2bb(self._page)

    msg = self._page.select('#message p')
    if msg:
      logging.error('Fetch {} failed: {}'.format(self._topic_id, msg[0].string.strip()))
      return []

    msg = self._page.select('form#login')
    if msg:
      logging.error('Login required to scrape {}'.format(self._topic_id))
      return []

    self._topic_title = self._page.select('div#page-body > h2.topic-title')[0].text.strip()
    self._locked = len(self._page.select('div.action-bar > a > i.fa-lock')) != 0

    url, start_value, pages_count = self._pagination()
    self._forum_id = self._path[-1]
    self._parse_page()

    if self._start != 0:
      return page_merger.append(self._topic_id, self._posts, self._start)

    pages = []
    media = page_merger.add(opts=self._opts, session=self._session,
                            paths=self._path, key=self._topic_id,
                            data=self._posts, size=self._elements_count,
                            cls=PhpBBTopic)
    pages.extend(media)
    if not 'start' in url:
      return pages

    last = int(url['start'])
    for i in range(1, pages_count):
      pages.append(PhpBBTopic(self._opts, self._session,
                              topic_id=self._topic_id,
                              start=i * start_value))

    return pages


  @staticmethod
  def save(session, id, data, paths, opts):
    ospaths = [p[0] for p in paths]
    dname = os.path.join(opts['output'], *ospaths)
    fname = '{}.json'.format(os.path.join(dname, str(id)))
    logging.info('Saving {} posts to {}'.format(len(data), fname))
    media = []
    if opts['save_media'] or opts['save_attachments']:
      fpaths = paths.copy()
      fpaths.append((str(id), 'files'))
      for d in data:
        if d['files']:
          for i in d['files']:
            fn = FileSaver.full_path(opts['output'], fpaths, i[0])
            if not os.path.isfile(fn) or opts['force']:
              media.append(FileSaver(opts=opts, session=session, paths=fpaths, fname=i[0], url=i[1], use_session=True))

        if 'media' in d:
          for i in d['media']:
            fn = FileSaver.full_path(opts['output'], fpaths, i[0])
            if not os.path.isfile(fn) or opts['force']:
              media.append(FileSaver(opts=opts, session=session, paths=fpaths, fname=i[0], url=i[1], use_session=False))
          del d['media']

    try:
      os.makedirs(dname, exist_ok=True)
    except Exception as e:
      logging.error('Error creating directory {}: {}'.format(dname, e))
      return []

    # Dump forum meta if not exists
    mpaths = [opts['output']]
    for p in paths:
      mpaths.append(p[0])
      mname = os.path.join(*mpaths, '.meta.json')
      if os.path.isfile(mname):
        continue
      try:
        with open(mname, 'wb+') as f:
          f.write(orjson.dumps({'id': p[0], 'name': p[1]}))
      except Exception as e:
        logging.error('Unable to create forum metadata {}: {}'.format(mname, e))
        return []

    try:
      with open(fname, 'wb+') as f:
        f.write(orjson.dumps(data))
    except Exception as e:
      logging.error('Error saving posts to {}: {}'.format(fname, e))
      return []

    return media


class PhpBBUsers(PhpBBElement):
  def __init__(self, opts, session, start=0):
    super().__init__(opts)
    self._session = session
    self._start = start
    self._path = []


  def __str__(self):
    return r'PhpBBUsers<start={}>'.format(self._start)


  def request(self):
    r = self._session.get(self._url(self._start), timeout=self._opts['timeout'])
    r.obj = self
    return r


  def scrape(self, resp, page_merger):
    self._page = BeautifulSoup(resp.content, self._opts['parser'])
    msg = self._page.select('#message p')
    if msg:
      logging.error('Fetch users failed: {}'.format(msg[0].string.strip()))
      return []

    msg = self._page.select('form#login')
    if msg:
      logging.error('Login required to scrape users')
      return []

    users = []
    pages = []
    fpaths = self._path + [('users', 0)]
    for tr in self._page.select('table#memberlist > tbody > tr'):
      tds = tr.find_all('td')
      u = tds[0]
      registered = tds[-1]
      rdate = registered.text.strip()
      if self._opts['parse_date']:
        rdate = int(dateutil.parser.parse(rdate).timestamp())
      uid = self._get_url_query(u.a['href'])['u']
      users.append({
        'uid': uid,
        'date': rdate,
        'user': u.a.string.strip()
      })
      if not self._opts['save_attachments'] and not self._opts['save_media']:
        continue

      for ext in ('png', 'jpg'):
        fn='{}.{}'.format(uid, ext)
        fname = FileSaver.full_path(self._opts['output'], fpaths, fn)
        if os.path.isfile(fname) or self._opts['force']:
          continue
        pages.append(FileSaver(opts=self._opts, session=self._session,
                               paths=fpaths, fname=fn,
                               url='{}/download/file.php?avatar={}.{}'.format(self._opts['url'], uid, ext),
                               use_session=True))


    url, start_value, pages_count = self._pagination()

    if self._start != 0:
      pages.extend(page_merger.append('users', users, self._start))
      return pages

    page_merger.add(opts=self._opts, session=self._session,
                    paths=self._path, key='users', data=users,
                    size=self._elements_count, cls=PhpBBUsers)
    if not 'start' in url:
      return pages

    last = int(url['start'])
    for i in range(1, pages_count):
      pages.append(PhpBBUsers(self._opts, self._session, start=i * start_value))

    return pages


  def _url(self, start = 0):
    args = []
    if start:
      args.append('start={}'.format(start))

    return '{}/memberlist.php?{}'.format(self._opts['url'], '&'.join(args))


  @staticmethod
  def save(session, id, data, paths, opts):
    ospaths = [p[0] for p in paths]
    dname = os.path.join(opts['output'], *ospaths)
    fname = '{}.json'.format(os.path.join(dname, str(id)))
    logging.info('Saving {} users to {}'.format(len(data), fname))

    try:
      os.makedirs(dname, exist_ok=True)
    except Exception as e:
      logging.error('Error creating directory {}: {}'.format(dname, e))
      return []

    try:
      with open(fname, 'wb+') as f:
        f.write(orjson.dumps(data))
    except Exception as e:
      logging.error('Error saving posts to {}: {}'.format(fname, e))
      return []

    return []


class RequestsIter:
  def __init__(self, size):
    self._queue = mp.Manager().Queue(size)
    self._enqueued = 0
    self.processed = 0


  def __next__(self):
    try:
      return self._queue.get(block=False)
    except Empty:
      raise StopIteration


  def __iter__(self):
    return self


  def is_done(self):
    if self.processed < self._enqueued:
      return False

    if self.processed > self._enqueued:
      logging.info('Processed pages {} more than enqueued {}'.format(self.processed, self._enqueued))
    return True


  def put(self, item):
    logging.debug('Enqueue: {}'.format(item))
    self._enqueued += 1
    self._queue.put(item)


class PhpBBScraper:
  def __init__(self, opts, forums_arg, topics_arg):
    self._opts = opts
    self._page_merger = PageMerger()
    self._topics = []
    self._processed_pages = 0
    self._opts['scraped_topics'] = []
    self._session = requests.Session()
    a = requests.adapters.HTTPAdapter(max_retries=self._opts['max_retries'])
    self._session.mount('http://', a)
    self._session.mount('https://', a)
    self._session.verify = False
    self._session.headers.update(opts['headers'])
    self._queue = RequestsIter(self._opts['pool_size'])
    if not self._opts['force']:
      self._load_state()

    if self._opts['save_users']:
      self._topics.append(PhpBBUsers(self._opts, self._session))
    elif not forums_arg and not topics_arg:
      forums_arg = ['0']

    self._parse_arg(forums_arg, is_topics=False)
    self._parse_arg(topics_arg, is_topics=True)


  def scrape(self):
    for t in self._topics:
      self._queue.put(t)

    while not self._queue.is_done():
      pool = mp.Pool(processes=self._opts['max_workers'])
      for response, obj in pool.imap_unordered(send_worker, self._queue):
        if response is None:
          self.processed()
          yield (False, None, None, None)
          continue

        if isinstance(obj, PhpBBTopic) and obj._start == 0:
          self._processed_pages += 1
        if response.status_code == 200:
          # Yield good result, no repsonse needed to pass back
          yield (True, obj, response, self._page_merger)
        else:
          # Bad status received, push back the repsonse details
          self.processed()
          yield (False, obj, response, None)

      pool.close()
      pool.join()


  def set_topics(self, topics):
    self._topics = topics


  def enqueue(self, pages):
    for p in pages:
      self._queue.put(p)


  def processed(self):
    self._queue.processed += 1


  def stats(self):
    tm = self._page_merger.stats()
    pages = ['{}\t{} of {}'.format(v['paths'], v['remain'], v['size']) for v in tm]

    logging.info('============ STATS ===========')
    if self._opts['scraped_topics']:
      logging.info('Already Scraped Topics: {}'.format(len(self._opts['scraped_topics'])))
    logging.info('Processed Requests: {}'.format(self._queue.processed))
    if self._processed_pages:
      logging.info('Processed Pages: {}'.format(self._processed_pages))
    if pages:
      logging.info('Incomplete Pages:\n\t{}'.format('\n\t'.join(pages)))


  def force_merge(self):
    return self._page_merger.force_save()


  def _parse_arg(self, arg, is_topics):
    for t in arg:
      if ',' in t:
        item_list = t.split(',')
      else:
        item_list = [t]

      for r in item_list:
        if '-' not in r:
          if is_topics and int(r) not in self._opts['scraped_topics']:
            self._topics.append(PhpBBTopic(self._opts, self._session, int(r), 0))
          elif not is_topics:
            self._topics.append(PhpBBForum(self._opts, self._session, int(r), 0))
          continue

        rlist = r.split('-')
        if len(rlist) != 2:
          raise ValueError('Wrong specifier for topic range in: {}'.format(t))

        for i in range(int(rlist[0]), int(rlist[1])+1):
          if is_topics and i not in self._opts['scraped_topics']:
            self._topics.append(PhpBBTopic(self._opts, self._session, i))
          elif not is_topics:
            self._topics.append(PhpBBForum(self._opts, self._session, i))


  def _load_state(self):
    logging.info('Searching for downloaded topics in {}'.format(self._opts['output']))
    for dname, subdirs, flist in os.walk(self._opts['output']):
      for f in flist:
        if f == '.meta.json':
          continue
        if '.json' in f:
          try:
            tid = int(f[:-5])
            self._opts['scraped_topics'].append(tid)
          except:
            pass


def usage(rc):
  print('Usage: {} [-v|-h|-w workers|-p pool-size|-o output-dir|-a user-agent|-c cookie|-m|-s|[-t|-f] id[-id|,id]] URL\n'.format(os.path.basename(sys.argv[0])))
  sys.exit(rc)


def main():
  try:
    opts, args = getopt.getopt(sys.argv[1:],
      'hf:t:w:a:c:mso:up:v', [
      'help',
      'force',
      'forum=',
      'topic=',
      'workers=',
      'pool-size=',
      'user-agent=',
      'cookie=',
      'save-media',
      'save-attachments',
      'save-users',
      'output=',
      'parse-date',
    ])
    if args:
      scraper_opts['url'] = args[0]
  except getopt.GetoptError as err:
    print(err)
    usage(2)

  if 'url' not in scraper_opts:
    usage(1)

  topics = []
  forums = []
  logger_fmt='%(levelname)s: %(message)s'
  for o, a in opts:
    if o in ('-h', '--help'):
      usage(0)
    elif o == '--parse-date':
      scraper_opts['parse_date'] = True
    elif o == '--force':
      scraper_opts['force'] = True
    elif o in ('-f', '--forum'):
      forums.append(a)
    elif o in ('-t', '--topic'):
      topics.append(a)
    elif o in ('-w', '--workers='):
      scraper_opts['max_workers'] = int(a)
    elif o in ('-p', '--pool-size='):
      scraper_opts['pool_size'] = int(a)
    elif o in ('-a', '--user-agent='):
      scraper_opts['headers']['user-agent'] = a
    elif o in ('-c', '--cookie='):
      scraper_opts['headers']['cookie'] = a
    elif o in ('-m', '--save-media'):
      scraper_opts['save_media'] = True
    elif o in ('-s', '--save-attachments'):
      scraper_opts['save_attachments'] = True
    elif o in ('-u', '--save-users'):
      scraper_opts['save_users'] = True
    elif o in ('-o', '--output='):
      scraper_opts['output'] = a
    elif o == '-v':
      if scraper_opts['log_level'] == logging.WARNING:
        scraper_opts['log_level'] = logging.INFO
      elif scraper_opts['log_level'] == logging.INFO:
        scraper_opts['log_level'] = logging.DEBUG
      logger_fmt='%(levelname)s:%(asctime)s:%(name)s:%(process)s:%(thread)s:%(message)s'

  if scraper_opts['output'] is None:
    scraper_opts['output'] = requests.utils.urlparse(scraper_opts['url']).netloc

  logging.basicConfig(level=scraper_opts['log_level'], format=logger_fmt)
  locale.setlocale(locale.LC_TIME, scraper_opts['lc_time'])

  urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
  start_ = time.time()

  web = PhpBBScraper(scraper_opts, forums, topics)

  pool = mp.Pool(processes=scraper_opts['max_workers'])
  for pages in pool.imap_unordered(scrape_worker, web.scrape()):
    web.enqueue(pages)
    web.processed()

  pool.close()
  pool.join()

  media = web.force_merge()
  if media:
    logging.info('FORCE: Fetching media from unmerged topics')

    pool = mp.Pool(processes=scraper_opts['max_workers'])
    web.set_topics(media)
    for pages in pool.imap_unordered(scrape_worker, web.scrape()):
      web.enqueue(pages)
      web.processed()

    pool.close()
    pool.join()

  web.stats()

  end_ = time.time()
  logging.info('Completed in {}'.format(datetime.timedelta(milliseconds=int((end_ - start_) * 1000))))

if __name__ == '__main__':
  main()
