#!/usr/bin/env python

from __future__ import print_function

import os, os.path, re, sys
import sqlite3
from lxml.html import document_fromstring

DIR = 'www.ibras.dk/montypython'
DB = 'quotes.db'

IGNORE = ['i.e.', 'e.g.', 'etc.', 'Mr.', 'Mrs.', 'Dr.', 'Ldt.', 'A.D.', 'B.C.']

EP_PREFIX = re.compile(r'^\s*Episode\s+\w+(-\w+)*:', re.I)
SPLIT = re.compile(r'(?:[\.!?]+|\r?\n[ \t]*\r?\n)', re.I)
NO_SPLIT = re.compile(r'\b(?:' + '|'.join(re.escape(x) for x in IGNORE) + r'|[IVXCM]+\.|[A-Z]\.|\.\.+)$', re.I)
PUNCT = re.compile(r'[-\s\.\!\?\'"/()\\+*:,;{}]+')
BLACKLIST = set(s.lower() for s in
	["Monty Python's Flying Circus.", "'MONTY PYTHON'S FLYING CIRCUS'"])

def split_sentences(text):
	prev = 0
	i = 0
	n = len(text)
	while i < n:
		m = SPLIT.search(text, i)
		if m:
			i = m.end()
			if i - prev < 3:
				continue
		else:
			i = n

		if i == n or not NO_SPLIT.search(text, prev, i):
			yield text[prev:i].strip()
			prev = i

def extract(srcdir, dbout):
	con = sqlite3.connect(dbout)
	try:
		cur = con.cursor()
		cur.execute('''create table episodes (episodenr integer primary key, title text)''')
		cur.execute('''create table sketches (episodenr integer, sketchnr integer, title text, primary key (episodenr, sketchnr))''')
		cur.execute('''create table quotes (episodenr integer, sketchnr integer, quotenr integer, primary key (episodenr, sketchnr, quotenr))''')
		cur.execute('''create virtual table quotes_fts using fts4(tokenize=porter unicode61 "remove_diacritics 1")''')
		con.commit()

		for episodenr in range(1,46):
			print("episode: %d" % episodenr)
			fname = "episode%02d.htm" % episodenr

			with open(os.path.join(srcdir, fname),'rb') as f:
				doc = document_fromstring(f.read())

			episode_title = EP_PREFIX.sub('', doc.cssselect('h1')[0].text_content()).strip()
			cur.execute('insert into episodes (episodenr, title) values (?, ?)', (episodenr, episode_title))

			sketchnr = 0
			cur.execute('insert into sketches (episodenr, sketchnr, title) values (?, ?, ?)', (episodenr, sketchnr, '(intro)'))

			quotenr = 1

			for el in doc.cssselect('a[name], td #John, td #Graham, td #Michael, td #Eric, td #TerryJ, td #TerryG, td #Carol'):
				if el.tag == 'a':
					sketch_ref = el.attrib['name']
					new_sketchnr = int(sketch_ref, 10)
					if new_sketchnr != sketchnr:
						sketchnr = new_sketchnr
						query = "body > center > a[href='%s#%s']" % (fname, sketch_ref)
						res = doc.cssselect(query)
						if res:
							sketch_title = res[0].text_content().strip()
						else:
							print("episode %d sketch %d: missing sketch title" % (episodenr, sketchnr))
							sketch_title = None
						cur.execute('insert into sketches (episodenr, sketchnr, title) values (?, ?, ?)', (episodenr, sketchnr, sketch_title))
				else:
					quote = el.text_content()
					if quote is not None:
						quote = quote.strip()
						if quote.lower() not in BLACKLIST:
							for sentence in split_sentences(quote):
								cur.execute('insert into quotes (episodenr, sketchnr, quotenr) values (?, ?, ?)', (episodenr, sketchnr, quotenr))
								docid = cur.lastrowid
								cur.execute('insert into quotes_fts (docid, content) values (?, ?)', (docid, sentence))
								quotenr += 1
		con.commit()
	finally:
		con.close()

if __name__ == '__main__':
	extract(DIR, DB)
