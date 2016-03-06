#!/usr/bin/env python
# coding: UTF-8

from __future__ import print_function

import re
import yaml
import irc.bot
import logging
import sqlite3
import struct

PUNCT = re.compile(r'[-\s\.\!\?\'"/()\\+*:,;{}]+')

logger = logging.getLogger('quotebot')

def parse_match_info(buf):
	bufsize = len(buf)  # Length in bytes.
	return [struct.unpack('@I', buf[i:i+4])[0] for i in range(0, bufsize, 4)]

# source: http://charlesleifer.com/blog/using-sqlite-full-text-search-with-python/
def rank(raw_match_info):
	# handle match_info called w/default args 'pcx' - based on the example rank
	# function http://sqlite.org/fts3.html#appendix_a
	match_info = parse_match_info(raw_match_info)
	score = 0.0
	p, c = match_info[:2]
	for phrase_num in range(p):
		phrase_info_idx = 2 + (phrase_num * c * 3)
		for col_num in range(c):
			col_idx = phrase_info_idx + (col_num * 3)
			x1, x2 = match_info[col_idx:col_idx + 2]
			if x1 > 0:
				score += float(x1) / x2
	return score

class QuoteBot(irc.bot.SingleServerIRCBot):
	def __init__(self, nickname, channels, cursor, password=None, server='irc.twitch.tv', port=6667, min_score=0, reply_with_next_line=False, react_to_messages=True):
		self._join_channels = [channel if channel.startswith('#') else '#'+channel for channel in channels]

		self._cursor = cursor
		self._min_score = min_score
		self._reply_with_next_line = reply_with_next_line
		self._react_to_messages = react_to_messages
		self._sender = None
		self._current_line = None
		self._mentioned = re.compile(r'\A\s*@?'+re.escape(nickname)+r'\b\s*:?', re.I)

		irc.bot.SingleServerIRCBot.__init__(self, [(server, port, password)], nickname, nickname)

	def on_welcome(self, connection, event):
		for channel in self._join_channels:
			print('/join',channel)
			connection.join(channel)

	def on_nicknameinuse(self, connection, event):
		logger.error('nickname in use')

	def on_pubmsg(self, connection, event):
		message = event.arguments[0]
		print('%s: %s' % (event.source.nick, message))
		if message.startswith('!'):
			command = message.split()
			command, args = command[0], command[1:]

			if command == '!pyline' and args:
				self._react(event.target, ' '.join(args), verbose=True, sender=event.source.nick)
				
			elif command == '!pynext' and args:
				self._react(event.target, ' '.join(args), next_line=True, verbose=True, sender=event.source.nick)

			elif command == '!pyline':
				self._say_line(event.target, sender=event.source.nick, verbose=True)

			elif command == '!pynext':
				self._say_next_line(event.target, sender=event.source.nick, verbose=True)
				
			elif command == '!pyinfo':
				self._say_info(event.target, sender=event.source.nick, verbose=True)

		else:
			match = self._mentioned.match(message)
			if match:
				self._react(event.target, message[match.end():], self._reply_with_next_line, verbose=True, sender=event.source.nick, reply=True)

			elif self._react_to_messages:
				self._react(event.target, message, self._reply_with_next_line, sender=event.source.nick)

	def _react(self, target, message, next_line=False, verbose=False, sender=None, reply=False):
		query = PUNCT.sub(" ", message).lower()
		self._cursor.execute("select docid, content, rank(matchinfo(quotes_fts)) as score from quotes_fts where content match ? order by score desc limit 1", [query])
		res = self._cursor.fetchone()
		found = False
		if res:
			docid, quote, score = res
			if score >= self._min_score:
				self._cursor.execute("select episodenr, sketchnr, quotenr from quotes where rowid = ?", [docid])
				episodenr, sketchnr, quotenr = self._cursor.fetchone()

				self._sender = sender
				self._current_line = (episodenr, sketchnr, quotenr, score, quote)

				if next_line:
					self._say_next_line(target, verbose=verbose, sender=sender, reply=reply)
				else:
					self._say_line(target, verbose=verbose, sender=sender, reply=reply)
				found = True

		if verbose and not found:
			self._say(target, 'no quote found', sender=sender, reply=reply)

	def _say_next_line(self, target, verbose=False, sender=None, reply=False):
		if not self._current_line:
			if verbose:
				self._say(target, 'nothing quoted yet', sender=sender, reply=reply)
			return

		episodenr, sketchnr, quotenr, score, quote = self._current_line

		quotenr += 1
		self._cursor.execute("select rowid from quotes where episodenr = ? and sketchnr = ? and quotenr = ?", (episodenr, sketchnr, quotenr))
		res = self._cursor.fetchone()
		if not res:
			self._say(target, 'no more lines in this sketch', sender=sender, reply=reply)
			return

		docid, = res
		self._cursor.execute("select content from quotes_fts where docid = ?", [docid])
		quote, = self._cursor.fetchone()

		self._sender = sender
		self._current_line = (episodenr, sketchnr, quotenr, score, quote)
		self._say_line(target, sender=sender, reply=reply)
	
	def _say_info(self, target, verbose=False, sender=None, reply=False):
		if not self._current_line:
			if verbose:
				self._say(target, 'nothing quoted yet', sender=sender, reply=reply)
			return

		episodenr, sketchnr, quotenr, score, quote = self._current_line

		self._cursor.execute("select title from episodes where episodenr = ?", [episodenr])
		episode_title = self._cursor.fetchone()[0]

		self._cursor.execute("select title from sketches where sketchnr = ?", [sketchnr])
		sketch_title = self._cursor.fetchone()[0]

		self._say(target, "Last quote was from Monty Python's Flying Circus episode %d: %s, sketch %d: %s in reply to %s and was matched with a score of %f. %s" % (
			episodenr, episode_title, sketchnr, sketch_title or '(unknonw)', self._sender, score,
			'http://www.ibras.dk/montypython/episode%02d.htm#%d' % (episodenr, sketchnr) if sketchnr > 0 else
			'http://www.ibras.dk/montypython/episode%02d.htm' % episodenr
		), sender=sender, reply=reply)

	def _say_line(self, target, verbose=False, sender=None, reply=False):
		if not self._current_line:
			if verbose:
				self._say(target, 'no quote found', sender=sender, reply=reply)
			return

#		reply = "episode %d sketch %d line %d (match score %d): %s" % self._current_line
		quote = self._current_line[4].replace('\n', ' ')
		max_len = 512 - len(target) - 16

		if reply:
			max_len -= len(sender) + 3
		
		if len(quote) > max_len:
			quote = quote[:max_len - 3].rstrip() + u'…'

		self._say(target, quote, sender=sender, reply=reply)
	
	def _say(self, target, message, sender=None, reply=False):
		if reply:
			message = '@'+sender+': '+message

		print('%s: %s' % (self.connection.get_nickname(), message))
		self.connection.privmsg(target, message)

def main(args):
	import yaml
	import argparse

	parser = argparse.ArgumentParser()
	parser.add_argument('-c','--config',default='config.yaml')
	parser.add_argument('-l','--log-level',type=int,default=0)
	opts = parser.parse_args(args)

	logger.setLevel(opts.log_level)

	with open(opts.config,'rb') as fp:
		config = yaml.load(fp)

	server, port = config.get('host','irc.twitch.tv:6667').split(':',1)
	port = int(port)

	connection = sqlite3.connect(config.get('quotes','quotes.db'))
	try:
		connection.create_function('rank', 1, rank)

		bot = QuoteBot(
			config['nickname'],
			config['channels'],
			connection.cursor(),
			config.get('password'),
			server,
			port,
			config.get('min_score', 0),
			config.get('reply_with_next_line', False),
			config.get('react_to_messages', True))
		bot.start()
	finally:
		connection.close()

if __name__ == '__main__':
	import sys

	try:
		main(sys.argv[1:])
	except KeyboardInterrupt:
		print()
