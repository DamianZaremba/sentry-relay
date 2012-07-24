import eventlet
from eventlet.green import socket
from django.utils.encoding import smart_str
from pqueue import PersistentQueue
from raven.base import Client
import logging
import threading
import time
import base64
import zlib
import json

logger = logging.getLogger("sentry.errors")
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())
queue = PersistentQueue("queue.dat")

class InvalidData(Exception): pass
class APIError(Exception): pass

class QueueProcessor(threading.Thread):
	def __init__(self, dsn):
		global queue
		self.client = Client(dsn)
		self.queue = queue
		threading.Thread.__init__(self)

	def forward(self, data, header=None):
		logger.info("Sending %s" % data)
		self.client.send_encoded(data, header)

	def run(self):
		while True:
			try:
				while not self.queue.empty():
					self.forward(queue.get())
			except (SystemExit, KeyboardInterrupt):
				break
			else:
				time.sleep(0.2)

class SentryRelay:
	BUF_SIZE = 2 ** 16
	POOL_SIZE = 1000

	_socket = None
	_spawn = None

	secret_key = None

	def __init__(self, host=None, port=None, debug=False):
		self.host = 'localhost'
		self.port = 9000

		self._socket = socket.socket
		self._pool = eventlet.GreenPool(size=self.POOL_SIZE)
		self._spawn = self._pool.spawn_n

	def handle(self, data, address):
		try:
			auth_header, data = data.split('\n\n', 1)
		except ValueError:
			raise APIError('missing auth header')
		else:
			header = dict(map(lambda x: x.strip().split('='), auth_header.split(' ', 1)[1].split(',')))

			if 'sentry_key' in header and self.secret_key != header['sentry_key']:
				raise APIError('authentication failure')

			logger.info("Adding %s to queue" % data)
			queue.put(data)

	def run(self):
		sock = self._socket(socket.AF_INET, socket.SOCK_DGRAM)
		sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		sock.bind((self.host, self.port))

		while True:
			try:
				self._spawn(self.handle, *sock.recvfrom(self.BUF_SIZE))
			except (SystemExit, KeyboardInterrupt):
				break

if __name__ == '__main__':
	sr = SentryRelay()
	sr.secret_key = 'abc123'

	sq = QueueProcessor(
		'https://s:p@app.getsentry.com/1'
	)

	sq.start()
	sr.run()
