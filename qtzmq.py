import zmq
import logging

from PyQt4.QtCore import (
    QObject,
    pyqtSignal,
    pyqtSlot,
)


logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)


class QtZmqListener(QObject):

    signal = pyqtSignal(list)

    url = None
    topics = None
    socktype = None
    sockopt = None

    def __init__(self, url=None, topics=None, socktype=None, sockopt=None):

        QObject.__init__(self)

        url = url or self.url
        topics = topics or self.topics
        socktype = socktype or self.socktype
        sockopt = sockopt or self.sockopt

        ctx = self.ctx = zmq.Context()
        sock = self.socket = ctx.socket(socktype)
        sock.connect(url)
        if isinstance(topics, (list, tuple)):
            for t in topics:
                sock.setsockopt(sockopt, t)
                LOG.info('Listener thread subscribed to url={url} topic={topic}'.format(url=url, topic=t))
        else:
            sock.setsockopt(sockopt, topics)
            LOG.info('Listener thread subscribed to url={url} topic={topic}'.format(url=url, topic=topics))

        self.running = True

    @pyqtSlot()
    def loop(self):
        sock = self.socket
        signal = self.signal
        LOG.info('Starting to route incoming 0mq messages from QtZmqListener thread (%x) to signal thread (%x)', hash(self), hash(signal))
        while self.running:
            mm = sock.recv_multipart()  # multipart message is a list
            signal.emit(mm)

    def stop(self):
        self.running = False
        LOG.info('QtZmqListener stopped routing incoming messages')
        self.socket.close()
        self.ctx.term()
        LOG.info('QtZmqListener stopped 0mq subscription, and closed 0mq context')
