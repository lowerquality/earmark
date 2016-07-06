# minimal seatbelt-like realtime websocket database thing

from autobahn.twisted.websocket import WebSocketServerProtocol, \
                                       WebSocketServerFactory
from autobahn.twisted.resource import WebSocketResource
from twisted.web.static import File
from twisted.web.server import Site
from twisted.internet import reactor

import gzip
import json
import os

class DBFactory(WebSocketServerFactory):
    def __init__(self, dbdir="db"):
        WebSocketServerFactory.__init__(self)
        self.clients = {}       # peerstr -> client

        self.dbdir = dbdir
        self.docs = {}       # _id -> {doc}

        if os.path.exists(dbdir):
            self.load_db()
        else:
            os.makedirs(dbdir)

        # Create a changelog
        self.change_fh = open(os.path.join(self.dbdir, '_changes'), 'w')

    def get(self, key, default=None):
        return self.docs.get(key, default)
    def __getitem__(self, key):
        return self.docs.__getitem__(key)
    def __setitem__(self, key, value):
        # TODO
        pass
    def __delitem__(self, key):
        # TODO
        pass

    def load_db(self):
        # Load into self.db
        dbpath = os.path.join(self.dbdir, 'db.json')
        if os.path.exists(dbpath):
            self.docs = json.load(open(dbpath))

        # Incorporate changes
        changes_file = os.path.join(self.dbdir, "_changes")
        if os.path.exists(changes_file):
            for line in open(changes_file):
                if len(line.strip()) > 2:
                    c = json.loads(line)
                    self.update_inmem(c)

            # Serialize to db.json
            json.dump(self.docs, open(dbpath, 'w'), indent=2)

            # Archive _changes
            changes_fh = open(changes_file)
            changes_out_pattern = os.path.join(self.dbdir, "_changes.%d.gz")
            changes_out_idx = 1
            while os.path.exists(changes_out_pattern % (changes_out_idx)):
                changes_out_idx += 1
            changes_out_fh = gzip.open(changes_out_pattern % (changes_out_idx), 'wb')
            changes_out_fh.writelines(changes_fh)
            changes_out_fh.close()
            changes_fh.close()

    def register(self, client):
        self.clients[client.peer] = client

        # Send history
        client.sendMessage(json.dumps(
            {"type": "history",
             "history": self.docs}))

    def unregister(self, client):
        if client.peer in self.clients:
            del self.clients[client.peer]

    def update_inmem(self, change_doc):
        if change_doc['type'] == 'delete':
            del self.docs[change_doc['id']]
        else:
            self.docs[change_doc['id']] = change_doc['doc']

    def onchange(self, sender, change_doc):
        self.update_inmem(change_doc)

        self.change_fh.write("%s\n" % (json.dumps(change_doc)))
        self.change_fh.flush()
        
        for client in self.clients.values():
            if client != sender:
                client.sendMessage(json.dumps(change_doc))

class DBProtocol(WebSocketServerProtocol):
    def onOpen(self):
        self.factory.register(self)
        WebSocketServerProtocol.onOpen(self)

    def connectionLost(self, reason):
        self.factory.unregister(self)
        WebSocketServerProtocol.connectionLost(self, reason)

    def onMessage(self, payload, isBinary):
        if not isBinary:
            change_doc = json.loads(payload)
            self.factory.onchange(self, change_doc)

if __name__=='__main__':
    factory = DBFactory()
    factory.protocol = DBProtocol
    ws_resource = WebSocketResource(factory)

    root = File('.')
    root.putChild('_db', ws_resource)
    site = Site(root)

    reactor.listenTCP(9669, site, interface='0.0.0.0')
    print 'http://localhost:9669'
    reactor.run()
