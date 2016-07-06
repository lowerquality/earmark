# websocket-based chunked attachment database
#
# client: {type: start-upload, filename: filename, size: bytes}
# server: {type: upload-started, id: <uid>}
#
# client: <binary data>
# server: {type: got-chunk, id: <uid>, size: <size-so-far>}

from autobahn.twisted.websocket import WebSocketServerProtocol, \
                                       WebSocketServerFactory

import hashlib
import json
import shutil
import os

class AttachFactory(WebSocketServerFactory):
    def __init__(self, attachdir="db/_attachments"):
        self.attachdir = attachdir

        try:
            os.makedirs(attachdir)
        except OSError:
            pass

        WebSocketServerFactory.__init__(self)

    def start_upload(self, peer):
        # return (id, filepath)
        upload_dir = os.path.join(self.attachdir, 'uploading', peer.peer)
        try:
            os.makedirs(upload_dir)
        except OSError:
            pass

        uid = 0
        while os.path.exists(os.path.join(upload_dir, str(uid))):
            uid += 1

        return uid, os.path.join(upload_dir, str(uid))

    def import_file(self, filepath):
        # Moves a file into the attachment store and returns the path

        # Compute hash
        sha1 = hashlib.sha1()
        with open(filepath) as fh:
            buf = fh.read(2**15)
            while len(buf) > 0:
                sha1.update(buf)
                buf = fh.read(2**15)
                
        return move_to_database(filepath ,sha1.hexdigest(), self.attachdir)

    def onupload(self, cmd):
        print 'upload complete', cmd

class AttachProtocol(WebSocketServerProtocol):
    def onMessage(self, payload, isBinary):
        if not isBinary:
            cmd = json.loads(payload)

            if cmd['type'] == 'start-upload':
                id, self.cur_filepath = self.factory.start_upload(self)

                self.cur_upload = cmd
                self.cur_size = 0
                self.cur_fh = open(self.cur_filepath, 'w')
                self.cur_id = id
                self.cur_sha1 = hashlib.sha1()

                # Dump metadata
                json.dump(cmd, open("%s.meta.json" % (self.cur_filepath), 'w'))

                self.sendMessage(json.dumps({"type": "upload-started", "id": id}))
        else:
            # binary message -- add chunk
            self.cur_fh.write(payload)
            self.cur_sha1.update(payload)
            self.cur_size += len(payload)
            if self.cur_size >= self.cur_upload['size']:
                # upload finished
                self.cur_fh.close()

                hashstr = self.cur_sha1.hexdigest()
                _r, ext = os.path.splitext(self.cur_upload['filename'])
                hashpath = move_to_database(self.cur_filepath, hashstr, self.factory.attachdir, ext=ext)

                self.cur_upload['type'] = 'finished-upload'
                self.cur_upload['path'] = hashpath

                outpath = os.path.join(self.factory.attachdir, hashpath)
                json.dump(self.cur_upload, open("%s.meta.json" % (outpath), 'w'))
                
                self.sendMessage(json.dumps({"type": "upload-finished", "id": self.cur_id, "path": hashpath}))
                self.factory.onupload(self.cur_upload)

            else:
                self.sendMessage(json.dumps({"type": "got-chunk", "id": self.cur_id, "size": self.cur_size}))


def move_to_database(filename, hashstr, attachdir, ext=None):
    if ext is None:
        _r, ext = os.path.splitext(filename)

    hashpath = os.path.join(hashstr[:2], "%s%s" % (hashstr[2:], ext))

    # Move to final location
    outpath = os.path.join(attachdir, hashpath)

    try:
        os.makedirs(os.path.dirname(outpath))
    except OSError:
        pass

    shutil.move(filename, outpath)
    
    return hashpath
