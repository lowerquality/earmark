import attachments
import minidb
import transcribe
import numm3

from autobahn.twisted.websocket import WebSocketServerProtocol, \
                                       WebSocketServerFactory
from autobahn.twisted.resource import WebSocketResource
from twisted.web.resource import Resource
from twisted.web.static import File
from twisted.web.server import Site, NOT_DONE_YET
from twisted.internet import reactor

import json
import multiprocessing
from multiprocessing.pool import ThreadPool as Pool
import numpy as np
import os
import shutil
import tempfile
import time
import zipfile

from gentle.paths import get_resource
from gentle.standard_kaldi import Kaldi
import gentle.metasentence as metasentence
import gentle.language_model as language_model

# kaldi quirk...
proto_langdir = get_resource('PROTO_LANGDIR')
vocab_path = os.path.join(proto_langdir, "graphdir/words.txt")
with open(vocab_path) as f:
    vocab = metasentence.load_vocabulary(f)

class AudioConferenceFactory(WebSocketServerFactory):
    def __init__(self, resources, dbdir="db", db=None):
        WebSocketServerFactory.__init__(self, None)
        self.clients = {}       # peerstr -> client

        self.resources = resources
        
        self.db = db
        self.gen_hclg_filename = db.gen_hclg_filename if db else None

        self.rerunning = False

        self.dbdir = dbdir
        if not os.path.exists(dbdir):
            os.makedirs(dbdir)

    def register(self, client):
        self.clients[client.peer] = client

    def endSession(self, client, timestamp):
        pass

    def sendPreview(self, p, utt_idx, session_id):
        utt_id = "utt-%s-%d" % (session_id, utt_idx)

        # Update the preview doc if it exists
        doc = self.db.get(utt_id, {
            "_id": utt_id,
            "type": "utterance",
            "session": session_id,
            "utt-idx": utt_idx})
        doc[p["type"]] = p["text"]

        self.db.onchange(None, {"type": "change",
                                "id": utt_id,
                                "doc": doc})

    def sendResult(self, res, utt_idx, session_id):
        utt_id = "utt-%s-%d" % (session_id, utt_idx)

        # Update the preview doc
        doc = self.db[utt_id]

        #print 'got utt alignment'
        doc[res["type"] + "_words"] = res["words"]
        #del doc[res["type"]]

        if 'duration' in res:
            doc['duration'] = res['duration']

            # having a duration also implies that the wave file is
            # ready; import to the database
            reactor.callInThread(self._put_attachment, doc)
        else:
            self.db.onchange(None, {"type": "change",
                                    "id": utt_id,
                                    "doc": doc})

        # make sure "start" time is set on utterances
        self.ensure_start_times(session_id)
        
        self.check_pending_audio_commands()

    def re_run_everything(self):
        if self.rerunning:
            print 'already running...'
            return

        self.rerunning = True
        
        utts = []
        for sess in self.get_all_sessions():
            utts.extend(self.get_session_utterances(sess['_id']))

        # unleash the threads...
        p = Pool(multiprocessing.cpu_count())
        # TODO: would be good to have some sort of identifier so that
        # these jobs can be cancelled if new commands are added.
        print 'starting re_run_everything'
        p.map(self.re_run, utts)
        p.close()
        self.rerunning = False
        print 'finished'

    def re_run(self, utt):
        if 'wavpath' not in utt:
            return
        
        k = Kaldi(
            get_resource('data/nnet_a_gpu_online'),
            self.gen_hclg_filename,
            get_resource('PROTO_LANGDIR'))
        audio = numm3.sound2np(
            os.path.join(self.resources['attach'].attachdir, utt['wavpath']),
            nchannels=1,
            R=8000)
        k.push_chunk(audio.tostring())
        wds = k.get_final()
        k.stop()
        for wd in wds:
            del wd['phones']
        utt['command_words'] = wds
        utt['command'] = ' '.join([X['word'] for X in wds])
        
        reactor.callFromThread(self.db.onchange, None, {"type": "change",
                                                        "id": utt["_id"],
                                                        "doc": utt})
            

    def get_all_sessions(self):
        return [X for X in self.db.docs.values() if X.get("type") == 'session']
    
    def get_session_utterances(self, session_id):
        return sorted([X for X in self.db.docs.values() if X.get("type") == 'utterance' and X.get("session") == session_id], key=lambda x: x['utt-idx'])

    def ensure_start_times(self, session_id):
        utts = self.get_session_utterances(session_id)

        cur_start = 0       # secs
            
        for utt in utts:
            if not 'duration' in utt:
                # utterance not sufficiently processed
                return
                
            if not 'start' in utt:
                # Add and save start time
                utt['start'] = cur_start
                self.db.onchange(None, {"type": "change",
                                        "id": utt["_id"],
                                        "doc": utt})

            cur_start += utt['duration']
        
    def check_pending_audio_commands(self):
        # Check if there are pending audio commands that can now be processed
        for acmd in self.db._pending_audio_commands:
            
            utts = self.get_session_utterances(acmd["session"])

            cur_start = 0       # secs
            cur_words = []
            
            for utt in utts:
                if not ('transcript_words' in utt and 'command_words' in utt and 'duration' in utt):
                    # utterance not sufficiently processed
                    break

                if utt['start'] + utt['duration'] > acmd['start']:
                    cur_words.extend(
                        [X['word'] for X in utt['transcript_words'] if (X['start'] + utt['start']) >= acmd['start'] and (X['start'] + utt['start']) < acmd['end'] and X['word'][0] != '['])

                    if utt['start'] + utt['duration'] > acmd['end']:
                        print 'finishing acmd', acmd
                        
                        # ready to finish audio command
                        acmd['type'] = 'command'
                        acmd['text'] = ' '.join(cur_words)
                        # Save
                        self.db.onchange(None, {"type": "change",
                                                "id": acmd["_id"],
                                                "doc": acmd})
                        
                        # Remove command
                        self.db._pending_audio_commands.remove(acmd)
                        # See if there are any more...
                        return self.check_pending_audio_commands()

                cur_start += utt['duration']
                    
            

    def get_utt_savepath(self, uttdoc):
        # XXX: refactor

        # Returns the filepath in which transcribe.py will save the
        # utterance wave file
        sess = self.db[uttdoc['session']]
        
        return os.path.join(self.dbdir, sess['peer'], 'utt-%d.wav' % (uttdoc['utt-idx']))

            
    def _put_attachment(self, doc):

        doc['wavpath'] = self.resources['attach'].import_file(self.get_utt_savepath(doc))

        # Finish: send notification
        reactor.callFromThread(self.db.onchange, None, {"type": "change",
                                                        "id": doc["_id"],
                                                        "doc": doc})

class AudioConferenceProtocol(WebSocketServerProtocol):
    def __init__(self, *a, **kw):
        self.buf_idx=0
        WebSocketServerProtocol.__init__(self, *a, **kw)

    def onOpen(self):
        print 'on-open'
        self.factory.register(self)
        WebSocketServerProtocol.onOpen(self)

    def connectionLost(self, reason):
        WebSocketServerProtocol.connectionLost(self, reason)
        if hasattr(self, "trans_sess"):
            self.trans_sess.stop()

    def onMessage(self, payload, isBinary):
        if isBinary:
            arr = np.fromstring(payload, dtype=np.int16)
            self.trans_sess.feed(arr)

            self.buf_idx += 1
            self.sendMessage(json.dumps({"type": "confirm-payload", "buf_idx": self.buf_idx}))

        else:
            cmd = json.loads(payload)
            if cmd.get("type") == 'session-start':
                self.start_session(cmd)
            elif cmd.get("type") == "session-end":
                self.stop_session(cmd)

    def stop_session(self, cmd):
        self.trans_sess.stop()

    def start_session(self, cmd):
        server_time = time.time()
        session_id = "session-%s-%d" % (self.peer, int(server_time*1000))

        self.trans_sess = SocketTranscriptionSession(os.path.join(self.factory.dbdir, self.peer), self.factory, session_id, self.factory.gen_hclg_filename)
        print 'trans_sess', self.trans_sess

        self.cur_session_id = session_id

        self.factory.db.onchange(None, {"type": "change",
                                "id": session_id,
                                "doc": {"_id": session_id,
                                        "type": "session",
                                        "peer": self.peer,
                                        "c_time": cmd["timestamp"], # client start time
                                        "s_time": server_time}}) # server start time

        self.sendMessage(json.dumps({"type": "session-confirm",
                                     "session_id": session_id}))

                

class SocketTranscriptionSession(transcribe.Session):
    def __init__(self, outdir, factory, session_id, gen_hclg_filename=None):
        self.factory = factory
        self.session_id = session_id
        transcribe.Session.__init__(self, outdir, gen_hclg_filename)

    def onpreview(self, p, utt_idx):
        reactor.callFromThread(self.factory.sendPreview, p, utt_idx, self.session_id)

    def onresult(self, res, utt_idx):
        # simplify result
        for wd in res['words']:
            del wd['phones']
        reactor.callFromThread(self.factory.sendResult, res, utt_idx, self.session_id)

class CommandDatabase(minidb.DBFactory):
    # We will look for "type=command" documents, and from them
    # assemble and compile a language model

    def __init__(self, dbdir="db", subdir_resources=None):
        self._command_seqs = {} # id -> [ks]
        self.subdir_resources = subdir_resources

        self._pending_audio_commands = [] # list of docs waiting to be processed

        minidb.DBFactory.__init__(self, dbdir)

        # XXX: re-initialize command_seqs
        # The `update_inmem` process will not update from `self.db` on launch
        self._command_seqs = dict([(key, doc["_ks"]) for (key, doc) in self.docs.items() if doc.get("type") == "command"])

        self.gen_hclg_filename = self.create_language_model()

    def create_language_model(self):
        "sets and returns hclg_filename"

        gen_hclg_filename = language_model.make_bigram_language_model(self._command_seqs.values(), proto_langdir, conservative=True)

        # Overwrite old gen_hclg_filen
        if hasattr(self, 'gen_hclg_filename'):
            shutil.move(gen_hclg_filename, self.gen_hclg_filename)
            gen_hclg_filename = self.gen_hclg_filename

        return gen_hclg_filename

    def onchange(self, sender, change_doc):
        update = False
        if change_doc.get("doc", {}).get("type") == "command":
            # Save kaldi-sequence from the text
            seq = metasentence.MetaSentence(change_doc["doc"].get("text", ""), vocab).get_kaldi_sequence()
            change_doc["doc"]["_ks"] = seq
            self._command_seqs[change_doc["id"]] = seq
            # Set "sender" to None so that all peers get a change update
            sender = None
            update = True
        elif change_doc["type"] == 'delete' and change_doc["id"] in self._command_seqs:
            del self._command_seqs[change_doc["id"]]
            update = True
        elif change_doc.get("doc", {}).get("type") == "audio-command":
            print 'got new audio command', change_doc['doc']
            self._pending_audio_commands.append(change_doc["doc"])
            
            self.subdir_resources['factory'].check_pending_audio_commands()

        minidb.DBFactory.onchange(self, sender, change_doc)

        if update:
            self.create_language_model()
            reactor.callInThread(
                self.subdir_resources['factory'].re_run_everything)

class TranscodingAttachFactory(attachments.AttachFactory):
    def __init__(self, factory, db, *a, **kw):
        self.factory = factory
        self.db = db
        attachments.AttachFactory.__init__(self, *a, **kw)

    def onupload(self, upl):
        session_id = upl['path']

        # Start a session, if it seems to be a valid media file
        # XXX: this list is arbitrary; use MIME or smth
        if not session_id.split('.')[-1] in ['mp3', 'mp4', 'm4v', 'wav', 'aac', 'm4a', 'mkv', 'ogg', 'ogv', 'flac']:
            return
        
        self.db.onchange(None, {"type": "change",
                                "id": session_id,
                                "doc": {
                                    "_id": session_id,
                                    "type": "session",
                                    "peer": upl['filename'],
                                    "filename": upl['filename'],
                                    "s_time": time.time(),
                                    }})

        reactor.callInThread(self._process_upload, upl, session_id)

    def _process_upload(self, upl, session_id):
        path = os.path.join(self.db.dbdir, '_attachments', upl['path'])

        print 'processing', path

        # XXX: use a tempdir
        outdir = os.path.join(self.db.dbdir, upl['filename'])
        sess = SocketTranscriptionSession(outdir, self.factory, session_id, self.factory.gen_hclg_filename)

        BUF_LEN = 200
        for chunk in numm3.sound_chunks(path, nchannels=1, R=8000, chunksize=BUF_LEN):
            sess.feed(chunk)

        sess.stop()
        sess.join()

        # Clean up: remove the tempdir and the original upload
        os.removedirs(outdir)
        os.remove(path)

class DBZipper(Resource):
    def __init__(self, db):
        self.db = db
        Resource.__init__(self)

    def render_GET(self, req):
        reactor.callInThread(self._zip, req)
        return NOT_DONE_YET

    def _zip(self, req):
        outdir = tempfile.mkdtemp()
        dbname = self.db.dbdir.split('/')[-1]
        
        print 'zipping', outdir
        
        # Copy docs into index for offline operation
        docstr = json.dumps(self.db.docs)
        
        indexstr = open('www/command/index.html').read().replace('var db = new M.Database();',
                                                              'var _docs = %s;\nvar db = new M.Database(_docs);\nwindow.setTimeout(main,1000/10);\n' % (docstr))

        open(os.path.join(outdir, 'index.html'), 'w').write(indexstr)
        
        open(os.path.join(outdir, 'db.json'), 'w').write(docstr)
        with zipfile.ZipFile(outdir + '.zip',
                             "w",
                             zipfile.ZIP_DEFLATED,
                             allowZip64=True) as zf:
            # Write index
            zf.write(os.path.join(outdir, 'index.html'), os.path.join(dbname, 'index.html'))

            # Write some JS libs (TODO: move to a subdirectory)
            zf.write(os.path.join('www', 'command', 'minidb.js'), os.path.join(dbname, 'minidb.js'))
            zf.write(os.path.join('www', 'command', 'earmark.js'), os.path.join(dbname, 'earmark.js'))
            zf.write(os.path.join('www', 'command', 'act.js'), os.path.join(dbname, 'act.js'))
            zf.write(os.path.join('www', 'command', 'attachments.js'), os.path.join(dbname, 'attachments.js'))

            # Write db
            zf.write(os.path.join(outdir, 'db.json'),
                     os.path.join(dbname, 'db.json'))
            
            for root, _, filenames in os.walk(os.path.join(self.db.dbdir, '_attachments')):
                for name in filenames:
                    name = os.path.join(root, name)
                    name = os.path.abspath(name)
                    zf.write(name, os.path.join(dbname, 'attachments', name.split('/')[-2], name.split('/')[-1]))

        # Serve zip
        zipres = File("%s.zip" % (outdir))
        reactor.callFromThread(zipres.render, req)

        # TODO: delete tempfile/zipfile (how do I know when
        # zipres.render finishes?)
        
        
class SubdirectoryContexts(Resource):
    def __init__(self, dbrootdir="db", webdir="www/command", landingdir="www/landing"):
        self.dbrootdir = dbrootdir
        self.webdir = webdir

        self.file_resource = File(landingdir)

        Resource.__init__(self)

    def getChild(self, name, request):

        if request.method == "GET":
            # Let's assume that all file are either empty (-> index.html) or have a period in them.
            if len(name) == 0 or "." in name:
                return self.file_resource.getChild(name, request)

            else:
                print 'making db', name

                dbdir = os.path.join(self.dbrootdir, name)

                subdir_resources = {}

                dbfactory = CommandDatabase(dbdir, subdir_resources)
                dbfactory.protocol = minidb.DBProtocol
                dbws_resource = WebSocketResource(dbfactory)
                subdir_resources['db'] = dbfactory

                factory = AudioConferenceFactory(subdir_resources, dbdir, dbfactory)
                factory.protocol = AudioConferenceProtocol
                ws_resource = WebSocketResource(factory)
                subdir_resources['factory'] = factory

                attachdir = os.path.join(dbdir, "_attachments")
                attach = TranscodingAttachFactory(factory, dbfactory, attachdir=attachdir)
                attach.protocol = attachments.AttachProtocol
                subdir_resources['attach'] = attach

                attachhttp = File(attachdir)
                attws_resource = WebSocketResource(attach)

                zipper = DBZipper(dbfactory)

                root = File(self.webdir)
                dbhttp = File(dbdir)                
                root.putChild('_ws', ws_resource)
                root.putChild('_db', dbws_resource)
                root.putChild('_attach', attws_resource)
                root.putChild('db', dbhttp)
                root.putChild('attachments', attachhttp)
                root.putChild('download.zip', zipper)

                self.putChild(name, root)
                return root

        return Resource.getChild(self, name, request)



if __name__=='__main__':

    site = Site(SubdirectoryContexts())
    reactor.listenTCP(9559, site, interface='0.0.0.0')
    print 'http://localhost:9559'
    reactor.run()
