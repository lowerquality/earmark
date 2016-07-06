import json
from Queue import Queue, Empty
import numm3
import numpy as np
import os
import threading

from gentle import standard_kaldi
from gentle.paths import get_resource

N_TRANSCRIPTION_THREADS = 4
PREVIEW_LEN = 40                # ~1s

# Do a naive silence detection to split utterances into a suitable length
MIN_UTTERANCE_LEN = 200         # ~5s
MAX_UTTERANCE_LEN = 800         # ~20s
# Minimum number of quiet utterances to trigger a split
N_QUIET_UTTERANCES= 13          # ~1/3s

# XXX: All of the above assumes constant-length buffers, but different
# browsers may send chunks at slightly different lengths; the server
# should coerce to a common chunk length.


# For full transcription
kaldi_queue = Queue()
for i in range(N_TRANSCRIPTION_THREADS):
    kaldi_queue.put(standard_kaldi.Kaldi())

class Utterance:
    def __init__(self):#, outfile=None):
        self.chunks = Queue()
        self.results = Queue()
        self.stopped = False

        # Start thread
        t = threading.Thread(target=self.start)
        t.start()

    def feed(self, buf):
        self.chunks.put(buf)

    def stop(self):
        self.stopped = True

    def get_kaldi(self):
        return kaldi_queue.get()

    def start(self):
        k = self.get_kaldi()
        acc = []

        while True:
            try:
                chunk = self.chunks.get(timeout=0.5)
            except Empty:
                if self.stopped:
                    # Feed remainder
                    rem = len(acc) % PREVIEW_LEN
                    if rem > 0:
                        k.push_chunk(np.concatenate(acc[-rem:]).tostring())
                        self.get_preview(k)
                    break
                else:
                    # print self.outfile, 'waiting'
                    continue

            acc.append(chunk)

            if (len(acc) % PREVIEW_LEN) == 0:
                buf = np.concatenate(acc[-PREVIEW_LEN:])
                k.push_chunk(buf.tostring())
                self.get_preview(k)

        self.finish(k, acc)

    def get_preview(self, k):
        self.results.put({"type": "transcript", "text": k.get_partial()})

    def finish(self, k, acc):
        final = k.get_final()
        
        k.reset()
        kaldi_queue.put(k)

        self.results.put({"type": "transcript", "words": final})

class LanguageModelUtterance(Utterance):
    def __init__(self, outfile, gen_hclg_filename, results_queue):
        self.outfile = outfile
        self.gen_hclg_filename = gen_hclg_filename
        Utterance.__init__(self)

        # Overwrite results queue to be the same as the Utterance
        self.results = results_queue

    def get_kaldi(self):
        # In theory, we could preserve these instances through a
        # session.
        return standard_kaldi.Kaldi(
            get_resource('data/nnet_a_gpu_online'),
            self.gen_hclg_filename,
            get_resource('PROTO_LANGDIR'))

    def get_preview(self, k):
        self.results.put({"type": "command", "text": k.get_partial()})

    def finish(self, k, acc):
        # Save audio
        if len(acc) > 0 and self.outfile is not None:
            numm3.np2sound(np.concatenate(acc), self.outfile, R=8000)
        else:
            print 'empty?!', self.outfile
        self.stopped = True

        # Align
        final = k.get_final()
        self.results.put({"type": "command", "words": final, "duration": sum([len(X) for X in acc]) / 8000.0})
        
        k.stop()

class MultiUtterance:
    def __init__(self, utts):
        self.utts = utts
        self.results = self.utts[0].results

    def feed(self, buf):
        for u in self.utts:
            u.feed(buf)
    def stop(self):
        for u in self.utts:
            u.stop()

class Session:
    def __init__(self, outdir, gen_hclg_filename=None):
        self.gen_hclg_filename = gen_hclg_filename

        self.outdir = outdir
        if not os.path.exists(outdir):
            os.makedirs(outdir)
        
        self.stopped = False
        self.idx = 0
        self.utt_idx = 0
        self.sub_idx = 0        # idx within current utterance

        self.mean_rms = 0
        self.n_quiet_utts = 0

        self.utts = [self.next_utt()]

        self.t = threading.Thread(target=self.start)
        self.t.start()

    def next_utt(self):
        self.utt_idx += 1
        self.sub_idx = 0
        utt = Utterance()
        if not self.gen_hclg_filename:
            return utt
        else:
            command_utt = LanguageModelUtterance(os.path.join(self.outdir, 'utt-%d.wav' % (self.utt_idx - 1)), self.gen_hclg_filename, utt.results)
            return MultiUtterance([utt, command_utt])

    def feed(self, buf):
        self.idx += 1
        self.sub_idx += 1

        self.utts[-1].feed(buf)

        rms = (pow(buf.astype(float), 2)).mean()
        self.mean_rms = ((self.idx-1)*self.mean_rms + rms) / self.idx

        if self.sub_idx >= MIN_UTTERANCE_LEN and rms < self.mean_rms / 10:
            self.n_quiet_utts += 1
        else:
            self.n_quiet_utts = 0

        if self.n_quiet_utts >= N_QUIET_UTTERANCES or self.sub_idx >= MAX_UTTERANCE_LEN:
            self.utts[-1].stop()
            self.utts.append(self.next_utt())

    def onpreview(self, p, utt_idx):
        print "U-%d: %s" % (utt_idx, p["text"])

    def onresult(self, r, utt_idx):
        json.dump(r, open(os.path.join(self.outdir, 'utt-%d.json' % (utt_idx)), 'w'))

    def start(self):
        cur_utt_idx = 0

        hit_results = {}        # idx -> cnt

        while True:
            # lookahead
            for utt_idx, utt in [(X, self.utts[X]) for X in range(cur_utt_idx, len(self.utts))]:
                #ret = self.utts[cur_utt_idx].results.get()

                # Is the current utterance finished?
                if utt_idx == cur_utt_idx and hit_results.get(utt_idx, 0) == 2:
                    cur_utt_idx += 1
                    if cur_utt_idx >= len(self.utts):
                        # Session ended by calling `stop.'
                        return
                    
                try:
                    ret = utt.results.get(timeout=0.01)
                except Empty:
                    continue

                if 'words' in ret:
                    self.onresult(ret, utt_idx)
                    hit_results[utt_idx] = hit_results.get(utt_idx, 0) + 1
                else:
                    self.onpreview(ret, utt_idx)

    def stop(self):
        self.stopped = True
        self.utts[-1].stop()

    def join(self):
        self.t.join()

if __name__=='__main__':
    import sys
    # Simulate with an audio file
    AUDIOFILE = sys.argv[1]
    OUTDIR = sys.argv[2]

    sess = Session(OUTDIR)
    test_audio = numm3.sound2np(AUDIOFILE, nchannels=1, R=8000)

    cur_start = 0
    BUF_LEN = 200
    while True:
        sess.feed(test_audio[cur_start:cur_start+BUF_LEN])
        cur_start += BUF_LEN
        if cur_start >= len(test_audio):
            break

    sess.stop()
    sess.join()
