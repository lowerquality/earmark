(function($) {

    $.Earmark = function(socket_url) {
        this.socket_url = socket_url;


        this.session_id = null;

        this.outgoing_idx = 0;  // number of buffers sent to server
        this.incoming_idx = 0;  // buffers bounced back from server
    }
    $.Earmark.prototype.start = function() {
        this._connect(function() {
            this.socket.onmessage = this._onmessage.bind(this)
            this._start_recording();
        }.bind(this));
    }
    $.Earmark.prototype.stop = function() {

        this.session_id   = null;
        
        // Release mic
        this.stream.getTracks()[0].stop();
        // ...audio ctx
        this.audio_ctx.close();

        // We may not want to close the socket until we're sure we've
        // sent all of the audio buffers we have, ie. until
        // incoming_idx == outgoing_idx.
        //
        // Instead, we will close it in the _onmessage callback if
        // session_id is null and incoming_idx == outgoing_idx.

        if(this.incoming_idx == this.outgoing_idx) {
            this._finish_stop();
        }
    }
    $.Earmark.prototype.buf_idx_to_time = function(buf_idx) {
        return (buf_idx * this.chunk_nframes) / 8000;
    }

    $.Earmark.prototype._finish_stop = function() {
        this.incoming_idx = 0;
        this.outgoing_idx = 0;
        
        this.socket.close()
    }
    $.Earmark.prototype._connect = function(cb) {
        this.socket = new WebSocket(this.socket_url);
        this.socket.binaryType = "arraybuffer";
        this.socket.onopen = cb;
    }
    $.Earmark.prototype._onmessage = function(e) {
        if(typeof e.data == 'string') {
            var req = JSON.parse(e.data);

            if (req.type == 'confirm-payload'){
                this.incoming_idx = req.buf_idx;
                if(this.session_id == null && this.incoming_idx == this.outgoing_idx) {
                    this._finish_stop();
                }
            }
            else if(req.type == 'session-confirm') {
                this.session_id = req['session_id'];
            }
            else {
                console.log("unknown message", req);
            }
        }
    }
    $.Earmark.prototype._start_recording = function() {
        // Note: navigator.getUserMedia will soon change to MediaDevices.getUserMedia()
        // https://developer.mozilla.org/en-US/docs/Web/API/MediaDevices/getUserMedia#Browser_compatibility
        var gUM = (navigator.getUserMedia ||
                   navigator.webkitGetUserMedia ||
                   navigator.mozGetUserMedia ||
                   navigator.msGetUserMedia).bind(navigator);
        if(!gUM) {
            alert("Error: no 'getusermedia' support in your browser.");
        }
        
        gUM({audio: true},
	    this._stream_mic_to_socket.bind(this),
            function(err) {
	        alert("Error: could not open an audio stream");
	        console.log(err);
	    })
    }
    $.Earmark.prototype._stream_mic_to_socket = function(stream) {
        this.socket.send(JSON.stringify({
            type: "session-start",
            timestamp: (new Date().getTime())/1000}));
        
        this.stream = stream;
        
        var audio_context = new AudioContext();
        this.audio_ctx = audio_context;
        this.srate = audio_context.sampleRate;

        var mic_source = audio_context.createMediaStreamSource(stream);

        var script_proc = audio_context.createScriptProcessor(1024, 1, 1);
        mic_source.connect(script_proc);
        script_proc.connect(audio_context.destination);

        script_proc.onaudioprocess = function(ev) {
	    var inp = ev.inputBuffer.getChannelData(0);

            if(!this.chunk_nframes) {
                this.chunk_nframes_fullrate = inp.length;
                this.chunk_nframes = Math.ceil(inp.length * (8000 / this.srate));
            }

	    // Send mic_input over websockets, downsampled to 8khz
	    var nsamples = this.chunk_nframes;
        
	    var abuffer = new ArrayBuffer(nsamples*2);
	    var arr = new Int16Array(abuffer);

	    for(var i=0; i<arr.length; i++) {
	        var idx = i * (this.srate/8000);
                
	        var a = Math.floor(idx);
	        var b = Math.ceil(idx);
            
	        var w_b = idx % 1;
	        var w_a = (1-w_b);

	        // Linear interpolation and conversion to int16
	        arr[i] = (inp[a]*w_a + inp[b]*w_b) * Math.pow(2,15);
	    }

            this.outgoing_idx += 1;
	    this.socket.send(abuffer);
        }.bind(this)
    };
})(window);
