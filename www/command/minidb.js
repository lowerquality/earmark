var M = M || {};

(function($) {

    function basepath() {
        return window.location.host + '/' + window.location.pathname.split('/')[1] + '/';
    }

    $.Database = function(offline_docs) {
        this._docs = offline_docs || {};        // id -> doc

        if(offline_docs) {
            // Offline!
            this.offline = true;
            this.socket = {
                "send": function() {}
            }
        }
        else {
            // Connect
            var proto = window.location.protocol;
            var wsproto = 'ws://';
            if(proto[proto.length-2] == 's') {
                wsproto = 'wss://';
            }

            var wsurl = wsproto + basepath() + "_db";
            this.socket = new WebSocket(wsurl);
            this.socket.onmessage = this._onmessage.bind(this)
            this.socket.onclose = this._onclose.bind(this)
        }
    }
    $.Database.prototype.items = function() {
        return Object.keys(this._docs)
            .map(function(k) { return this._docs[k]; }, this);
    }
    $.Database.prototype.onload = function() {
        // Called initially when docs are populated
        console.log("db loaded!");
    }
    $.Database.prototype.onupdate = function(doc) {
    }
    $.Database.prototype.ondelete = function(doc) {
    }
    
    $.Database.prototype.deletedoc = function(id) {
        this.ondelete(this._docs[id]);
        
        delete this._docs[id];
        this.socket.send(JSON.stringify({
            "type": "delete",
            "id": id}));
    }
    $.Database.prototype.next_id = function() {
        var uid = null;
        while(!uid || uid in this._docs) {
            uid = 'id_' + Math.floor(Math.random()*100000);
        }
        return uid;
    }
    $.Database.prototype.updatedoc = function(doc) {
        doc._id = doc._id || this.next_id();
        
        this._docs[doc._id] = doc;

        this.onupdate(doc);

        this.socket.send(JSON.stringify({
            "type": "change",
            "id": doc._id,
            "doc": doc
        }));
    }

    $.Database.prototype._onmessage = function(e) {
        var res = JSON.parse(e.data);
        if(res.type == 'history') {
            this._docs = res.history;
            this.onload();
        }
        else if(res.type == "delete") {
            this.ondelete(this._docs[res.id]);
            delete this._docs[res.id];
        }
        else {
            this._docs[res.id] = res.doc;
            this.onupdate(res.doc);
        }
    }
    $.Database.prototype._onclose = function() {
        alert("connection to database closed. will reload");
        window.location.reload();
    }

})(M);
