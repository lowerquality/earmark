var A = A || {};
(function($) {

    $.Elt = function(nodename, attrs) {
        this._children = [];
        this._name = nodename;
        this._attrs = attrs || {};
        this._id = this._attrs.id || this.getHash();
        if(this._attrs.parent) {
            this._attrs.parent.addChild(this);
        }
    }
    $.Elt.prototype.addChild = function(c) {
        this._children.push(c);
    }
    $.Elt.prototype.renderElt = function(node) {
        // Render an element, or updates a node's existing $el if
        // appropriate.

        node = node || {};

        this.$el = node.$el || document.createElement(this._name);
        this.$text = node.$text || document.createTextNode("");
        if(!node.$text) {
            this.$el.appendChild(this.$text);
        }

        // Only re-render when there's been a change
        if(Object.keys(node).length == 0 || node.getHash() != this.getHash()) {
            this.getElt(this.$el, this.$text);
        }
    }
    $.Elt.prototype.getChildren = function() {
        // Returns this node's children
        return this._children;
    }
    $.Elt.prototype.update = function(node) {
        // Updates a node's state, if given
        this.renderElt(node);

        // Compare children by ID
        var node_children = node ? node.getChildren() : [];
        var node_children_by_id = {};
        node_children.forEach(function(c) {
            node_children_by_id[c._id] = c;
        })


        var cur_node_idx = -1;

        this.getChildren()
            .forEach(function(my_c) {

                // Recursively update
                my_c.update(node_children_by_id[my_c._id]);

                if(my_c._id in node_children_by_id) {
                    // Element was updated: indicate by removing from `node_children_by_id`
                    
                    var node_idx = node_children.indexOf(node_children_by_id[my_c._id]);
                    if(node_idx < cur_node_idx) {
                        // out of order: re-insert before next item
                        this.$el.insertBefore(my_c.$el, (node_children[cur_node_idx+1] || {}).$el);
                    }
                    else {
                        cur_node_idx = node_idx;
                    }
                    
                    delete node_children_by_id[my_c._id];
                }
                else {
                    // New element: append to DOM before the next item
                    this.$el.insertBefore(my_c.$el, (node_children[cur_node_idx+1] || {}).$el);
                }
            }, this);

        // Remove a node's former children
        Object.keys(node_children_by_id)
            .forEach(function(id) {
                this.$el.removeChild(node_children_by_id[id].$el);
            }, this);
    }

    
    $.Elt.prototype.getHash = function() {
        // Returns a unique string for this element
        return "<" + this._name +
            (this._attrs.attrs ? " " + dictHash(this._attrs.attrs) : "" )+
            (this._attrs.classes ? ' className="' + listHash(this._attrs.classes) + '"' : "") +
            (this._attrs.id ? ' id="' + this._attrs.id + '"' : "") +
            // (this is not quite correct for styles)
            (this._attrs.styles ? ' style="' + dictHash(this._attrs.styles) + '"' : "") +
            (this._attrs.events ? " " + dictHash(this._attrs.events, functionquote) : "") + ">" +
            (this._attrs.text || "") +
            "</" + this._name + ">";
    }
    $.Elt.prototype.getElt = function($el, $txt) {

        if(this._attrs.id) {
            $el.id = this._attrs.id;
        }

        // XXX: will not remove attributes
        Object.keys(this._attrs.attrs || {})
            .forEach(function(k) {
                $el.setAttribute(k, this._attrs.attrs[k]);
            }, this);
        
        if(this._attrs.classes) {
            $el.className = listHash(this._attrs.classes);
        }
        
        // & will not un-set styles
        Object.keys(this._attrs.styles || {})
            .forEach(function(k) {
                $el.style[k] = this._attrs.styles[k];
            }, this);
        // ibid
        Object.keys(this._attrs.events || {})
            .forEach(function(k) {
                $el[k] = this._attrs.events[k];
            }, this);

        $txt.textContent = this._attrs.text || "";

        return $el;
    }


    $.Root = function() {
        $.Elt.call(this, "body", {});
    }
    $.Root.prototype = new $.Elt;
    $.Root.prototype.renderElt = function(node) {
        node = node || {};

        this.$el = node.$el || document.body;
        this.$text = node.$text || document.createTextNode("");
        if(!node.$text) {
            this.$el.appendChild(this.$text);
        }
    }
    

    function listHash(x) {
        x.sort();
        return x.map(function(v) {
            return v;
        }).join(" ");
    }
    function dictHash(x, fn) {
        fn = fn || function(x) { return x;}; // (identity)
        
        var keys = Object.keys(x);
        keys.sort();
        return keys
            .map(function(k) {
                return k + '="' + fn(x[k]) + '"';
            }).join(" ");
    }

    function quote(x) {
        return x.replace(/"/g, '\\"');
    }

    function functionquote(x) {
        return quote(x.toString());
    }

})(A);
