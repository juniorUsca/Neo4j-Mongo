var events = require("events"),
    mongo = require("mongoskin");

var OplogWatcher = module.exports = function OplogWatcher(options) {
  events.EventEmitter.call(this);

  this._db = mongo.db([options.host || "localhost", options.oplogDb || "local"].join("/"), {safe: true});
  this._collection = this._db.collection(options.oplogCollection || "oplog.rs");

  var self = this;

  var openLog = function openLog() {
    var q = {
      ts: {
        $gt: new mongo.BSONPure.Timestamp(0, options.since || (Date.now() / 1000)),
      },
    };

    var q_drop = {
      ts: {
        $gt: new mongo.BSONPure.Timestamp(0, options.since || (Date.now() / 1000)),
      },
    };

    if (options.ns) {
      q.ns = options.ns;
    }
    if (options.map_reduce) {
      // options.ns = "test.tmp.mr"
      q.ns = {
        '$regex': ".*"+options.ns+".*"
      };
      console.info(q.ns);
    }

    if (options.ns_drop) {
      // options.ns_drop = "test"
      q_drop.ns = options.ns_drop + ".$cmd";
    }

    self._collection.find(q, {tailable: true}, function(err, cursor) {
      if (err) {
        return self.emit("error", err);
      }

      var cursorStream = cursor.stream();

      cursorStream.on("data", function(doc) {
        switch (doc.op) {
          case "i": {
            self.emit("insert", doc.o);
            break;
          }
          case "u": {
            self.emit("update", doc.o);
            break;
          }
          case "d": {
            self.emit("delete", doc.o._id);
            break;
          }
        }
      });

      cursorStream.on("error", function(err) {
        self.emit("error", err);
        setImmediate(openLog);
      });

      cursorStream.on("end", function() {
        setImmediate(openLog);
      });
    });

    /// LISTEN QUERY TO DROP DATA
    self._collection.find(q_drop, {tailable: true}, function(err, cursor) {
      if (err) {
        return self.emit("error", err);
      }

      var cursorStream = cursor.stream();

      var ns__ = options.ns;
      var sub_ns = '' + ns__.substring(ns__.indexOf('.')+1);

      cursorStream.on("data", function(doc) {
        if (doc.o.create) {
          //console.info( doc.o );
          
          if ( doc.o.create.indexOf(sub_ns) >= 0 )
            //console.info( doc.o );
            self.emit("create", doc.o);
        }
        
        /*switch (doc.op) {
          case "i": {
            self.emit("insert", doc.o);
            break;
          }
          case "u": {
            self.emit("update", doc.o);
            break;
          }
          case "d": {
            self.emit("delete", doc.o._id);
            break;
          }
          case "c": {
            self.emit("delete", doc.o._id);
            break;
          }
        }*/
      });

      cursorStream.on("error", function(err) {
        self.emit("error", err);
        setImmediate(openLog);
      });

      cursorStream.on("end", function() {
        setImmediate(openLog);
      });
    });


  };

  setImmediate(openLog);
};
OplogWatcher.prototype = Object.create(events.EventEmitter.prototype, {constructor: {value: OplogWatcher}});
