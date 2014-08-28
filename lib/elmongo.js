var request = require('request'),
    mongoose = require('mongoose'),
    util = require('util'),
    url = require('url'),
    helpers = require('./helpers'),
    sync = require('./sync');

// turn off request pooling
request.defaults({ agent:false });

// cache elasticsearch url options for elmongo.search() to use
var elasticUrlOptions = null;

/**
 * Attach mongoose plugin for elasticsearch indexing
 *
 * @param  {Object} schema      mongoose schema
 * @param  {Object} options     elasticsearch options object. Keys: host, port, index, type
 */
module.exports = elmongo = function (schema, options) {

  // attach methods to schema
  schema.methods.index = index;
  schema.methods.unindex = unindex;

  schema.statics.sync = function (cb) {
    options = helpers.mergeModelOptions(options, this);

    return sync.call(this, schema, options, cb);
  };

  // attach mongoose middleware hooks
  schema.post('save', function () {
    options = helpers.mergeModelOptions(options, this);
    try{
      this.index(options);
    }catch(err){
      // ignore if there is currently no way to index
    }
  });
  schema.post('remove', function () {
    options = helpers.mergeModelOptions(options, this);
    try{
      this.unindex(options);
    }catch(err){
      // ignore if there is currently no way to unindex
    }
  });
};

/**
 * Index a document in elasticsearch (create if not existing)
 *
 * @param  {Object} options     elasticsearch options object. Keys: host, port, index, type
 */
function index (options) {
  var self = this;
  // strip mongoose-added functions, depopulate any populated fields, and serialize the doc
  var esearchDoc = helpers.serializeModel(this);

  var indexUri = helpers.makeDocumentUri(options, self);

  var reqOpts = {
    method: 'PUT',
    url: indexUri,
    body: JSON.stringify(esearchDoc)
  };

  // console.log('index:', indexUri)

  helpers.backOffRequest(reqOpts, function (err, res, body) {
    if (err) {
      var error = new Error('Elasticsearch document indexing error: '+util.inspect(err, true, 10, true));
      error.details = err;

      self.emit('error', error);
      return;
    }

    self.emit('elmongo-indexed', body);
  });
}

/**
 * Remove a document from elasticsearch
 *
 * @param  {Object} options     elasticsearch options object. Keys: host, port, index, type
 */
function unindex (options) {
  var self = this;

  var unindexUri = helpers.makeDocumentUri(options, self);

  // console.log('unindex:', unindexUri)

  var reqOpts = {
    method: 'DELETE',
    url: unindexUri
  };

  helpers.backOffRequest(reqOpts, function (err, res, body) {
    if (err) {
      var error = new Error('Elasticsearch document index deletion error: '+util.inspect(err, true, 10, true));
      error.details = err;

      self.emit('error', error);
      return;
    }

    self.emit('elmongo-unindexed', body);
  });
}

