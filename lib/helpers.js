var request = require('request'),
    mongoose = require('mongoose'),
    ObjectId = mongoose.Types.ObjectId,
    util = require('util'),
    url = require('url');

/**
 * Sends an http request using `reqOpts`, calls `cb` upon completion.
 * Upon ECONNRESET, backs off linearly in increments of 500ms with some noise to reduce concurrency.
 *
 * @param  {Object}   reqOpts   request options object
 * @param  {Function} cb        Signature: function (err, res, body)
 */
exports.backOffRequest = function (reqOpts, cb) {
  var maxAttempts = 3;
  var backOffRate = 500;

  function makeAttempts (attempts) {
    attempts++;

    request(reqOpts, function (err, res, body) {
      if(err){
        if((err.code === 'ECONNRESET' || err.code === 'EPIPE' || err.code === 'ETIMEDOUT') && attempts <= maxAttempts){
          var waitTime = backOffRate*attempts+Math.random()*backOffRate;

          setTimeout(function () {
            makeAttempts(attempts);
          }, waitTime);
          return;
          }else{
            var error = new Error('elasticsearch request error: '+err);
            error.details = err;
            error.attempts = attempts;
            error.reqOpts = reqOpts;

            return cb(error);
          }
        }

      // parse the response body as JSON
      try {
        var parsedBody = JSON.parse(body);
      } catch (parseErr) {
        var error = new Error('Elasticsearch did not send back a valid JSON reply: '+util.inspect(body, true, 10, true));
        error.elasticsearchReply = body;
        error.reqOpts = reqOpts;
        error.details = parseErr;

        return cb(error);
      }

      // success case
      return cb(err, res, parsedBody);
    });
  }

  makeAttempts(0);
};

/**
 * Performs deep-traversal on `thing` and converts
 * any object ids to hex strings, and dates to ISO strings.
 *
 * @param  {Any type} thing
 */
exports.serialize = function (thing) {
  if (Array.isArray(thing)) {
    return thing.map(exports.serialize);
  } else if (thing instanceof ObjectId) {
    return thing.toHexString();
  } else if (thing instanceof Date) {
    return thing.toISOString();
  } else if (typeof thing === 'object' && thing !== null) {
    Object
    .keys(thing)
    .forEach(function (key) {
        thing[key] = exports.serialize(thing[key]);
    });
    return thing;
  } else {
    return thing;
  }
};

/**
 * Serialize a mongoose model instance for elasticsearch.
 *
 * @param  {Mongoose model instance} model
 * @return {Object}
 */
exports.serializeModel = function (model) {
  // strip mongoose-added functions, and depopulate any populated model references
  var deflated = model.toObject({ depopulate: true });
  return exports.serialize(deflated);
};

/**
 * Merge user-supplied `options` object with defaults (to configure Elasticsearch url)
 * @param  {Object} options
 * @return {Object}
 */
exports.mergeOptions = function (options) {
  // default options
  var defaultOptions = {
    protocol: 'http',
    host: 'localhost',
    port: 9200,
    prefix: '',
    index: 'default',
    type: 'default',
  };

  if (!options) {
    return defaultOptions;
  }

  // if user specifies an `options` value, ensure it's an object
  if (typeof options !== 'object') {
    throw new Error('elmongo options was specified, but is not an object. Got:'+util.inspect(options, true, 10, true));
  }

  var mergedOptions = {};

  if (options.url) {
    // node's url module doesn't parse imperfectly formed URLs sanely.
    // use a regex so the user can pass in urls flexibly.

    // Rules:
    // url must specify at least host and port (protocol falls back to options.protocol or defaults to http)
    // if `host`, `port` or `protocol` specified in `options` are different than those in url, throw.
    var rgxUrl = /^((http|https):\/\/)?(.+):([0-9]+)/;
    var urlMatch = rgxUrl.exec(options.url);

    if (!urlMatch) {
      throw new Error('url from `options` must contain host and port. url: `'+options.url+'`.');
    }

    // if no protocol in url, default to options protocol, or http
    var protocol = urlMatch[2];
    if (protocol && options.protocol && protocol !== options.protocol) {
      // user passes in `protocol` and a different protocol in `url`.
      throw new Error('url specifies different protocol than protocol specified in `options`. Pick one to use in `options`.');
    }
    mergedOptions.protocol = protocol || options.protocol || defaultOptions.protocol;

    var hostname = urlMatch[3];
    if (!hostname) {
      // hostname must be parseable from the url
      throw new Error('url from `options` must contain host and port. url: `'+options.url+'`.');
    }
    mergedOptions.host = hostname;

    var port = urlMatch[4];
    if (!port) {
      // port must be specified in url
      throw new Error('url from `options` must contain host and port. url: `'+options.url+'`.');
    }

    if (port && options.port && port !== options.port) {
      // if port is specified in `options` too, and its a different value, throw.
      throw new Error('url specifies different port than port specified in `options`. Pick one to use in `options`.');
    }
    mergedOptions.port = port;

    mergedOptions.prefix = typeof options.prefix === 'string' ? options.prefix : '';
  } else {
    Object.keys(defaultOptions).forEach(function (key) {
      mergedOptions[key] = options[key] || defaultOptions[key];
    });
  }

  mergedOptions.index = options.index || defaultOptions.index;
  mergedOptions.type = options.type || defaultOptions.type;

  return mergedOptions;
};

/**
 * Merge the default elmongo collection options with the user-supplied options object
 *
 * @param  {Object} options (optional)
 * @param  {Object}
 * @return {Object}
 */
exports.mergeModelOptions = function (options, model) {
  var mergedOptions = exports.mergeOptions(options);

  return mergedOptions;
};


/**
 * Make index name (with prefix) from `options`
 *
 * @param  {Object} options
 * @return {String}
 */
exports.makeIndexName = function (options) {
  return options.prefix ? (options.prefix + '-' + options.index) : options.index;
};

/**
 * Form the elasticsearch URI for indexing/deleting a document
 *
 * @param  {Object} options
 * @param  {Mongoose document} doc
 * @return {String}
 */
exports.makeDocumentUri = function (options, doc) {
  var typeUri = exports.makeTypeUri(options);
  var docUri = typeUri+'/'+doc._id;

  return docUri;
};

/**
 * Form the elasticsearch URI up to the type of the document
 *
 * @param  {Object} options
 * @return {String}
 */
exports.makeTypeUri = function (options) {
  var indexUri = exports.makeIndexUri(options);
  var typeUri = indexUri + '/' + options.type;

  return typeUri;
};

/**
 * Form the elasticsearch URI up to the index of the document (index is same as type due to aliasing)
 * @param  {Object} options
 * @return {String}
 */
exports.makeIndexUri = function (options) {
  var domainUri = exports.makeDomainUri(options);
  var indexName = exports.makeIndexName(options);
  var indexUri = domainUri + '/' + indexName;

  return indexUri;
};

exports.makeDomainUri = function (options) {
  var domainUri = url.format({
    protocol: options.protocol,
    hostname: options.host,
    port: options.port
  });

  return domainUri;
};

exports.makeAliasUri = function (options) {
  var domainUri = exports.makeDomainUri(options);
  var aliasUri = domainUri + '/_aliases';

  return aliasUri;
};

exports.makeBulkIndexUri = function (options) {
  var domainUri = exports.makeDomainUri(options);
  var bulkIndexUri = domainUri + '/' + options.index + '/_bulk';

  return bulkIndexUri;
};

// Checks that a response body from elasticsearch reported success
exports.elasticsearchBodyOk = function (elasticsearchBody) {
  console.log(elasticsearchBody);
  return elasticsearchBody && (elasticsearchBody.ok || elasticsearchBody.acknowledged);
};

exports.elasticsearchIndexExists = function (elasticsearchBody) {
  console.log(elasticsearchBody);
  return elasticsearchBody && (elasticsearchBody.status === 400);
};
