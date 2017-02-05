
var async = require('async');
var mysql = require('mysql');
var _ = require('underscore');
var request = require('request');
var noop = function(){};
var logPrefix = '[nodebb-plugin-import-ubb]';

(function(Exporter) {

	Exporter.setup = function(config, callback) {
		Exporter.log('setup');

		// mysql db only config
		// extract them from the configs passed by the nodebb-plugin-import adapter
		var _config = {
			host: config.dbhost || config.host || 'localhost',
			user: config.dbuser || config.user || 'root',
			password: config.dbpass || config.pass || config.password || '',
			port: config.dbport || config.port || 3306,
			database: config.dbname || config.name || config.database || 'flarum',
      url: config.custom.url || '',
      download: config.custom.download || false
		};

		Exporter.config(_config);
		Exporter.config('prefix', config.prefix || config.tablePrefix || 'fl1_');

		Exporter.connection = mysql.createConnection(_config);
		Exporter.connection.connect();

		callback(null, Exporter.config());
	};

	Exporter.getUsers = function(callback) {
		return Exporter.getPaginatedUsers(0, -1, callback);
	};

	Exporter.getPaginatedUsers = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;

		var err;
		var prefix = Exporter.config('prefix');
    var url = Exporter.config('url');
    var download = Exporter.config('download');
		var query = 'SELECT '
				+ prefix + 'users.id as _uid, '
				+ prefix + 'users.username as _username, '
        + prefix + 'users.email as _email, '
        + prefix + 'users.bio as _signature, '
        + prefix + 'users.avatar_path as _picture, '
        + prefix + 'users.join_time as _joindate, '
        + prefix + 'users.last_seen_time as _lastonline, '
        + 'GROUP_CONCAT(' + prefix + 'users_groups.group_id) as _groups '
				+ 'FROM ' + prefix + 'users '
				+ 'LEFT JOIN ' + prefix + 'users_groups ON ' + prefix + 'users_groups.user_id = ' + prefix + 'users.id '
        + 'GROUP BY ' + prefix + 'users.id '
				+ (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');


		if (!Exporter.connection) {
			err = {error: 'MySQL connection is not setup. Run setup(config) first'};
			Exporter.error(err.error);
			return callback(err);
		}

		Exporter.connection.query(query,
				function(err, rows) {
					if (err) {
						Exporter.error(err);
						return callback(err);
					}

					//normalize here
					var map = {};
          var i = rows.length;

					rows.forEach(function(row) {
						// from unix timestamp (s) to JS timestamp (ms)
						//row._joindate = ((row._joindate || 0) * 1000) || startms;

						// lower case the email for consistency
						row._email = (row._email || '').toLowerCase();
            row._joindate = row._joindate ? Date.parse(row._joindate) : null;
            row._lastonline = row._lastonline ? Date.parse(row._lastonline) : null;
						// I don't know about you about I noticed a lot my users have incomplete urls, urls like: http://
            if(row._picture) {
						  row._picture = Exporter.validateUrl(url + '/assets/avatars/' + row._picture);
            }
            row._website = Exporter.validateUrl(row._website);
            if(row._groups) {
              row._groups = row._groups.split(',');
            }
						map[row._uid] = row;

            if(download && row._picture) {
              Exporter.log("Downloading " + row._picture);
              request({url: row._picture, encoding:null}, function(error , response, body) {
                row._pictureBlob = body;
                i--;
              });
              delete row._picture;
            } else
              i--;
            if(i == 0)
              callback(null, map);
					});
				});
	};

	Exporter.getGroups = function(callback) {
		return Exporter.getPaginatedGroups(0, -1, callback);
	};

	Exporter.getPaginatedGroups = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;

		var err;
		var prefix = Exporter.config('prefix');
		var query = 'SELECT '
				+ prefix + 'groups.id as _gid, '
				+ prefix + 'groups.name_singular as _name '
				+ 'FROM ' + prefix + 'groups '
				+ (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');
		if (!Exporter.connection) {
			err = {error: 'MySQL connection is not setup. Run setup(config) first'};
			Exporter.error(err.error);
			return callback(err);
		}

		Exporter.connection.query(query,
				function(err, rows) {
					if (err) {
						Exporter.error(err);
						return callback(err);
					}
					//normalize here
					var map = {};
					rows.forEach(function(row) {
						map[row._gid] = row;
					});

					callback(null, map);
				});
	};

	Exporter.getCategories = function(callback) {
		return Exporter.getPaginatedCategories(0, -1, callback);
	};
	Exporter.getPaginatedCategories = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;

		var err;
		var prefix = Exporter.config('prefix');
		var startms = +new Date();
		var query = 'SELECT '
				+ 'id as _cid, '
				+ 'name as _name, '
        + 'slug as _slug, '
        + 'description as _description, '
        + 'color as _bgColor, '
        + 'position as _order, '
        + 'parent_id as _parentCid '
				+ 'FROM ' + prefix + 'tags '
				+ (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');


		if (!Exporter.connection) {
			err = {error: 'MySQL connection is not setup. Run setup(config) first'};
			Exporter.error(err.error);
			return callback(err);
		}

		Exporter.connection.query(query,
				function(err, rows) {
					if (err) {
						Exporter.error(err);
						return callback(err);
					}

					//normalize here
					var map = {};
					rows.forEach(function(row) {
						row._description = row._description || 'No description available';

						map[row._cid] = row;
					});

					callback(null, map);
				});
	};

  function removeMarkup(i) {
    return i.replace(/<([^<|>]*)>/g , '').replace(/(@\w+)#(\d+)/g , '$1');
  }

	Exporter.getTopics = function(callback) {
		return Exporter.getPaginatedTopics(0, -1, callback);
	};
	Exporter.getPaginatedTopics = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;

		var err;
		var prefix = Exporter.config('prefix');
		var query =
				'SELECT '
        + prefix + 'discussions.id as _tid, '
        + prefix + 'discussions.start_user_id as _uid, '
        + prefix + 'discussions.title as _title, '
        + prefix + 'discussions.slug as _slug, '
        + prefix + 'discussions.is_sticky as _pinned, '
        + prefix + 'discussions.is_locked as _locked, '
				+ prefix + 'discussions_tags.tag_id as _cid, '
        + prefix + 'posts.content as _content, '
        + prefix + 'posts.time as _timestamp, '
        + prefix + 'posts.edit_time as _edited, '
        + prefix + 'posts.ip_address as _ip  '
				+ 'FROM ' + prefix + 'discussions '
				+ 'JOIN ' + prefix + 'discussions_tags ON ' + prefix + 'discussions_tags.discussion_id=' + prefix + 'discussions.id '
        + 'LEFT JOIN ' + prefix + 'posts ON ' + prefix + 'posts.id=' + prefix + 'discussions.start_post_id '
				+ (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');
		if (!Exporter.connection) {
			err = {error: 'MySQL connection is not setup. Run setup(config) first'};
			Exporter.error(err.error);
			return callback(err);
		}

		Exporter.connection.query(query,
				function(err, rows) {
					if (err) {
						Exporter.error(err);
						return callback(err);
					}

					//normalize here
					var map = {};

					rows.forEach(function(row) {
						row._title = row._title ? row._title[0].toUpperCase() + row._title.substr(1) : 'Untitled';
            row._edited = row._edited ? Date.parse(row._edited) : null;
            row._timestamp = row._timestamp ? Date.parse(row._timestamp) : null;
            row._content = row._content ? removeMarkup(row._content) : null;

						map[row._tid] = row;
					});

					callback(null, map);
				});
	};

	Exporter.getPosts = function(callback) {
		return Exporter.getPaginatedPosts(0, -1, callback);
	};
	Exporter.getPaginatedPosts = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;

		var err;
		var prefix = Exporter.config('prefix');
		var query =
				'SELECT '
				+ 'id as _pid, '
				+ 'discussion_id as _tid, '
				+ 'content as _content, '
				+ 'time as _timestamp, '
        + 'user_id as _uid, '
        + 'ip_address as _ip, '
        + 'edit_time as _edited '

				+ 'FROM ' + prefix + 'posts '
					// this post cannot be a its topic's main post, it MUST be a reply-post
					// see https://github.com/akhoury/nodebb-plugin-import#important-note-on-topics-and-posts
				+ 'WHERE number > 1 AND type="comment" '
				+ (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');


		if (!Exporter.connection) {
			err = {error: 'MySQL connection is not setup. Run setup(config) first'};
			Exporter.error(err.error);
			return callback(err);
		}

		Exporter.connection.query(query,
				function(err, rows) {
					if (err) {
						Exporter.error(err);
						return callback(err);
					}

					//normalize here
					var map = {};
					rows.forEach(function(row) {
            row._timestamp = row._timestamp ? Date.parse(row._timestamp) : null;
						row._edited = row._edited ? Date.parse(row._edited) : null;
            row._content = row._content ? removeMarkup(row._content) : null;

						map[row._pid] = row;
					});

					callback(null, map);
				});
	};

	Exporter.getVotes = function(callback) {
		return Exporter.getPaginatedVotes(0, -1, callback);
	};
	Exporter.getPaginatedVotes = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;

		var err;
		var prefix = Exporter.config('prefix');
		var query =
				'SELECT '
        + 'post_id as _pid, '
        + 'user_id as _uid '

				+ 'FROM ' + prefix + 'posts_likes '
				+ (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');


		if (!Exporter.connection) {
			err = {error: 'MySQL connection is not setup. Run setup(config) first'};
			Exporter.error(err.error);
			return callback(err);
		}

		Exporter.connection.query(query,
				function(err, rows) {
					if (err) {
						Exporter.error(err);
						return callback(err);
					}

					//normalize here
					var map = {};
					rows.forEach(function(row, i) {
            row._vid = i;
            row._action = 1;

						map[row._vid] = row;
					});

					callback(null, map);
				});
	};

	Exporter.teardown = function(callback) {
		Exporter.log('teardown');
		Exporter.connection.end();

		Exporter.log('Done');
		callback();
	};

	Exporter.testrun = function(config, callback) {
		async.series([
			function(next) {
				Exporter.setup(config, next);
			},
			function(next) {
				Exporter.getUsers(next);
			},
			function(next) {
				Exporter.getGroups(next);
			},
			function(next) {
				Exporter.getCategories(next);
			},
			function(next) {
				Exporter.getTopics(next);
			},
			function(next) {
				Exporter.getPosts(next);
			},
      function(next) {
        Exporter.getVotes(next);
      },
			function(next) {
				Exporter.teardown(next);
			}
		], callback);
	};

	Exporter.warn = function() {
		var args = _.toArray(arguments);
		args.unshift(logPrefix);
		console.warn.apply(console, args);
	};

	Exporter.log = function() {
		var args = _.toArray(arguments);
		args.unshift(logPrefix);
		console.log.apply(console, args);
	};

	Exporter.error = function() {
		var args = _.toArray(arguments);
		args.unshift(logPrefix);
		console.error.apply(console, args);
	};

	Exporter.config = function(config, val) {
		if (config != null) {
			if (typeof config === 'object') {
				Exporter._config = config;
			} else if (typeof config === 'string') {
				if (val != null) {
					Exporter._config = Exporter._config || {};
					Exporter._config[config] = val;
				}
				return Exporter._config[config];
			}
		}
		return Exporter._config;
	};

	// from Angular https://github.com/angular/angular.js/blob/master/src/ng/directive/input.js#L11
	Exporter.validateUrl = function(url) {
		var pattern = /^(ftp|http|https):\/\/(\w+:{0,1}\w*@)?(\S+)(:[0-9]+)?(\/|\/([\w#!:.?+=&%@!\-\/]))?$/;
		return url && url.length < 2083 && url.match(pattern) ? url : '';
	};

	Exporter.truncateStr = function(str, len) {
		if (typeof str != 'string') return str;
		len = _.isNumber(len) && len > 3 ? len : 20;
		return str.length <= len ? str : str.substr(0, len - 3) + '...';
	};

	Exporter.whichIsFalsy = function(arr) {
		for (var i = 0; i < arr.length; i++) {
			if (!arr[i])
				return i;
		}
		return null;
	};

})(module.exports);
