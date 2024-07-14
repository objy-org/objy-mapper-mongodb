var mongoose = require('mongoose');
var Schema = mongoose.Schema;
var Admin = mongoose.mongo.Admin;

mongoose.set('autoCreate', false);

var clientSchema = {
    name: String,
};
var ClientSchema = new Schema(clientSchema);

function parseError(err) {
    console.log('err', err);
    return err;
}

Mapper = function (OBJY, options) {
    return Object.assign(new OBJY.StorageTemplate(OBJY, options), {
        database: {},
        index: {},
        globalPaging: 20,

        generalObjectModel: {
            type: { type: String, index: true },
            applications: { type: [String], index: true },
            created: { type: String, index: true },
            lastModified: { type: String, index: true },
            role: String,
            spooAdmin: Boolean,
            inherits: [],
            name: String,
            onDelete: {},
            onCreate: {},
            onChange: {},
            permissions: {},
            properties: {},
            privileges: {},
            aggregatedEvents: [],
            tenantId: String,
            password: String,
            username: String,
            email: String,
            _clients: [],
            authorisations: {},
        },

        staticDatabase: null,

        ObjSchema: new Schema(this.generalObjectModel, { minimize: false, strict: false }),

        NestedSchema: new Schema({}, { minimize: false }),

        structure: function (structure) {
            this.generalObjectModel = Object.assign(this.generalObjectModel, structure);
            this.generalObjectModel.properties = Object.assign(this.generalObjectModel.properties, structure.structure);
            this.ObjSchema = new Schema(this.generalObjectModel, { minimize: false });
            return this;
        },

        setStaticDatabase(name) {
            this.staticDatabase = name;
            return this;
        },

        connect: function (connectionString, success, error, options) {
            this.database = mongoose.createConnection(connectionString, options);

            this.database.on('error', function (err) {
                error(err);
            });

            this.database.once('open', function () {
                success();
            });

            return this;
        },

        getConnection: function () {
            return this.database;
        },

        useConnection: function (connection, success, error) {
            this.database = connection;

            this.database.on('error', function (err) {
                error(err);
            });

            this.database.once('open', function () {
                success();
            });

            return this;
        },

        getDBByMultitenancy: function (client) {
            if (this.staticDatabase) return this.database.useDb(this.staticDatabase, { useCache: true });

            if (this.multitenancy == this.CONSTANTS.MULTITENANCY.SHARED) {
                return this.database.useDb('spoo', { useCache: true });
            } else if (this.multitenancy == this.CONSTANTS.MULTITENANCY.ISOLATED) {
                return this.database.useDb(client, { useCache: true });
            }
        },

        createClient: function (client, success, error) {
            var db = this.getDBByMultitenancy(client);

            var ClientInfo = db.model('clientinfos', ClientSchema);

            ClientInfo.find({ name: client }).exec(function (err, data) {
                if (err) {
                    error(err);
                    return;
                }
                if (data.length >= 1) {
                    error('client name already taken');
                } else {
                    new ClientInfo({ name: client }).save(function (err, data) {
                        if (err) {
                            error(err);
                            return;
                        }

                        success(data);
                    });
                }
            });
        },

        listClients: function (success, error) {
            if (this.multitenancy == this.CONSTANTS.MULTITENANCY.ISOLATED) {
                new Admin(this.database.db).listDatabases(function (err, result) {
                    if (err) error(err);
                    success(
                        result.databases.map(function (item) {
                            return item.name;
                        })
                    );
                });
            } else {
                var db = this.getDBByMultitenancy('spoo');

                var ClientInfo = db.model('clientinfos', ClientSchema);

                ClientInfo.find({}).exec(function (err, data) {
                    if (err) {
                        error(err);
                        return;
                    }

                    success(
                        data.map(function (item) {
                            return item.name;
                        })
                    );
                });
            }
        },

        getById: function (id, success, error, app, client) {
            var db = this.getDBByMultitenancy(client);

            var constrains = { _id: id };

            if (app) constrains['applications'] = { $in: [app] };

            if (this.multitenancy == this.CONSTANTS.MULTITENANCY.SHARED && client) constrains['tenantId'] = client;

            Obj = db.model(this.objectFamily, this.ObjSchema);

            Obj.findOne(constrains)
                .lean()
                .exec(function (err, data) {
                    if (err) {
                        error(err);
                        return;
                    }

                    if (data?._id) {
                        data._id = String(data._id);
                    }

                    success(data);
                    return;
                });
        },

        getByCriteria: function (criteria, success, error, app, client, flags) {
            var db = this.getDBByMultitenancy(client);

            var Obj = db.model(this.objectFamily, this.ObjSchema);

            if (flags.$page == 1) flags.$page = 0;
            else flags.$page -= 1;

            if (flags.$pageSize) if (flags.$pageSize > 1000) flags.$pageSize = 1000;

            if (this.multitenancy == this.CONSTANTS.MULTITENANCY.SHARED && client) criteria['tenantId'] = client;

            if (criteria.$query) {
                criteria = JSON.parse(JSON.stringify(criteria.$query));
                delete criteria.$query;
            }

            var arr = [{ $match: criteria }, { $limit: 20 }];
            if (flags.$page) arr.push({ $skip: (flags.$pageSize || this.globalPaging) * (flags.$page || 0) });

            var s = {};

            if (flags.$sort) {
                if (flags.$sort.charAt(0) == '-') {
                    s[flags.$sort.slice(1)] = -1;
                } else {
                    s[flags.$sort] = 1;
                }

                s['_id'] = -1;

                arr.push({ $sort: s });
            }

            if (Object.keys(s).length == 0) s = { _id: -1 };

            if (app) criteria['applications'] = { $in: [app] };

            var finalQuery = Obj.find(criteria);

            if (flags.$limit) finalQuery.limit(flags.$limit).sort(s);
            else
                finalQuery
                    .limit(parseInt(flags.$pageSize || this.globalPaging))
                    .skip(parseInt((flags.$pageSize || this.globalPaging) * (flags.$page || 0)))
                    .sort(s);

            if (criteria.$sum || criteria.$count || criteria.$avg) {
                var aggregation = JSON.parse(JSON.stringify(criteria.$sum || criteria.$count || criteria.$avg));
                var pipeline = [];
                var match = criteria.$match;

                if (typeof match === 'string') match = JSON.parse(match);

                if (match) pipeline.push({ $match: match });

                if (criteria.$sum) pipeline.push({ $group: { _id: null, sum: { $sum: { $toDouble: aggregation } } } });
                else if (criteria.$count) pipeline.push({ $group: { _id: { field: aggregation }, count: { $sum: 1 } } });
                else if (criteria.$avg) pipeline.push({ $group: { _id: null, avg: { $avg: { $toDouble: aggregation } } } });

                Obj.aggregate(pipeline, function (err, data) {
                    if (err) {
                        console.warn('mongo err', err);
                        error(err);
                        return;
                    }

                    if (data.length) {
                        data.forEach((d) => {
                            //d._id = String(d._id);

                            if (match.inherits) d.inherits = match.inherits.$in;
                        });
                    }

                    success(data);
                    return;
                });
            } else {
                finalQuery.lean().exec(function (err, data) {
                    if (err) {
                        console.warn('mongo err', err);
                        error(err);
                        return;
                    }

                    data.forEach((d) => {
                        d._id = String(d._id);
                    });

                    success(data);
                    return;
                });
            }
        },

        count: function (criteria, success, error, app, client, flags) {
            var db = this.getDBByMultitenancy(client);

            var Obj = db.model(this.objectFamily, this.ObjSchema);

            if (criteria.$query) {
                criteria = JSON.parse(JSON.stringify(criteria.$query));
                delete criteria.$query;
            }

            if (app) criteria['applications'] = { $in: [app] };

            if (this.multitenancy == this.CONSTANTS.MULTITENANCY.SHARED && client) criteria['tenantId'] = client;

            Obj.count(criteria).exec(function (err, data) {
                if (err) {
                    error(err);
                    return;
                }

                success({ result: data });
                return;
            });
        },

        update: function (spooElement, success, error, app, client) {
            var db = this.getDBByMultitenancy(client);

            var Obj = db.model(this.objectFamily, this.ObjSchema);

            var criteria = { _id: spooElement._id };

            if (app) criteria.applications = { $in: [app] };

            if (this.multitenancy == this.CONSTANTS.MULTITENANCY.SHARED && client) criteria['tenantId'] = client;

            Obj.findOneAndUpdate(criteria, JSON.parse(JSON.stringify(spooElement)), function (err, data) {
                if (err) {
                    error(err);
                    return;
                }

                spooElement._id = String(spooElement._id);

                if (data.n != 0) success(spooElement);
                else error('object not found');
            });
        },

        add: function (spooElement, success, error, app, client) {
            var db = this.getDBByMultitenancy(client);

            if (app) {
                if (spooElement.applications.indexOf(app) == -1) spooElement.applications.push(app);
            }

            var Obj = db.model(this.objectFamily, this.ObjSchema);

            //delete spooElement._id;
            if (!mongoose.Types.ObjectId.isValid(spooElement._id)) delete spooElement._id;
            else spooElement._id = new mongoose.mongo.ObjectId(spooElement._id);

            if (this.multitenancy == this.CONSTANTS.MULTITENANCY.SHARED) spooElement.tenantId = client;

            new Obj(spooElement).save(function (err, data) {
                if (err) {
                    error(parseError(err));
                    return;
                }

                data._id = String(data._id);

                success(data);
            });
        },

        remove: function (spooElement, success, error, app, client) {
            var db = this.getDBByMultitenancy(client);

            var Obj = db.model(this.objectFamily, this.ObjSchema);

            var criteria = { _id: spooElement._id };

            if (app) criteria['applications'] = { $in: [app] };

            if (this.multitenancy == this.CONSTANTS.MULTITENANCY.SHARED && client) criteria['tenantId'] = client;

            Obj.deleteOne(criteria, function (err, data) {
                if (err) {
                    error(err);
                    return;
                }
                if (data.n == 0) error('object not found');
                else {
                    success(true);
                }
            });
        },
    });
};

module.exports = Mapper;
