const { MongoClient, ObjectId } = require('mongodb');
var pluralize = require('mongoose-legacy-pluralize');

function parseError(err) {
    console.log('err', err);
    return err;
};

Mapper = function(OBJY, options) {
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
            authorisations: {}
        },

        staticDatabase: null,


        setStaticDatabase(name) {
            this.staticDatabase = name;
            return this;
        },

        connect: function(connectionString, success, error, options) {

            this.database = new MongoClient(connectionString);

            this.database.connect().then(_success => {
                success()
            }).catch(e => {
                error(e)
            });

            return this;
        },

        getConnection: function() {
            return this.database;
        },

        useConnection: function(connection, success, error) {
            this.database = connection;

            return this;
        },

        getDBByMultitenancy: function(client) {

            if (this.staticDatabase) return this.database.useDb(this.staticDatabase)

            if (this.multitenancy == this.CONSTANTS.MULTITENANCY.SHARED) {
                return this.database.db('spoo')
            } else if (this.multitenancy == this.CONSTANTS.MULTITENANCY.ISOLATED) {
                return this.database.db(client);
            }
        },

        createClient: function(client, success, error) {

            var db = this.getDBByMultitenancy(client);

            const ClientInfo = db.collection('clientinfos');

            ClientInfo.find({ name: client }).toArray().then(function(data) {
               
                if (data.length >= 1) {
                    error("client name already taken")
                } else {

                    ClientInfo.insertMany([{ name: client }]).then(function(data) {
                        success({ name: client });
                    }).catch(err => {
                        error(parseError(err));
                    })
                }

            }).catch(err => {
                error(err);
            });
        },


        listClients: function(success, error) {

            if (this.multitenancy == this.CONSTANTS.MULTITENANCY.ISOLATED) {
                const admin = this.getDBByMultitenancy('admin');

                admin.command({ listDatabases: 1, nameOnly: true }).then(function(result) {
                    success(result.databases.map(function(item) {
                        return item.name
                    }));
                }).catch(err => {
                    error(err)
                })

            } else {
                var db = this.getDBByMultitenancy('spoo');

                var ClientInfo = db.collection('clientinfos');

                ClientInfo.find({}).then(function(data) {

                    success(data.map(function(item) {
                        return item.name
                    }))

                }).catch(err => {
                    error(err);
                });

            }
        },

        getById: function(id, success, error, app, client) {

            var db = this.getDBByMultitenancy(client);

            const Obj = db.collection(pluralize(this.objectFamily));

            if (typeof id === "string") id = new ObjectId(id);

            var constrains = { _id: id };
            

            if (app) constrains['applications'] = { $in: [app] }

            if (this.multitenancy == this.CONSTANTS.MULTITENANCY.SHARED && client) constrains['tenantId'] = client;

            Obj.findOne(constrains).then(function(data) {
                success(data);
            }).catch(err => {
                error(err);
            });
        },

        getByCriteria: function(criteria, success, error, app, client, flags) {

            var db = this.getDBByMultitenancy(client);

            const Obj = db.collection(pluralize(this.objectFamily));

            if (flags.$page == 1) flags.$page = 0;
            else flags.$page -= 1;

            if (flags.$pageSize)
                if (flags.$pageSize > 1000) flags.$pageSize = 1000;

            if (this.multitenancy == this.CONSTANTS.MULTITENANCY.SHARED && client) criteria['tenantId'] = client;

            if (criteria.$query) {
                criteria = JSON.parse(JSON.stringify(criteria.$query));
                delete criteria.$query;
            }

            var arr = [{ $match: criteria }, { $limit: 20 }];
            if (flags.$page) arr.push({ $skip: (flags.$pageSize || this.globalPaging) * (flags.$page || 0) })

            var s = {};

            if (flags.$sort) {

                if (flags.$sort.charAt(0) == '-') {
                    s[flags.$sort.slice(1)] = -1;
                } else {
                    s[flags.$sort] = 1;
                }

                s['_id'] = -1;

                arr.push({ $sort: s })
            }

            if (Object.keys(s).length == 0) s = { '_id': -1 };

            if (app) criteria['applications'] = { $in: [app] }


            var finalQuery = Obj.find(criteria);

            if (flags.$limit) finalQuery.limit(flags.$limit).sort(s);
            else finalQuery.limit((flags.$pageSize || this.globalPaging)).skip((flags.$pageSize || this.globalPaging) * (flags.$page || 0)).sort(s);

            if (criteria.$sum || criteria.$count || criteria.$avg) {
                var aggregation = JSON.parse(JSON.stringify(criteria.$sum || criteria.$count || criteria.$avg));;
                var pipeline = [];
                var match = criteria.$match;

                if (typeof match === 'string') match = JSON.parse(match);

                if (match) pipeline.push({ $match: match });

                if (criteria.$sum) pipeline.push({ $group: { _id: null, "sum": { $sum: { $toDouble: aggregation } } } })
                else if (criteria.$count) pipeline.push({ $group: { _id: { "field": aggregation }, count: { $sum: 1 } } })
                else if (criteria.$avg) pipeline.push({ $group: { _id: null, avg: { $avg: { $toDouble: aggregation } } } });

                Obj.aggregate(pipeline).toArray().then(function(data) {
                   
                    if(data.length){
                        data.forEach(d => {
                            if(match.inherits) d.inherits = match.inherits.$in;
                        })
                    }
                    
                    success(data);
                    return;
                }).catch(err => {
                    error(err);
                });

            } else {
                finalQuery.toArray().then(function(data) {
                    success(data);
                }).catch(err => {
                    error(err);
                });
            }

        },

        count: function(criteria, success, error, app, client, flags) {

            var db = this.getDBByMultitenancy(client);

            const Obj = db.collection(pluralize(this.objectFamily));

            if (criteria.$query) {
                criteria = JSON.parse(JSON.stringify(criteria.$query));
                delete criteria.$query;
            }

            if (app) criteria['applications'] = { $in: [app] }

            if (this.multitenancy == this.CONSTANTS.MULTITENANCY.SHARED && client) criteria['tenantId'] = client;

            Obj.countDocuments(criteria, { hint: "_id_" }).then(function(data) {
                success({ 'result': data });
            }).catch(err => {
                error(err);
            });
        },

        update: function(spooElement, success, error, app, client) {

            var db = this.getDBByMultitenancy(client);

            const Obj = db.collection(pluralize(this.objectFamily));

            var criteria = { _id: spooElement._id };

            if (app) criteria.applications = { $in: [app] };

            if (this.multitenancy == this.CONSTANTS.MULTITENANCY.SHARED && client) criteria['tenantId'] = client;

            Obj.updateOne(criteria, JSON.parse(JSON.stringify(spooElement))).then(function(data) {
                if (data.n != 0) success(spooElement);
                else error("object not found");
            }).catch(err => {
                error(err);
            })
        },

        add: function(spooElement, success, error, app, client) {

            var db = this.getDBByMultitenancy(client);

            const Obj = db.collection(pluralize(this.objectFamily));

            if (app) {
                if (spooElement.applications.indexOf(app) == -1) spooElement.applications.push(app);
            }

            //delete spooElement._id;
            if (!ObjectId.isValid(spooElement._id)) delete spooElement._id;
            else spooElement._id = new ObjectId(spooElement._id);


            if (this.multitenancy == this.CONSTANTS.MULTITENANCY.SHARED) spooElement.tenantId = client;

            if(!Array.isArray(spooElement)) spooElement = [spooElement];

            Obj.insertMany(spooElement).then(function(data) {
                success(data);
            }).catch(err => {
                error(parseError(err));
            })

        },

        remove: function(spooElement, success, error, app, client) {

            var db = this.getDBByMultitenancy(client);

            const Obj = db.collection(pluralize(this.objectFamily));

            var criteria = { _id: spooElement._id };

            if (app) criteria['applications'] = { $in: [app] }

            if (this.multitenancy == this.CONSTANTS.MULTITENANCY.SHARED && client) criteria['tenantId'] = client;

            Obj.deleteMany(criteria).then(function(data) {
                if (data.n == 0) error("object not found");
                else {
                    success(true);
                }
            }).catch(err => {
                error(err);
            })
        }
    })
}



module.exports = Mapper;