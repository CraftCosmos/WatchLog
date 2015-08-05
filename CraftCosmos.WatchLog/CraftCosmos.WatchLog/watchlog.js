module.exports = function () {
    'use strict';
    var sys = require('sys'),
        Tail = require('tail').Tail,
        azure = require('azure-storage'),
        Rcon = require('rcon'),
        http = require('http'),

        initAzure = function (accountName, accountKey) {
            var StorageAccountName = accountName,
                StorageAccountKey = accountKey,
                retryOperations = new azure.ExponentialRetryPolicyFilter(),
            //TableQuery = azure.TableQuery,
                TableUtilities = azure.TableUtilities;
            entGen = TableUtilities.entityGenerator;
            
            tableSvc = azure.createTableService(StorageAccountName, StorageAccountKey).withFilter(retryOperations);
            TABLES.forEach(function (table) {
                openTable(table);
            });
        },
        initRcon = function (port, password) {
            rconHost = 'localhost';
            rconPort = port;
            rconPass = password;
        },
        watch = function (path) {
            var tail = new Tail(path + '/logs/latest.log');

            if (tail) {
                tail.on("line", function (line) {
                    seqNumber = seqNumber + 1;
                    parse(line);
                })
            }
            else {
                return sys.puts('Log file: ' + path + '/logs/latest.log could not be found.');
            }
        },
        archive = function (path) {

        },
        parse = function (logline, logdate) {
            var logLine = logline.toString(),
                logDate = (logdate ? logdate : new Date()),
                logEntry = {},
                logMsg = '';
            if (logLine) {
                logEntry = extractLogEntry(logLine, logDate);
                logMsg = logEntry.Message._;
                if (logMsg.substring(0, 4) == '[@: ') {
                        consoleLog('command block used.');
                }
                // player chat
                else if (logMsg.substring(0, 1) == '<') {
                    extractChat(logEntry);
                }
                // Rcon or command block chat
                else if ((logMsg.substring(0, 6) == '[Rcon]') || (logMsg.substring(0, 3) == '[@]')) {
                    extractSysChat(logEntry);
                }
                // admin command
                else if (logMsg.substring(0, 1) == '[') {
                    extractCmd(logEntry);
                }
                // UUID of player
                else if (logMsg.substring(0, 4) == 'UUID') {
                    extractUuid(logMsg, function (logmsg, utable, name) {
                        checkFishBans(name, function (result) {
                            if (result.success) {
                                if (result.stats.totalbans) {
                                    enforceFishBans(logmsg, utable, name, result);
                                }
                            }
                        });
                    });
                }
                // IP and spawn location
                else if (logMsg.indexOf(' logged in with entity id ') > 0) {
                    extractSpawn(logEntry);
                }
                // join and left events
                else if ((logMsg.substring(logMsg.length - 15, logMsg.length) == 'joined the game') 
                || (logMsg.substring(logMsg.length - 13, logMsg.length) == 'left the game')) {
                    extractJoinLeft(logEntry);
                }
                // achievements
                else if (logMsg.indexOf(' has just earned the achievement ') > 0) {
                    extractAchievement(logEntry);
                }
                // me
                else if (logMsg.substring(0, 1) == '*') {
                    extractMe(logEntry);
                }
                // lag
                else if (logMsg.substring(0, 14) == "Can't keep up!") {
                    extractLag(logEntry);
                }
                // other
                else {
                    extractOther(logEntry);
                };
            }
        },

        extractAchievement = function (logrow) {
            var logmsg = logrow.Message._,
                who = logmsg.substring(0, logmsg.indexOf(' ')),
                did = logmsg.substring(logmsg.indexOf('[') + 1, logmsg.indexOf(']')),
                chatrow = {
                    PartitionKey: logrow.PartitionKey,
                    Time: logrow.Time,
                    Who: entGen.String(who),
                    Did: entGen.String(did)
                };
            
            saveRow(TABLE_CHAT, chatrow);
        },
        extractChat = function (logrow) {
            var logmsg = logrow.Message._,
                who = logmsg.substring(1, logmsg.indexOf('>')),
                said = logmsg.substring(logmsg.indexOf('>') + 2, logmsg.length),
                chatrow = {
                    PartitionKey: logrow.PartitionKey,
                    Time: logrow.Time,
                    Who: entGen.String(who),
                    Said: entGen.String(said)
                };
            
            saveRow(TABLE_CHAT, chatrow);
        },
        extractSysChat = function (logrow) {
            var logmsg = logrow.Message._,
                who = logmsg.substring(0, logmsg.indexOf(']') + 1),
                said = logmsg.substring(logmsg.indexOf(']') + 2, logmsg.length),
                chatrow = {
                    PartitionKey: logrow.PartitionKey,
                    Time: logrow.Time,
                    Who: entGen.String(who),
                    Said: entGen.String(said)
                };
            
            saveRow(TABLE_CHAT, chatrow);
        },
        extractCmd = function (logrow) {
            var logmsg = logrow.Message._,
                who = logmsg.substring(1, logmsg.indexOf(":")),
                what = logmsg.substring(logmsg.indexOf(":") + 2, logmsg.length - 1),
                chatrow = {
                    PartitionKey: logrow.PartitionKey,
                    Time: logrow.Time,
                    Who: entGen.String(who),
                    What: entGen.String(what)
                };
            
            saveRow(TABLE_ADMIN, chatrow);
        },
        extractJoinLeft = function (logrow) {
            var logmsg = logrow.Message._,
                who = logmsg.substring(0, logmsg.indexOf(' ')),
                action = '',
                chatrow = {
                    PartitionKey: logrow.PartitionKey,
                    Time: logrow.Time,
                    Who: entGen.String(who),
                    Action: entGen.String('LEFT')
                };
            
            if (logmsg.substring(logmsg.length - 15, logmsg.length - 9) == 'joined') {
                chatrow.Action = entGen.String('JOIN');
            }
            else {
                lookupUserTable(who, function (userTable) { 
                    var userRec = chatrow;
                    delete userRec.Who;
                    saveRow(userTable, userRec);
                });
            }
            
            saveRow(TABLE_CHAT, chatrow);
        },
        extractLag = function (logrow) {
            var logmsg = logrow.Message._,
                lag = logmsg.substring(logmsg.indexOf('Running ') + 8, logmsg.indexOf('ms behind')),
                skip = logmsg.substring(logmsg.indexOf('skipping ') + 9, logmsg.indexOf('tick(s)') - 1),
                chatrow = {
                    PartitionKey: logrow.PartitionKey,
                    Time: logrow.Time,
                    Lag: entGen.String(lag),
                    Skip: entGen.String(skip)
                };
            
            saveRow(TABLE_LAG, chatrow);
        },
        extractLogEntry = function (logline, logdate) {
            var logline = logline.replace(/[\n\r]/g, ''),
                logtime = logline.substring(0, logline.indexOf(" ")).replace(/\[|\]/gi, ''),
                loglesstime = logline.substring(logline.indexOf(" ") + 1, logline.length),
                rawsource = loglesstime.substring(1, loglesstime.indexOf(":") - 1).split("/"),
                source = rawsource[0],
                level = rawsource[1],
                logmsg = loglesstime.substring(loglesstime.indexOf(": ") + 2, loglesstime.length),
                datestring = logdate.getUTCFullYear().toString() + ("0" + (logdate.getUTCMonth() + 1).toString()).slice(-2) + ("0" + logdate.getUTCDate().toString()).slice(-2);
            var logrow = {
                PartitionKey: entGen.String(datestring)
                , Time: entGen.String(logtime)
                , Source: entGen.String(source)
                , Level: entGen.String(level)
                , Message: entGen.String(logmsg)
            };
            return logrow;
        },
        extractMe = function (logrow) {
            var logmsg = logrow.Message._,
                meMsg = logmsg.substring(2, logmsg.length),
                who = meMsg.substring(0, meMsg.indexOf(' ')),
                did = meMsg.substring(who.length + 1, meMsg.length),
                chatrow = {
                    PartitionKey: logrow.PartitionKey,
                    Time: logrow.Time,
                    Who: entGen.String(who),
                    Me: entGen.String(did)
                };
            
            saveRow(TABLE_CHAT, chatrow);
        },
        extractOther = function (logrow) {
            var logmsg = logrow.Message._,
                chatrow = {
                    PartitionKey: logrow.PartitionKey,
                    Time: logrow.Time,
                    Other: entGen.String(logmsg)
                };
            
            saveRow(TABLE_SYSTEM, chatrow);
        },
        extractSpawn = function (logrow) {
            var logmsg = logrow.Message._,
                name = logmsg.substring(0, logmsg.indexOf('['));
            
            lookupUserTable(name, function (table) {
                var ip = logmsg.substring(logmsg.indexOf('/') + 1, logmsg.indexOf(':')),
                    coords = logmsg.substring(logmsg.indexOf('(') + 1, logmsg.indexOf(')')).split(', '),
                    x = coords[0],
                    y = coords[1],
                    z = coords[2],
                    playerip = {
                        PartitionKey: logrow.PartitionKey,
                        Action: entGen.String('JOIN'),
                        Time: logrow.Time,
                        IP: entGen.String(ip)
                    },
                    playercoord = {
                        PartitionKey: logrow.PartitionKey,
                        Action: entGen.String('JOIN'),
                        Time: logrow.Time,
                        X: entGen.String(x),
                        Y: entGen.String(y),
                        Z: entGen.String(z)
                    };
                
                //saveRow(table, playerip, function () {
                    saveRow(table, playercoord, function () { 
                        // first time user
                        if ((x.split('.')[1].length == 1) && (z.split('.')[1].length == 1)) {
                            var first = {
                                PartitionKey: entGen.String('info')
                                , RowKey: entGen.String('joined')
                                , JoinDate: logrow.PartitionKey
                                , JoinTime: logrow.Time
                            };
                            consoleLog("First time user detected!");
                            saveRow(table, first);
                        };                    
                    });
               // });
            });
        },
        extractUuid = function (logmsg, callback) {
            var uuid = '',
                name = '',
                userTable = '';
            if (typeof callback !== "function") {
                callback = false;
            }
            
            uuid = logmsg.substring(logmsg.length - 36, logmsg.length);
            name = logmsg.substring(15, logmsg.indexOf(' is '));
            userTable = 'z' + uuid.replace(/-/g, "x");
            saveRow(TABLE_LOOKUP, {
                PartitionKey: entGen.String('NAME'),
                RowKey: entGen.String(name),
                id: entGen.String(uuid),
                table: entGen.String(userTable)
            }, function () { 
                openTable(userTable, function () {
                    saveRow(userTable, {
                        PartitionKey: entGen.String('info'),
                        RowKey: entGen.String('name'),
                        name: entGen.String(name)
                    }, 
                        function () { 
                        saveRow(userTable, {
                            PartitionKey: entGen.String('info'),
                            RowKey: entGen.String('lastlogin'),
                            lastlogin: entGen.DateTime(new Date())
                        }, 
                            function () { 
                                if (callback) {
                                    callback(logmsg, userTable, name);
                                }
                        });                
                    });
                });
            
            });
        },

        openTable = function (table, callback) {
            if (typeof callback !== "function") {
                callback = false;
            }
            
            tableSvc.createTableIfNotExists(table, function (error) {
                if (error) {
                    consoleLog("Could not create or open '" + table + "' table.");
                    return;
                }
                consoleLog('Azure table "' + table + '" opened and/or created successfully.');
                if (callback) {
                    callback();
                }
            });
        },
        lookupUserTable = function (name, callback) {
            if (typeof callback !== "function") {
                callback = false;
            }
            tableSvc.retrieveEntity(TABLE_LOOKUP, 'NAME', name, function (error, result, response) {
                var table = '';
                if (error) {
                    consoleLog('players UUID query error ' + error);
                }
                else {
                    table = result.table._;
                    if (callback) {
                        callback(table);
                    }
                } 
            });
        },
        saveRow = function (table, row, callback) {
            if (typeof callback !== "function") {
                callback = false;
            }
            if (!row.RowKey) {
                row.RowKey = entGen.String(seqNumber.toString());
            }
            tableSvc.insertOrReplaceEntity(table, row, function (error, result, response) {
                consoleLog("[" + response.statusCode + "] " + table + " " + row.RowKey._);
                if (!response.isSuccessful) {
                    consoleLog("  Cannot insert '" + table + "' row using RowKey '" + row.RowKey._ + "'. Retrying...");
                }
                else {
                    consoleLog("'" + table + "' '" + row.RowKey._ + "' added.");
                }
                if (callback) {
                    callback(table);
                }
            });
        },
        StorageAccountName = null,
        StorageAccountKey = null,
        tableSvc = {},
        entGen = {},
        getSeqNumber = function () {
            return ((new Date().valueOf()) * 100);
        },
        seqNumber = getSeqNumber(),

        checkFishBans = function (name, callback) {
            if (typeof callback !== "function") {
                callback = false;
            }
            var options = {
                    hostname: 'api.fishbans.com',
                    path: '/stats/' + name,        
                },
                result = false;

            http.request(options, function (res) {
                consoleLog("Got response: " + res.statusCode);
                res.on('data', function (chunk) {
                    result = JSON.parse(chunk);
                    if (callback) {
                        callback(result);
                    }
                }).on('error', function (e) {
                    consoleLog("Got error: " + e.message);
                });
            }).end();
        },
        enforceFishBans = function (logmsg, utable, name, result) {
            var conn = new Rcon(rconHost, rconPort, rconPass);
            conn.on('auth', function () {
                consoleLog("Authed!");
                conn.send('ban ' + name);
                conn.send('say ' + name + ' has a negative FishBans.com report and has been automatically banned!');

                saveRow(utable, {
                    PartitionKey: entGen.String('WatchLog'),
                    RowKey: logmsg.PartitionKey,
                    Action: entGen.String('BANNED'),
                    FishBansResult: entGen.String(JSON.stringify(result))
                });
            }).on('response', function (str) {
                consoleLog("Got response: " + str);
            }).on('end', function () {
                consoleLog("Socket closed!");
            });
            conn.connect();
        },

        consoleLog = function (msg) {
            if (process.env.WATCHLOG_VERBOSE) {
                console.log(msg);
            }
        },
        rconHost,
        rconPort,
        rconPass,

        TABLE_ADMIN = 'admin',
        TABLE_CHAT = 'chat',
        TABLE_LAG = 'lag',
        TABLE_LOOKUP = 'lookup',
        TABLE_PLAYERS = 'players',
        TABLE_SYSTEM = 'system',
        TABLES = [TABLE_SYSTEM, TABLE_CHAT, TABLE_LAG, TABLE_ADMIN, TABLE_LOOKUP];
    
    return {
        initAzure: initAzure,
        initRcon: initRcon,
        watch: watch,
        archive: archive,
        parse: parse
    }

}();