var watchlog = require('./watchlog.js');

var AZURE_STORAGE_ACCOUNT = 'your azure storage account name here',
    AZURE_STORAGE_ACCESS_KEY = 'your azure storage account password here',
    RCON_PORT = '25566',
    RCON_PASSWORD = 'pass@w0rd',
    MINECRAFT_WORLD_ROOT = 'C:/minecraft';

watchlog.initAzure(AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_ACCESS_KEY);
watchlog.initRcon(RCON_PORT, RCON_PASSWORD);

watchlog.watch(MINECRAFT_WORLD_ROOT);
