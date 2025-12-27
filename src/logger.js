const pino = require('pino');
const { logLevel } = require('./config');

module.exports = pino({ level: logLevel });
