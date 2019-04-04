'use strict';

const co           = require('co');
const EventEmitter = require('events');
const AWS          = require('aws-sdk');
const ssm          = new AWS.SSM();

const DEFAULT_CACHE_LIFETIME_IN_MILLISECONDS = 3 * 60 * 1000; // default expiry is 3 mins

let cache = {
    expiration : new Date(0),
    items      : {}
};

function load (keys, cacheLifetimeInMilliseconds) {
    cacheLifetimeInMilliseconds = cacheLifetimeInMilliseconds || DEFAULT_CACHE_LIFETIME_IN_MILLISECONDS;

    if (!keys || !Array.isArray(keys) || keys.length === 0) {
        throw new Error('you need to provide a non-empty array of config keys');
    }

    if (cacheLifetimeInMilliseconds <= 0) {
        throw new Error('you need to specify an expiry (ms) greater than 0, or leave it undefined');
    }

    let eventEmitter = new EventEmitter();

    let validate = (keys, params) => {
        let missing = keys.filter(k => params[k] === undefined);
        if (missing.length > 0) {
            throw new Error(`missing keys: ${missing}`);
        }
    };

    let reload = co.wrap(function* () {
        console.log(`loading cache keys: ${keys}`);

        const j = keys.length;
        const params = [];
        const maxChunkSize = 10;
        for (let i = 0; i < j; i += maxChunkSize) {
            const chunk = keys.slice(i, i + maxChunkSize);

            let req = {
                Names: chunk,
                WithDecryption: true
            };
            let resp = yield ssm.getParameters(req).promise();

            for (let p of resp.Parameters) {
                params[p.Name] = p.Value;
            }
        }

        validate(keys, params);

        console.log(`successfully loaded cache keys: ${keys}`);
        let now = new Date();

        cache.expiration = new Date(now.getTime() + cacheLifetimeInMilliseconds);
        cache.items = params;

        eventEmitter.emit('refresh');
    });

    let getValue = co.wrap(function* (key) {
        let now = new Date();
        if (now <= cache.expiration) {
            return cache.items[key];
        }

        try {
            yield reload();
            return cache.items[key];
        } catch (err) {
            if (cache.items && cache.items.length > 0) {
                // swallow exception if cache is stale, as we'll just try again next time
                console.log('[WARN] swallowing error from SSM Parameter Store:\n', err);

                eventEmitter.emit('refreshError', err);

                return cache.items[key];
            }

            console.log(`[ERROR] couldn't fetch the initial configs : ${keys}`);
            console.error(err);

            throw err;
        }
    });

    let config = {
        onRefresh      : listener => eventEmitter.addListener('refresh', listener),
        onRefreshError : listener => eventEmitter.addListener('refreshError', listener)
    };
    for (let key of keys) {
        Object.defineProperty(config, key, {
            get: function() { return getValue(key); },
            enumerable: true,
            configurable: false
        });
    }

    return config;
}

module.exports = load;
