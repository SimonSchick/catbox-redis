
'use strict';

// Load modules

const Code = require('code');
const Lab = require('lab');
const Catbox = require('catbox');
const { CatboxRedis: Redis, NotConnectedError } = require('..');
const RedisClient = require('ioredis');
const EventEmitter = require('events').EventEmitter;


// Declare internals

const internals = {};


// Test shortcuts

const lab = exports.lab = Lab.script();
const expect = Code.expect;
const describe = lab.describe;
const it = lab.test;
const before = lab.before;
const after = lab.after;


// Utils

const timeoutPromise = (timer) => {

    return new Promise((resolve) => {

        setTimeout(resolve, timer);
    });
};

const makeRedis = async (ropts) => {
    const cl = new RedisClient(ropts);
    await new Promise((resolve, reject) => {
        cl.once('error', reject);
        if (ropts && ropts.lazyConnect) {
            resolve();
            return;
        }
        cl.once('ready', resolve);
    });
    return cl;
}

const makeClient = async (opts, ropts) => new Catbox.Client(Redis, {
    ...opts,
    client: await makeRedis(ropts),
});

const makeCBRedis = async (opts, ropts) =>
    new Redis({
        ...opts,
        client: await makeRedis(ropts),
});

describe('Redis', () => {
    it('creates a new connection', async () => {

        const client = await makeClient();
        await client.start();
        expect(client.isReady()).to.equal(true);
    });

    it('closes the connection', async () => {

        const client = await makeClient();
        await client.start();
        expect(client.isReady()).to.equal(true);
        await client.stop();
        expect(client.isReady()).to.equal(false);
    });

    it('allow passing client in option', async () => {

        const redisClient = await makeRedis();

        let getCalled = false;
        const _get = redisClient.get;
        redisClient.get = function (key, callback) {

            getCalled = true;
            return _get.apply(redisClient, arguments);
        };

        const client = new Catbox.Client(Redis, {
            client: redisClient
        });
        await client.start();
        expect(client.isReady()).to.equal(true);
        const key = { id: 'x', segment: 'test' };
        await client.get(key);
        expect(getCalled).to.equal(true);
    });

    it('does not stop provided client in options', async () => {

        const redisClient = await makeRedis();

        const client = new Catbox.Client(Redis, {
            client: redisClient
        });
        await client.start();
        expect(client.isReady()).to.equal(true);
        await client.stop();
        expect(client.isReady()).to.equal(false);
        expect(redisClient.status).to.equal('ready');
        await redisClient.quit();
    });

    it('gets an item after setting it', async () => {

        const client = await makeClient();
        await client.start();

        const key = { id: 'x', segment: 'test' };
        await client.set(key, '123', 500);

        const result = await client.get(key);
        expect(result.item).to.equal('123');
    });

    it('fails setting an item circular references', async () => {

        const client = await makeClient();
        await client.start();
        const key = { id: 'x', segment: 'test' };
        const value = { a: 1 };
        value.b = value;

        await expect(client.set(key, value, 10)).to.reject(Error, 'Converting circular structure to JSON');
    });

    it('ignored starting a connection twice on same event', () => {

        return new Promise(async (resolve, reject) => {

            const client = await makeClient();
            let x = 2;
            const start = async () => {

                await client.start();
                expect(client.isReady()).to.equal(true);
                --x;
                if (!x) {
                    resolve();
                }
            };

            start();
            start();
        });
    });

    it('ignored starting a connection twice chained', async () => {

        const client = await makeClient();

        await client.start();
        expect(client.isReady()).to.equal(true);

        await client.start();
        expect(client.isReady()).to.equal(true);
    });

    it('returns not found on get when using null key', async () => {

        const client = await makeClient();
        await client.start();

        const result = await client.get(null);

        expect(result).to.equal(null);
    });

    it('returns not found on get when item expired', async () => {

        const client = await makeClient();
        await client.start();

        const key = { id: 'x', segment: 'test' };
        await client.set(key, 'x', 1);

        await timeoutPromise(2);
        const result = await client.get(key);
        expect(result).to.equal(null);
    });

    it('returns error on set when using null key', async () => {

        const client = await makeClient();
        await client.start();

        await expect(client.set(null, {}, 1000)).to.reject(Error);
    });

    it('returns error on get when using invalid key', async () => {

        const client = await makeClient();
        await client.start();

        await expect(client.get({})).to.reject(Error);
    });

    it('returns error on drop when using invalid key', async () => {

        const client = await makeClient();
        await client.start();

        await expect(client.drop({})).to.reject(Error);
    });

    it('returns error on set when using invalid key', async () => {

        const client = await makeClient();
        await client.start();

        await expect(client.set({}, {}, 1000)).to.reject(Error);
    });

    it('ignores set when using non-positive ttl value', async () => {

        const client = await makeClient();
        await client.start();
        const key = { id: 'x', segment: 'test' };
        await client.set(key, 'y', 0);
    });

    it('returns error on drop when using null key', async () => {

        const client = await makeClient();
        await client.start();

        await expect(client.drop(null)).to.reject(Error);
    });

    it('returns error on get when stopped', async () => {

        const client = await makeClient();
        await client.stop();

        const key = { id: 'x', segment: 'test' };
        await expect(client.connection.get(key)).to.reject(NotConnectedError);
    });

    it('returns error on set when stopped', async () => {

        const client = await makeClient();
        await client.stop();

        const key = { id: 'x', segment: 'test' };
        await expect(client.connection.set(key, 'y', 1)).to.reject(NotConnectedError);
    });

    it('returns error on drop when stopped', async () => {

        const client = await makeClient();
        await client.stop();

        const key = { id: 'x', segment: 'test' };
        await expect(client.connection.drop(key)).to.reject(NotConnectedError);
    });

    it('returns error on missing segment name', async () => {
        await expect((async () => {
            const client = await makeClient();
            new Catbox.Policy({
                expiresIn: 50000
            }, client, '');
        })()).to.reject();
    });

    it('returns error on bad segment name', async () => {
        await expect((async () => {
            const client = await makeClient();
            new Catbox.Policy({
                expiresIn: 50000
            }, client, 'a\0b');
        })()).to.reject();
    });

    it('returns error when cache item dropped while stopped', async () => {

        const client = await makeClient();
        await client.stop();

        await expect(client.drop('a')).to.reject(Error);
    });

    describe('start()', () => {

        it('sets client to when the connection succeeds', async () => {

            const redis = await makeCBRedis();

            await redis.start();
            expect(redis.client).to.exist();
        });

        it('reuses the client when a connection is already started', async () => {
            const redis = await makeCBRedis();

            await redis.start();
            const client = redis.client;

            await redis.start();
            expect(client).to.equal(redis.client);
        });

        it('returns an error when connection fails', async () => {

            const redis = await makeCBRedis({}, {
                port: 6730,
                lazyConnect: true,
            });

            await expect(redis.start()).to.reject(Error);
        });
    });

    describe('isReady()', () => {

        it('returns true when when connected', async () => {

            const redis = await makeCBRedis();
            await redis.start();
            expect(redis.client).to.exist();
            expect(redis.isReady()).to.equal(true);
            await redis.stop();
        });

        it('returns false when stopped', async () => {

            const redis = await makeCBRedis();

            await redis.start();
            expect(redis.client).to.exist();
            expect(redis.isReady()).to.equal(true);
            await redis.stop();
            expect(redis.isReady()).to.equal(false);
        });
    });

    describe('validateSegmentName()', () => {

        it('returns an error when the name is empty', async () => {

            const redis = await makeClient();

            const result = redis.validateSegmentName('');

            expect(result).to.be.instanceOf(Error);
            expect(result.message).to.equal('Empty string');
        });

        it('returns an error when the name has a null character', async () => {

            const redis = await makeClient();

            const result = redis.validateSegmentName('\0test');

            expect(result).to.be.instanceOf(Error);
        });

        it('returns null when there aren\'t any errors', async () => {

            const redis = await makeClient();

            const result = redis.validateSegmentName('valid');

            expect(result).to.not.be.instanceOf(Error);
            expect(result).to.equal(null);
        });
    });

    describe('get()', () => {

        it('returns a promise that rejects when the connection is closed', async () => {

            const redis = await makeClient();
            await redis.stop();

            await expect(redis.get('test')).to.reject(NotConnectedError);
        });

        it('returns a promise that rejects when there is an error returned from getting an item', async () => {

            const redis = await makeClient();
            redis.client = {
                async get (item) { throw new Error(); }
            };

            await expect(redis.get('test')).to.reject(Error);
        });

        it('returns a promise that rejects when there is an error parsing the result', async () => {

            const redis = await makeCBRedis();
            redis.client = {
                async get (item) { return 'test'; }
            };

            await expect(redis.get('test')).to.reject(Error, 'Bad envelope content');
        });

        it('returns a promise that rejects when there is an error with the envelope structure (stored)', async () => {

            const redis = await makeCBRedis();
            redis.client = {
                async get (item) { return '{ "item": "false" }' },
            };

            await expect(redis.get('test')).to.reject(Error, 'Incorrect envelope structure');
        });

        it('returns a promise that rejects when there is an error with the envelope structure (item)', async () => {

            const redis = await makeCBRedis();
            redis.client = {
                async get (item) { return '{ "stored": "123" }'; }
            };

            await expect(redis.get('test')).to.reject(Error, 'Incorrect envelope structure');
        });

        it('is able to retrieve an object thats stored when connection is started', async () => {

            const key = {
                id: 'test',
                segment: 'test'
            };

            const redis = await makeClient({
                partition: 'wwwtest',
            });;
            await redis.start();
            await redis.set(key, 'myvalue', 200);
            const result = await redis.get(key);
            expect(result.item).to.equal('myvalue');
        });

        it('returns null when unable to find the item', async () => {

            const key = {
                id: 'notfound',
                segment: 'notfound'
            };

            const redis = await makeClient({
                partition: 'wwwtest',
            });
            await redis.start();
            const result = await redis.get(key);
            expect(result).to.not.exist();
        });

        it('can store and retrieve falsy values such as int 0', async () => {

            const key = {
                id: 'test',
                segment: 'test'
            };

            const redis = await makeClient({
                partition: 'wwwtest',
            });
            await redis.start();
            await redis.set(key, 0, 200);
            const result = await redis.get(key);
            expect(result.item).to.equal(0);
        });
    });

    describe('set()', () => {

        it('returns a promise that rejects when the connection is closed', async () => {

            const redis = await makeCBRedis();
            await redis.stop();

            await expect(redis.set('test1', 'test1', 3600)).to.reject(NotConnectedError);
        });

        it('returns a promise that rejects when there is an error returned from setting an item', async () => {

            const redis = await makeCBRedis();
            redis.client = {
                async set (key, item, callback) { throw new Error(); },
            };

            await expect(redis.set('test', 'test', 3600)).to.reject(Error);
        });
    });

    describe('drop()', () => {

        it('returns a promise that rejects when the connection is closed', async () => {

            const redis = await makeCBRedis();

            await redis.stop();
            await expect( redis.drop('test2')).to.reject(NotConnectedError);
        });

        it('deletes the item from redis', async () => {

            const redis = await makeCBRedis();
            redis.client = {
                async del (key) {
                    return null;
                }
            };

            await redis.drop('test');
        });
    });

    describe('generateKey()', () => {

        it('generates the storage key from a given catbox key', async () => {

            const redis = await makeCBRedis({
                partition: 'foo'
            });

            const key = {
                id: 'bar',
                segment: 'baz'
            };

            expect(redis.generateKey(key)).to.equal('foo:baz:bar');
        });

        it('generates the storage key from a given catbox key without partition', async () => {

            const redis = await makeCBRedis();

            const key = {
                id: 'bar',
                segment: 'baz'
            };

            expect(redis.generateKey(key)).to.equal('baz:bar');
        });
    });

    describe('stop()', () => {

        it('sets the client to null', async () => {

            const redis = await makeCBRedis();

            await redis.start();
            expect(redis.client).to.exist();
            await redis.stop();
            expect(redis.client).to.not.exist();
        });
    });
});
