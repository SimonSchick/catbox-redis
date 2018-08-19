"use strict";
// TODO: Go back to use catbox-redis package when they update
Object.defineProperty(exports, "__esModule", { value: true });
class NotConnectedError extends Error {
    constructor() {
        super(...arguments);
        this.message = 'Client not connected';
    }
}
exports.NotConnectedError = NotConnectedError;
class CatboxRedis {
    constructor(options) {
        this.client = options.client;
        this.partition = options.partition;
    }
    async start() {
        if (this.client.status !== 'wait') {
            return;
        }
        await this.client.connect();
    }
    stop() {
        // noop
    }
    isReady() {
        return this.client.status === 'ready';
    }
    validateSegmentName(name) {
        if (!name) {
            return new Error('Empty string');
        }
        if (name.includes('\0')) {
            return new Error('Includes null character');
        }
        return null;
    }
    async get(key) {
        if (!this.isReady()) {
            throw new NotConnectedError();
        }
        const result = await this.client.get(this.generateKey(key));
        if (!result) {
            return null;
        }
        let envelope = null;
        try {
            envelope = JSON.parse(result);
        }
        catch (err) {
            // Handled by validation below
        }
        if (!envelope) {
            throw new Error('Bad envelope content');
        }
        if ((!envelope.item && envelope.item !== 0) ||
            !envelope.stored) {
            throw new Error('Incorrect envelope structure');
        }
        return envelope;
    }
    async set(key, value, ttl) {
        if (!this.isReady()) {
            throw new NotConnectedError();
        }
        const cacheKey = this.generateKey(key);
        await this.client.set(cacheKey, JSON.stringify({
            item: value,
            stored: Date.now(),
            ttl,
        }), 'ex', Math.max(1, Math.floor(ttl / 1000)));
    }
    async drop(key) {
        if (!this.isReady()) {
            throw new NotConnectedError();
        }
        return this.client.del(this.generateKey(key));
    }
    generateKey(key) {
        const parts = [];
        if (this.partition) {
            parts.push(encodeURIComponent(this.partition));
        }
        parts.push(encodeURIComponent(key.segment));
        parts.push(encodeURIComponent(key.id));
        return parts.join(':');
    }
}
exports.CatboxRedis = CatboxRedis;
//# sourceMappingURL=catbox-redis.js.map