const { EventEmitter } = require('events');
const fs = require('fs');
const path = require('path');
const { promisify } = require('util');
const mkdirp = require('mkdirp');
const rimraf = require('rimraf');

const noop = () => { };
const EXT = 'mnmntsk';
const readdir = promisify(fs.readdir);
const mkdir = promisify(mkdirp);

class Handler extends EventEmitter {
    constructor(file) {
        super();
        this.path = file.path;
        this.dir = path.dirname(file.path);
        this.length = file.length;
        this.offset = file.offset;
        this.creating = false;
        this.writingback = false;
        this.expression = new RegExp(`^${path.basename(this.path)}\.([0-9]+)\.${EXT}$`);
    }
    async write(offset, buffer) {
        await mkdir(this.dir);
        const ws = await getStreamIfExist(fs.createWriteStream, this.path, { flags: 'r+', start: offset });
        if (ws) {
            this.writeback().catch(noop);
            await promisifiedWrite(ws, buffer);
            return;
        }

        if (this.creating) {
            const created = new Promise((resolve, reject) => {
                this.once('created', resolve);
            });
            await created;
            await this.write(offset, buffer);
            return;
        }

        this.creating = true;
        const newStream = fs.createWriteStream(this.path, { flags: 'w', start: offset });
        try {
            await promisifiedWrite(newStream, buffer);
        } finally {
            this.creating = false;
        }
        this.emit('created', this.path);
    }
    async cache(offset, buffer) {
        await mkdir(this.dir);
        const ws = await getStreamIfExist(fs.createWriteStream, this.path, { flags: 'r+', start: offset });
        if (ws) {
            await promisifiedWrite(ws, buffer);
            return;
        }
        const cacheStream = fs.createWriteStream(`${this.path}.${offset}.${EXT}`, { flags: 'w' });
        await promisifiedWrite(cacheStream, buffer);
    }
    async writeback() {
        if (this.writingback) {
            return;
        }
        this.writingback = true;
        const files = await readdir(this.dir);
        let i = 0;
        for (; i < files.length; i++) {
            const file = files[i];
            const ret = file.match(this.expression);
            if (ret) {
                const offset = parseInt(ret[1]);
                const buff = await readcache(this.path, offset);
                const ws = fs.createWriteStream(this.path, { flags: 'r+', start: offset });
                await promisifiedWrite(ws, buff);
                rimraf(path.join(this.dir, file), { maxBusyTries: 10 }, noop);
            }
        }
        this.writingback = false;
    }
    async read(offset, length) {
        const rs = await getStreamIfExist(fs.createReadStream, this.path, { flags: 'r', start: offset, end: offset + length - 1 });
        return rs ? readStream(rs) : readcache(this.path, offset);
    }

}

function getStreamIfExist(factory, ...args) {
    const stream = factory(...args);
    const deferred = {};
    const promise = new Promise((resolve, reject) => {
        deferred.resolve = resolve;
        deferred.reject = reject;
    });
    const onerror = (error) => {
        if (error.code === 'ENOENT') {
            deferred.resolve(null);
        } else {
            deferred.reject(error);
        }
    };
    stream.once('ready', () => {
        deferred.resolve(stream);
        stream.off('error', onerror)
    });
    stream.once('error', onerror);

    return promise;
}

function promisifiedWrite(stream, buffer) {
    const write = promisify(stream.write.bind(stream));
    try {
        write(buffer);
    } catch (e) {
        throw e;
    } finally {
        stream.close();
        stream.on('error', noop);
    }
}

function readcache(path, offset) {
    const rs = fs.createReadStream(`${path}.${offset}.${EXT}`, { flags: 'r' });
    return readStream(rs);
}

const readStream = (stream) => {
    let data = Buffer.alloc(0);
    const ondata = (chunk) => {
        data = Buffer.concat([data, chunk]);
    }
    const promise = new Promise((resolve, reject) => {
        stream.once('error', reject);
        stream.once('end', () => {
            stream.off('data', ondata);
            stream.off('error', reject);
            stream.on('error', noop);
            resolve(data);
        });
    });
    stream.on('data', ondata);
    return promise;
}

module.exports = Handler;