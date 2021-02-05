var piece = require('torrent-piece')
const parseTorrent = require('parse-torrent');
const crypto = require('crypto');
const path = require('path')
const { EventEmitter } = require('events');
const Discovery = require('torrent-discovery');
var pws = require('peer-wire-swarm')
var bitfield = require('bitfield')
const { promisify } = require('util')
var Store = require('../store')

const noop = () => { };

var sha1 = function (data) {
    return crypto.createHash('sha1').update(data).digest('hex')
}

class Client extends EventEmitter {
    constructor(buffer, opts) {
        super();
        const result = parseTorrent(buffer)
        var pieceLength = result.pieceLength
        var pieceRemainder = (result.length % pieceLength) || pieceLength
        var pieces = result.pieces.map(function (hash, i) {
            return piece(i === result.pieces.length - 1 ? pieceRemainder : pieceLength)
        })
        const discovery = new Discovery({
            peerId: Buffer.concat([Buffer.from('-FMN001-'), crypto.randomBytes(12)]),
            port: 6881
            // announce: result['announce-list'] ? result['announce-list'] : [result.announce]
        });
        discovery.setTorrent(result);
        discovery.once('error', () => {
            discovery.stop();
        });
        discovery.on('peer', (peer) => {
            // console.log(peer)
            swarm.add(peer)
            update()
        });
        const swarm = pws(result.infoHash, discovery.peerId, { connectTimeout: 10000 });
        swarm.listen(6881);
        swarm.on('wire', (wire, connection) => {
            console.log(wire.peerAddress)
            wire._badrequests = 0

            const ontimeout = () => {
                connection.destroy();
            }
            connection.once('timeout', ontimeout);
            connection.once('close', () => {
                connection.off('timeout', ontimeout);
            });
            connection.setTimeout(10000);

            wire.interested()
            wire.unchoke()

            wire.on('bitfield', update);
            wire.on('have', update);

            var timeout = 5000
            var id

            var onchoketimeout = function () {
                if (swarm.queued > 2 * (swarm.size - swarm.wires.length) && wire.amInterested) return wire.destroy()
                id = setTimeout(onchoketimeout, timeout)
            }

            wire.on('close', function () {
                clearTimeout(id)
            })

            wire.on('choke', function () {
                clearTimeout(id)
                id = setTimeout(onchoketimeout, timeout)
            })

            wire.on('unchoke', function () {
                clearTimeout(id)
            })

            wire.on('request', (index, offset, length, cb) => {
                if (!this.bitfield.get(index)) return
                this.store.get(index, { offset: offset, length: length }, (err, buffer) => {
                    if (err) return cb(err)
                    this.emit('upload', index, offset, length)
                    cb(null, buffer)
                })
            })
        });
        this.swarm = swarm;

        this.selection = [];
        this.selectedFiles = new Set();
        this.toBeDownloaded = new Set();
        this.toBeConsumed = new Set();
        this.files = result.files.map((file) => {
            file = Object.create(file)
            var offsetPiece = (file.offset / result.pieceLength) | 0
            var endPiece = ((file.offset + file.length - 1) / result.pieceLength) | 0

            file.deselect = () => {
                this.deselect(offsetPiece, endPiece)
                this.selectedFiles.delete(path.join(opts.path, file.path))
                this._update()
            }

            file.select = () => {
                this.select(offsetPiece, endPiece, () => {
                    this.emit('file-completed', file)
                })
                this.selectedFiles.add(path.join(opts.path, file.path))
                this._update()
            }

            return file
        });
        this.store = new Store(result.pieceLength, {
            files: result.files.map((file) => {
                return {
                    path: path.join(opts.path, file.path),
                    name: file.path,
                    length: file.length,
                    offset: file.offset
                }
            }),
            selectedFiles: this.selectedFiles
        });
        this.torrent = result;
        this.bitfield = bitfield(result.pieces.length);
        this.checkContent();

        const update = () => {
            clearTimeout(update.timer);
            update.timer = setTimeout(this._produce.bind(this), 100);
            // this.download();
        }
        update.timer = null;

        this.on('piece-downloaded', update);
        this.on('piece-download-failed', update);
        this.on('consume', this.download.bind(this));
        this.on('verify', this._gc.bind(this));
        this.on('file-completed', (file) => {
            console.log(`${file.path} is done.`);
        })
    }
    checkContent() {
        var loop = (i) => {
            if (i >= this.torrent.pieces.length) return this.emit('ready', this.torrent)
            this.store.get(i, (_, buf) => {
                if (!buf || sha1(buf) !== this.torrent.pieces[i]) {
                    return loop(i + 1)
                }
                this.bitfield.set(i, true)
                // this.emit('verify', i)
                loop(i + 1)
            })
        }

        loop(0)
    }
    start() {
    }

    select(from, to, notify = noop) {
        this.selection.push({
            from: from,
            to: to,
            offset: 0,
            notify
        })
    }

    deselect(from, to, priority, notify) {
        for (var i = 0; i < this.selection.length; i++) {
            var s = this.selection[i]
            if (s.from !== from || s.to !== to) continue
            //   if (s.priority !== toNumber(priority)) continue
            //   if (s.notify !== notify) continue
            this.selection.splice(i, 1)
            i--
            break
        }
    }

    _update() {
        let i = 0;
        for (; i < this.selection.length; i++) {
            const selection = this.selection[i];
            let j = selection.from;
            for (; j <= selection.to; j++) {
                if (!this.bitfield.get(j)) {
                    this.toBeDownloaded.add(j);
                } else {
                    this.toBeDownloaded.delete(j);
                }
            }
        }
        this._produce();
    }
    _produce() {
        // this.swarm.wires
        if (this.toBeConsumed.size === 10) {
            this.emit('consume')
            return;
        }
        for (let index of this.toBeDownloaded) {
            if (isAvailable(index, this.swarm.wires) && !this.toBeConsumed.has(index)) {
                this.toBeConsumed.add(index);
                return this._produce();
            }
        }
        this.emit('consume')
    }
    download() {
        for (let index of this.toBeConsumed) {
            for (let wire of this.swarm.wires) {
                if (selectWire(wire, index)) {
                    this.request(index, wire);
                    break;
                }
            }
        }
    }
    async request(index, wire) {
        var pieceLength = this.torrent.pieceLength
        var pieceRemainder = (this.torrent.length % pieceLength) || pieceLength
        const length = index === this.torrent.pieces.length - 1 ? pieceRemainder : pieceLength;
        try {
            const buffer = await requestPiece(wire, index, length);
            if (sha1(buffer) === this.torrent.pieces[index] && !this.bitfield.get(index)) {
                this.store.put(index, buffer, (err) => {
                    if (err) return;
                    this.toBeDownloaded.delete(index);
                    this.toBeConsumed.delete(index);
                    this.bitfield.set(index, true);
                    this.emit('verify', index);
                });
                // this.swarm.wires.forEach(w => {
                //     w.cancel(index, 0, length);
                // })
            } else {
                if (wire._badrequests > 3) {
                    this.swarm.remove(wire.peerAddress);
                    return;
                }
                wire._badrequests += 1;
                // console.dir('invalid buffer')
                this.emit('invalid-piece', index);

            }
            this.emit('piece-downloaded', index);
        } catch (e) {
            console.dir(e.message);
            this.emit('piece-download-failed', index);
        }
    }
    _gc() {
        for (var i = 0; i < this.selection.length; i++) {
            var s = this.selection[i]

            while (this.bitfield.get(s.from + s.offset) && s.from + s.offset < s.to) s.offset++

            if (s.to !== s.from + s.offset) continue
            if (!this.bitfield.get(s.to)) continue
            s.notify()
            // oninterestchange()
        }
    }
}

function isAvailable(index, wires) {
    let peers = 0;
    for (wire of wires) {
        if (!wire.peerChoking && wire.peerPieces[index]) {
            peers++;
        }
    }
    return peers;
}

async function requestPiece(wire, index, pieceLength) {
    const BLOCKLENGTH = 16384;
    let offset = 0;
    let remain = pieceLength;
    const params = [];
    while (remain > BLOCKLENGTH) {
        params.push({
            offset,
            length: BLOCKLENGTH
        });
        remain -= BLOCKLENGTH;
        offset += BLOCKLENGTH;
    }
    params.push({
        offset,
        length: remain
    });
    wire.setTimeout(30000);
    const request = promisify(wire.request.bind(wire));
    let buffer = Buffer.alloc(0);
    for (const { offset, length } of params) {
        const block = await request(index, offset, length);
        buffer = Buffer.concat([buffer, block]);
    }
    return buffer;
}

function selectWire(wire, index) {
    if (wire.peerChoking || !wire.peerPieces[index] || (wire.requests.length >= 3) || hasRequest(wire, index)) {
        return false;
    }
    return true;
}

function hasRequest(wire, index) {
    const req = wire.requests.find(r => r.index === index);
    return !!req;
}

module.exports = Client;