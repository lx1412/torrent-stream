const path = require('path');
const { randomBytes } = require('crypto');
var parallel = require('run-parallel')
const Handler = require('./handler');

class Store {
    constructor(chunkLength, opts = {}) {
        this.chunkLength = Number(chunkLength);
        if (!this.chunkLength) throw new Error('First argument must be a chunk length');
        if (opts.files) {
            if (!Array.isArray(opts.files)) {
                throw new Error('`files` option must be an array')
            }
            this.files = opts.files.slice(0).map(function (file, i, files) {
                if (file.path == null) throw new Error('File is missing `path` property')
                if (file.length == null) throw new Error('File is missing `length` property')
                if (file.offset == null) {
                    if (i === 0) {
                        file.offset = 0
                    } else {
                        var prevFile = files[i - 1]
                        file.offset = prevFile.offset + prevFile.length
                    }
                }
                return new Handler(file);
            })
            this.length = this.files.reduce(function (sum, file) { return sum + file.length }, 0)
            if (opts.length != null && opts.length !== this.length) {
                throw new Error('total `files` length is not equal to explicit `length` option')
            }
        } else {
            var len = Number(opts.length) || Infinity
            this.files = [{
                offset: 0,
                path: path.resolve(opts.path || path.join(TMP, 'fs-chunk-store', randomBytes(20).toString('hex'))),
                length: len
            }]
            this.length = len
        }

        this.chunkMap = []
        this.closed = false
        this.selectedFiles = opts.selectedFiles

        if (this.length !== Infinity) {
            this.lastChunkLength = (this.length % this.chunkLength) || this.chunkLength
            this.lastChunkIndex = Math.ceil(this.length / this.chunkLength) - 1

            this.files.forEach((file) => {
                var fileStart = file.offset
                var fileEnd = file.offset + file.length

                var firstChunk = Math.floor(fileStart / this.chunkLength)
                var lastChunk = Math.floor((fileEnd - 1) / this.chunkLength)

                for (var p = firstChunk; p <= lastChunk; ++p) {
                    var chunkStart = p * this.chunkLength
                    var chunkEnd = chunkStart + this.chunkLength

                    var from = (fileStart < chunkStart) ? 0 : fileStart - chunkStart
                    var to = (fileEnd > chunkEnd) ? this.chunkLength : fileEnd - chunkStart
                    var offset = (fileStart > chunkStart) ? 0 : chunkStart - fileStart

                    if (!this.chunkMap[p]) this.chunkMap[p] = []

                    this.chunkMap[p].push({
                        from: from,
                        to: to,
                        offset: offset,
                        file: file
                    })
                }
            })
        } else {
            throw new Error('infinity length')
        }
    }
    put(index, buf, cb) {
        var self = this
        if (typeof cb !== 'function') cb = noop
        if (self.closed) return nextTick(cb, new Error('Storage is closed'))

        var isLastChunk = (index === self.lastChunkIndex)
        if (isLastChunk && buf.length !== self.lastChunkLength) {
            return nextTick(cb, new Error('Last chunk length must be ' + self.lastChunkLength))
        }
        if (!isLastChunk && buf.length !== self.chunkLength) {
            return nextTick(cb, new Error('Chunk length must be ' + self.chunkLength))
        }

        if (self.length === Infinity) {
            self.files[0].open(function (err, file) {
                if (err) return cb(err)
                file.write(index * self.chunkLength, buf, cb)
            })
        } else {
            var targets = self.chunkMap[index]
            if (!targets) return nextTick(cb, new Error('no files matching the request range'))

            var tasks = targets.map((target) => {
                return function (cb) {
                    var data = buf.slice(target.from, target.to)
                    if (!self.selectedFiles.has(target.file.path) && (data.length !== target.file.length)) {
                        return target.file.cache(target.offset, data).then(cb).catch(cb)
                    }
                    target.file.write(target.offset, data).then(cb).catch(cb)
                }
            })
            parallel(tasks, cb)
        }
    }
    get(index, opts, cb) {
        var self = this
        if (typeof opts === 'function') return self.get(index, null, opts)
        if (self.closed) return nextTick(cb, new Error('Storage is closed'))

        var chunkLength = (index === self.lastChunkIndex)
            ? self.lastChunkLength
            : self.chunkLength

        var rangeFrom = (opts && opts.offset) || 0
        var rangeTo = (opts && opts.length) ? rangeFrom + opts.length : chunkLength

        if (rangeFrom < 0 || rangeFrom < 0 || rangeTo > chunkLength) {
            return nextTick(cb, new Error('Invalid offset and/or length'))
        }

        if (self.length === Infinity) {
            if (rangeFrom === rangeTo) return nextTick(cb, null, Buffer.from(0))
            self.files[0].open(function (err, file) {
                if (err) return cb(err)
                var offset = (index * self.chunkLength) + rangeFrom
                file.read(offset, rangeTo - rangeFrom, cb)
            })
        } else {
            var targets = self.chunkMap[index]
            if (!targets) return nextTick(cb, new Error('no files matching the request range'))
            if (opts) {
                targets = targets.filter(function (target) {
                    return target.to > rangeFrom && target.from < rangeTo
                })
                if (targets.length === 0) {
                    return nextTick(cb, new Error('no files matching the requested range'))
                }
            }
            if (rangeFrom === rangeTo) return nextTick(cb, null, Buffer.from(0))

            var tasks = targets.map(function (target) {
                return function (cb) {
                    var from = target.from
                    var to = target.to
                    var offset = target.offset

                    if (opts) {
                        if (to > rangeTo) to = rangeTo
                        if (from < rangeFrom) {
                            offset += (rangeFrom - from)
                            from = rangeFrom
                        }
                    }

                    target.file.read(offset, to - from).then((buff) => cb(null, buff)).catch(cb)
                }
            })

            parallel(tasks, function (err, buffers) {
                if (err) return cb(err)
                cb(null, Buffer.concat(buffers))
            })
        }
    }
}

function nextTick(cb, err, val) {
    process.nextTick(function () {
        if (cb) cb(err, val)
    })
}

function noop() { }

module.exports = Store;
