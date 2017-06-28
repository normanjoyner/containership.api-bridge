'use strict';

const _ = require('lodash');
const EventEmitter = require('events');
const net = require('net');
const request = require('request');
const semver = require('semver');

const ApiImplementation = require('@containership/containership.abstraction.api');

class ContainershipApiBridge extends ApiImplementation {
    constructor(ip, port, privateIP, core) {
        super();
        this.apiIP = ip || 'localhost';
        this.apiPort = port || '8081';
        this.myriadPort = 2666;
        this.apiAddr = `http://${this.apiIP}:${port}`;
        this.core = core;
        this.privateIP = privateIP;

        this.defaultErrorHandler = (err) => {
            // eslint-disable-next-line no-console
            console.log(`There was an error in ContainershipApiBridge: ${err}.`);
        };

        // eslint-disable-next-line no-console
        console.log('Api Bridge constructed with: ' + this.apiAddr);
    }

    makeHandler(path, cb) {
        cb = this.formatCallback(cb);

        return (err, resp) => {
            const errFn = cb.errorHandler;
            const resFn = cb.handler;

            if(err) {
                errFn(err);
            } else {
                resFn(_.get(resp, path));
            }

        };

    }

    apiVERB(verb, endpoint, data, path, cb) {
        request({
            baseUrl: this.apiAddr,
            url: endpoint,
            body: data,
            method: verb,
            json: true
        }, this.makeHandler(path, cb));
    }

    apiGET(endpoint, cb, path) {
        path = path || 'body';
        this.apiVERB('GET', endpoint, undefined, path, cb);
    }

    apiPOST(endpoint, data, cb, path) {
        path = path || 'body';
        this.apiVERB('POST', endpoint, data, path, cb);
    }

    apiPUT(endpoint, data, cb, path) {
        path = path || 'body';
        this.apiVERB('PUT', endpoint, data, path, cb);
    }

    apiDELETE(endpoint, cb, path) {
        path = path || 'body';
        this.apiVERB('DELETE', endpoint, null, path, cb);
    }

    getClusterId(cb) {
        this.apiGET('/v1/cluster', cb, ['body', 'id']);
    }

    getApplications(cb) {
        this.apiGET('/v1/applications', cb);
    }

    createContainers(appId, containerConfig, cb) {
        this.apiPOST(`/v1/applications/${appId}/containers?count=${containerConfig.count}`, {}, cb);
    }

    createApplication(appDesc, cb) {
        this.apiPOST(`/v1/applications/${appDesc.id}`, appDesc, cb);
    }

    updateApplication(appId, appDesc, cb) {
        this.apiPUT(`/v1/applications/${appId}`, appDesc, cb);
    }

    deleteApplication(appId, cb) {
        this.apiDELETE(`/v1/applications/${appId}`, cb);
    }

    getHosts(cb) {
        this.apiGET('/v1/hosts', cb);
    }

    discoverPeers(cidr) {
        if(!this.core) {
            throw new Error('A core reference is required to use this method.');
        }

        this.core.cluster.legiond.options.network.cidr = cidr;
        this.core.cluster.legiond.actions.discover_peers(cidr);
    }

    makeSocketRequest(message, cb, closeAfterMessage) {
        closeAfterMessage = closeAfterMessage || true;

        const socket = new net.Socket();

        socket.connect(this.myriadPort, this.privateIP, () => {
            // eslint-disable-next-line no-console
            console.log('Connected!');
            socket.write(message);
            socket.write(ContainershipApiBridge.MYRIAD_DELIMETER);
        });

        socket.on('error', (err) => {
            socket.destroy();
            return cb(err);
        });

        let buffer = '';
        socket.on('data', (data) => {
            buffer += data.toString();

            //Wait until we have a complete message.
            if(!_.includes(buffer, ContainershipApiBridge.MYRIAD_DELIMETER)) {
                return;
            }

            if(closeAfterMessage) {
                socket.end();
            }

            const messageJSON = _.first(_.split(buffer, ContainershipApiBridge.MYRIAD_DELIMIETER));

            let message;

            try {
                message = JSON.parse(messageJSON);
            } catch(err) {
                return cb(null, messageJSON);
            }

            if(!_.isEmpty(message.error)) {
                cb(new Error(message.error));
            } else {
                cb(null, message);
            }

        });
    }

    setDistributedKey(k, v, cb) {
        this.makeSocketRequest(`SET ${k} ${v}`, cb);
    }

    getDistributedKey(k, cb) {
        this.makeSocketRequest(`GET ${k}`, cb);
    }

    // Returns an event emitter.
    subscribeDistributedKey(pattern) {
        const attributes = this.core.cluster.legiond.get_attributes();
        const containership_version = _.get(attributes, 'metadata.containership.version');

        const ee = new EventEmitter();

        if(containership_version && semver.gte(containership_version, '1.8.0')) {
            const command = pattern ? `SUBSCRIBE ${pattern}` : `SUBSCRIBE`;

            this.makeSocketRequest(command, (err, data) => {
                if(err) {
                    ee.emit('error', JSON.stringify({
                        error: err.message
                    }));
                } else {
                    ee.emit('message', data);
                }
            }, false);
        } else {
            ee.emit('error', JSON.stringify({
                error: 'Containership version 1.8.0 or greater required for subscribing to myriad-kv changes!'
            }));
        }

        return ee;
    }

}

ContainershipApiBridge.MYRIAD_DELIMETER = '\r\n';

module.exports = ContainershipApiBridge;
