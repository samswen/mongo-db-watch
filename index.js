'use strict';

const os = require('os');
const fs = require('fs');
const node_path = require('path');
const resume_token_util = require('./resume-token');
const default_cfg = require('./default-cfg');

/**
 * watch change stream of mongodb collection
 */
class DbWatch {

    constructor(config = {}) {
        this.config = { ...default_cfg };
        for (const key in default_cfg) {
            if (config.hasOwnProperty(key)) this.config[key] = config[key];
        }
        if (!this.config.data_dir) {
            this.config.data_dir = node_path.join(os.homedir(), '.db-watch');
        }
        if (!fs.existsSync(this.config.data_dir)) {
            fs.mkdirSync(this.config.data_dir, {recursive: true});
        }
        this.change_streams = [];
        this.event_queue = [];
    }

    set_watch(db_handle, db_name, db_cname, projection, actions_filter, full_document = 'updateLookup') {
        if (!db_handle || !db_name || !db_cname) {
            throw new Error('missing required db_handle, db_name and/or db_cname');
        }
        if (this.change_streams.find(x => x.db_name === db_name && x.db_cname === db_cname)) {
            console.error('the same watch settings already existed');
            return false;
        }
        const change_stream = { db_handle, db_name, db_cname, projection, actions_filter, full_document, stopped: false };
        this.change_streams.push(change_stream);
        this.process_stream(change_stream);
        return true;
    }

    async start() {
        this.stopped = false;
        while(!this.stopped) {
            const start_ms = Date.now();
            for (const change_stream of this.change_streams) {
                if (change_stream.stopped && 
                    (!change_stream.restarted_at || start_ms - change_stream.restarted_at > 30000)) {
                    change_stream.stopped = false;
                    change_stream.restarted_at = start_ms;
                    this.process_stream(change_stream);
                }
            }
            const promises = [];
            while(this.event_queue.length > 0) {
                const queue_item = this.event_queue.shift();
                if (!queue_item) break;
                const {db_name, db_cname, event} = queue_item;
                promises.push(this.process_event(db_name, db_cname, event));
                if (promises.length === this.config.batch_size) {
                    await Promise.all(promises);
                    promises.length = 0;
                }
            }
            if (promises.length > 0) {
                await Promise.all(promises);
            }
            const duration_ms = Date.now() - start_ms;
            const { minima_duration_ms = 250 } = this.config;
            if (duration_ms < minima_duration_ms) {
                await new Promise(resolve => setTimeout(resolve, minima_duration_ms - duration_ms))
            }
        }
    }

    async process_stream(change_stream) {
        try {
            await this.run_stream(change_stream);
        } catch(err) {
            change_stream.stopped = true;
            console.error(err);
        }
    }

    async run_stream(change_stream) {
        const { db_handle, db_name, db_cname, projection, actions_filter, full_document } = change_stream;
        console.log('mongo start', db_name, db_cname);
        const pipeline = [];
        if (projection && Object.keys(projection).length > 0) {
            pipeline.push({$project: projection})
        }
        if (actions_filter && actions_filter.length > 0) {
            pipeline.unshift({ $match: { operationType: { $in: actions_filter }}});
        }
        const data_dir = this.config.data_dir;
        let resume_token = resume_token_util.load(data_dir, db_name, db_cname);
        const handle = await db_handle.open(db_name, db_cname);
        const { batch_size = 1024, max_await_time_ms = 1000 } = this.config;
        const opts = {batchSize: batch_size, maxAwaitTimeMS: max_await_time_ms};
        if (full_document) opts.fullDocument = full_document;
        let cursor = await handle.watch(pipeline, {resumeAfter: resume_token, ...opts});
        let check_resume_token;
        try { 
            check_resume_token = await cursor.hasNext(); 
        } catch (err) {
            resume_token_util.remove(data_dir, db_name, db_cname);
            console.error(err.message);
        }
        if (check_resume_token === undefined) {
            resume_token = null;
            cursor = await handle.watch(pipeline, {resumeAfter: resume_token, ...opts});
        }
        // cursor.hasNext() will wait until there is next
        while (!change_stream.stopped && await cursor.hasNext()) {
            const change_event = await cursor.next();
            resume_token = change_event._id;
            const {operationType, fullDocument, documentKey, updateDescription} = change_event;
            const { updatedFields = null, removedFields = null  } = updateDescription ? updateDescription : {};
            const event = {_id: documentKey._id, action: operationType, update: updatedFields, remove: removedFields, document: fullDocument};
            this.event_queue.push({db_name, db_cname, event});
            resume_token_util.save(data_dir, db_name, db_cname, resume_token);
        }
        await cursor.close();
    }

    async process_event(db_name, db_cname, event) {
        console.log(db_name, db_cname, event);
    }

    stop() {
        this.stopped = true;
        for (const change_stream of this.change_streams) change_stream.stopped = true;
    }    
}

module.exports = DbWatch;
