'use strict';

const os = require('os');
const fs = require('fs');
const node_path = require('path');
const resume_token_util = require('./resume-token');
const default_cfg = require('./default-cfg');

/**
 * watch change stream of mongodb collections
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
        this.events_queue = [];
    }

    set_watch(db_handle, db_name, db_cname, match, projection, full_document) {
        if (!db_handle || !db_name || !db_cname) {
            throw new Error('missing required db_handle, db_name and/or db_cname');
        }
        if (this.change_streams.find(x => x.db_name === db_name && x.db_cname === db_cname)) {
            console.error('the same watch settings already existed');
            return false;
        }
        if (full_document === undefined) full_document = this.config.full_document;
        if (!full_document && projection && Object.keys(projection).length > 0) full_document = 'updateLookup';
        const change_stream = { db_handle, db_name, db_cname, match, projection, full_document, 
            stopped: false, total_events: 0, start_ms: Date.now() };
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
            let events = [];
            while(this.events_queue.length > 0) {
                const event = this.events_queue.shift();
                if (!event) break;
                events.push(event);
                if (events.length === this.config.max_events) {
                    promises.push(this.process_events(events));
                    events = [];
                    if (promises.length === this.config.max_promises) {
                        await Promise.all(promises);
                        promises.length = 0;;
                    }
                }
            }
            if (events.length > 0) {
                promises.push(this.process_events(events));
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
            console.error(err);
        }
        change_stream.stopped = true;
    }

    async run_stream(change_stream) {
        const { db_handle, db_name, db_cname, match, projection, full_document } = change_stream;
        console.log('mongo start', db_name, db_cname);
        const pipeline = [];
        if (match && Object.keys(match).length > 0) {
            pipeline.push({ $match: match });
        }
        if (projection && Object.keys(projection).length > 0) {
            pipeline.push({$project: {...projection, _id: 1, operationType: 1, documentKey: 1, updateDescription: 1}});
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
            change_stream.last_event_time = new Date();
            change_stream.total_events++;
            const event = this.transform_event(db_name, db_cname, change_event);
            this.events_queue.push(event);
            resume_token_util.save(data_dir, db_name, db_cname, resume_token);
        }
        await cursor.close();
    }

    transform_event(db_name, db_cname, change_event) {
        return {db_name, db_cname, ...change_event};
    }

    async process_events(events) {
        console.log(events);
    }

    stop() {
        this.stopped = true;
        for (const change_stream of this.change_streams) change_stream.stopped = true;
    }
    
    status() {
        const result = {running: !this.stopped, pending_events: this.events_queue.length};
        const streams = [];
        for (const {db_name, db_cname, stopped, last_event_time, total_events, start_ms} of this.change_streams) {
            const events_per_second = (total_events * 1000 / (Date.now() - start_ms + 1)).toFixed(4)
            streams.push({db_name, db_cname, running: !stopped, last_event_time, total_events, events_per_second});
        }
        result.streams = streams;
        return result;
    }
}

module.exports = DbWatch;
