'use strict';

const DbWatch = require('./');
const Mongodb = require('@samwen/mongodb');

class MyDbWatch extends DbWatch {
    async process_event(db_name, db_cname, event) {
        const { _id, action, update, remove } = event;
        const signature = {db_name, db_cname, _id, action, update, remove};
        console.log(signature);
        console.log(event.document);
    }
}

(async () => {

    const mdb_handle = new Mongodb(/* mdb_url */);

    const my_watch = new MyDbWatch();
    
    my_watch.set_watch(mdb_handle, 'my-db', 'my-collection');

    await my_watch.start();

})();