'use strict';

const DbWatch = require('./');
const Mongodb = require('@samwen/mongodb');

class MyDbWatch extends DbWatch {
    async process_event(db_name, db_cname, event) {
        console.log({db_name, db_cname});
        const signature = {db_name, db_cname, _id: event._id, action: event.operationType, update: event.updateDescription, remove_fields: event.removedFields};
        console.log(signature);
        console.log(event.fullDocument);
    }
}

(async () => {

    const mdb_handle = new Mongodb(/* mdb_url */);

    const my_watch = new MyDbWatch();
    
    my_watch.set_watch(mdb_handle, 'my-db', 'my-collection');

    await my_watch.start();

})();