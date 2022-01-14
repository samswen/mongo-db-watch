'use strict';

const DbWatch = require('./');
const Mongodb = require('@samwen/mongodb');

class MyDbWatch extends DbWatch {
    async process_events(events) {
        for (const event of events) {
            const {db_name, db_cname, operationType, documentKey, updateDescription } = event;
            const { updatedFields = null, removedFields = null  } = updateDescription ? updateDescription : {};
            const signature = {db_name, db_cname, _id: documentKey._id, action: operationType, 
                update: updatedFields, remove: removedFields};
            console.log(signature);
            console.log(event.fullDocument);
        }
    }
}

(async () => {

    const mdb_handle = new Mongodb(/* mdb_url */);

    const my_watch = new MyDbWatch();
    
    my_watch.set_watch(mdb_handle, 'my-db', 'my-collection');

    await my_watch.start();

})();