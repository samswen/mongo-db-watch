'use strict';

const fs = require('fs');
const node_path = require('path');

module.exports = {
    load,
    save,
    remove
};

function load(data_dir, db_name, db_cname) {
    const resume_token_filepath = node_path.join(data_dir, `${db_name}-${db_cname}.json`);
    let resume_token = 0;
    if (fs.existsSync(resume_token_filepath)) {
        try {
            const result = JSON.parse(fs.readFileSync(resume_token_filepath, {encoding:'utf8', flag:'r'}));
            resume_token = result.resume_token;
        } catch(err) {
            console.error(err.message);
        }
    }
    return resume_token;
}

function save(data_dir, db_name, db_cname, resume_token) {
    const resume_token_filepath = node_path.join(data_dir, `${db_name}-${db_cname}.json`);
    fs.writeFileSync(resume_token_filepath, JSON.stringify({resume_token}));
}

function remove(data_dir, db_name, db_cname) {
    const resume_token_filepath = node_path.join(data_dir, `${db_name}-${db_cname}.json`);
    if (fs.existsSync(resume_token_filepath)) fs.unlinkSync(resume_token_filepath);
}