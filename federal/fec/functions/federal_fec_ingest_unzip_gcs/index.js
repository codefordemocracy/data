/* globals exports, require */
/* jshint node: true */
/* jshint strict: false */
/* jshint esversion: 6 */

"use strict";
const crc32 = require("fast-crc32c");
const gcs = require('@google-cloud/storage')();
const stream = require("stream");
const unzipper = require("unzipper");
const {SecretManagerServiceClient} = require('@google-cloud/secret-manager');
const client = new SecretManagerServiceClient();

async function getProject() {
  const [accessResponse] = await client.accessSecretVersion({name: 'projects/952416783871/secrets/gcp_project_id/versions/1'});
  const responsePayload = accessResponse.payload.data.toString();
  return responsePayload;
}

exports.federal_fec_ingest_unzip_gcs = function(data, context, callback) {
  getProject().then(function(gcp_project_id) {
    const file = data;
    // check that object appears to be zip file in the fec downloads folder
    if (!file.name.includes('.zip') || !file.name.includes('downloads/federal/fec/')) {
      console.log(`Skipping File: ${file.name}`);
      callback();
      return false;
    }
    // unzip file
    console.log(`Processing Zip: ${file.name}`);
    var bucket = gcs.bucket(gcp_project_id);
    var gcsSrcObject = bucket.file(file.name);
    var folder = file.name.replace(".zip", "");
    gcsSrcObject.createReadStream()
    .pipe(unzipper.Parse())
    .on("entry", function(entry) {
        console.log(`Found ${entry.type}: ${entry.path}`);
        var gcsDstObject = bucket.file(`${folder}/${entry.path}`);
        entry
          .pipe(gcsDstObject.createWriteStream())
          .on('error', function(err) {
            console.log(`File Error: ${err}`);
          })
          .on('finish', function() {
            console.log(`File Extracted: ${entry.path}`);
          });
    })
    .promise()
    .then(() => {
      console.log(`Zip Processed: ${file.name}`);
      callback();
    },(err) => {
      console.log(`Zip Error: ${err}`);
    });
  })
};
