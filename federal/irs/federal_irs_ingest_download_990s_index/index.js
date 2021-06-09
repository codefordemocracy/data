'use strict';

const request = require('request');
const gcs = require('@google-cloud/storage')();
const {SecretManagerServiceClient} = require('@google-cloud/secret-manager');
const client = new SecretManagerServiceClient();

async function getProject() {
  const [accessResponse] = await client.accessSecretVersion({name: 'projects/952416783871/secrets/gcp_project_id/versions/1'});
  const responsePayload = accessResponse.payload.data.toString();
  return responsePayload;
}

exports.federal_irs_ingest_download_990s_index = function(data, context, callback) {
  getProject().then(function(gcp_project_id) {
    const current_year = new Date().getFullYear();
    const url = 'https://s3.amazonaws.com/irs-form-990/index_' + String(current_year) + '.csv'
    const filename = url.substring(url.lastIndexOf('/') + 1);
    // download file
    console.log(`Downloading File: ${filename}`);
    var bucket = gcs.bucket(gcp_project_id);
    var gcsDescObject = bucket.file('downloads/federal/irs/'+filename);
    const req = request(url);
    req.pause();
    req.on('response', res => {
      // Don't set up the pipe to the write stream unless the status is ok.
      // See https://stackoverflow.com/a/26163128/2669960 for details.
      if (res.statusCode !== 200) {
        return;
      }
      // build the stream
      var writeStream = gcsDescObject.createWriteStream({
        metadata: {
          contentType: res.headers['content-type']
        }
      });
      req.pipe(writeStream)
        .on('finish', () => {
          console.log(`Download Finished: ${filename}`)
          callback();
        })
        .on('error', err => {
          writeStream.end();
          console.log(`Download Error: ${err}`);
          callback();
        });
      // Resume only when the pipe is set up.
      req.resume();
    });
    req.on('error', err => {
      console.log(`Download Error: ${err}`)
      callback();
    });
  })
}
