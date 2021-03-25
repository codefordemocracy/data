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

exports.federal_fec_ingest_download_zip = function(data, context, callback) {
  getProject().then(function(gcp_project_id) {
    const zipurl = data.attributes.zipurl;
    const filename = zipurl.substring(zipurl.lastIndexOf('/') + 1);
    // download file
    console.log(`Downloading Zip: ${filename}`);
    var bucket = gcs.bucket(gcp_project_id);
    if (filename.includes('zip')) {
      var gcsDescObject = bucket.file('downloads/federal/fec/'+filename);
    } else {
      var gcsDescObject = bucket.file('downloads/federal/fec/'+filename.split('.')[0]+'/'+filename);
    }
    const req = request(zipurl);
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
