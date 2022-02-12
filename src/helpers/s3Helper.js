const _ = require('lodash');
const AWS = require('aws-sdk');
var zlib = require('zlib');
const { DateTime } = require("luxon");

const s3 = new AWS.S3();

const s3Bucket = process.env.OPENTRANSIT_S3_BUCKET;
if (!s3Bucket) {
  throw new Error("Missing OPENTRANSIT_S3_BUCKET environment variable");
}
console.log(`Reading state from s3://${s3Bucket}`);

const stateVersion = 'v1';

/*
 * Gets bucket prefix at the minute-level
 * @param agencyId - String
 * @param timestamp - Number (Unix timestamp in seconds)
 * @return prefix - String
 */
function getBucketHourPrefix(agencyId, timestamp) {
  const dateTime = DateTime.fromSeconds(timestamp, {zone:'UTC'});
  const dateTimePathSegment = dateTime.toFormat('yyyy/MM/dd/HH');
  return `state/${stateVersion}/${agencyId}/${dateTimePathSegment}/`;
}

function getS3Paths(prefix) {
  return new Promise((resolve, reject) => {
    s3.listObjects({
      Bucket: s3Bucket,
      Prefix: prefix,
    }, (err, data) => {
      if (err) {
        reject(err);
      } else {
        resolve(data.Contents.map(obj => obj.Key));
      }
    });
  });
}

/*
 * @param startEpoch - Number
 * @param endEpoch - Number
 * @return s3Files - [String]
 */
async function getVehiclePaths(agencyId, startEpoch, endEpoch) {
  if (!endEpoch) {
    endEpoch = startEpoch + 60;
  }
  // There are typically about 4*60=240 state data files per hour,
  // and the S3 API can return up to 1000 key names with a particular prefix
  // (by default), so we can request all keys prefixed by each hour within
  // the time range, then filter the resulting keys to make sure the timestamps
  // are in the requested interval
  let hourPrefixes = [];

  // UTC hours always start with a timestamp at multiples of 3600 seconds
  const startHour = startEpoch - (startEpoch % 3600);

  for (let time = startHour; time < endEpoch; time += 3600) {
    hourPrefixes.push(getBucketHourPrefix(agencyId, time));
  }
  let files = _.flatten(await Promise.all(hourPrefixes.map(prefix => getS3Paths(prefix))));

  let timestampsMap = {};
  let res = [];

  files.map(key => {
     const timestamp = getTimestamp(key);
     if (timestamp >= startEpoch && timestamp < endEpoch && !timestampsMap[timestamp]) {
         timestampsMap[timestamp] = true;
         res.push(key);
     }
  });
  return res;
}

// unzip the gzip data
function decompressData(data) {
  return new Promise((resolve, reject) => {
    return zlib.unzip(data, (err, decoded) => {
      if (err) {
        reject(err);
      } else {
        var parsedData;
        try {
          parsedData = JSON.parse(decoded.toString());
        } catch (e) {
          reject(e);
        }
        resolve(parsedData);
      }
    });
  });
}

/*
 * Downloads and unzips the S3 files
 */
async function getVehicles(agencyId, startEpoch, endEpoch) {
  const keys = await getVehiclePaths(agencyId, startEpoch, endEpoch);

  return _.flatten(await Promise.all(keys.map(key => {
      return new Promise((resolve, reject) => {
        s3.getObject({
          Bucket: s3Bucket,
          Key: key,
        }, (err, data) => {
          if (err) {
            reject(err);
          } else {
              const timestamp = getTimestamp(key);
              decompressData(data.Body)
                .then(decodedData =>
                  resolve(insertTimestamp(timestamp, decodedData)));
          }
        });
      }).catch((err) => {
        return Promise.reject(`Error loading s3://${s3Bucket}/${key}: ${err}`);
      });
  })));
}

function getTimestamp(key) {
    const keyParts = key.split('_');
    return Math.floor(Number(keyParts[keyParts.length - 1].split('.json')[0])/1000);
}

/*
 * The API defines timestamp (epoch time in seconds) as a field for each vehicle,
 * which was also a column in Cassandra.
 * Since the timestamp is in the key in S3, that field does not exist,
 * thus we have to add it in the S3Helper to maintain compatibility
 */
function insertTimestamp(timestamp, vehicles) {
  return vehicles.map(vehicle => {
    return {
      ...vehicle,
      timestamp: timestamp,
    };
  });
}

module.exports = {
  getVehicles,
};
