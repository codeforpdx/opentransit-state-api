const _ = require('lodash');
const AWS = require('aws-sdk');
var zlib = require('zlib');

const s3 = new AWS.S3();

const s3Bucket = process.env.TRYNAPI_S3_BUCKET || "orion-vehicles";
console.log(`Reading state from s3://${s3Bucket}`);

function convertTZ(date, tzString) {
  return new Date((typeof date === "string" ? new Date(date) : date).toLocaleString("en-US", {timeZone: tzString}));
}

/*
 * Gets bucket prefix at the hour-level
 * Note to Jesse - I changed the bucket structure
 * when we switch to a new bucket (code for PDX owned)
 * we could switch it back. For now, it's not great
 * but I think it's not the source of the problem.
 * see getVehiclePaths - I think the function is
 * not as fast as it could be but it works
 * @param agencyId - String
 * @param currentTime - Number
 * @return prefix - String
 */
function getBucketHourPrefix(agencyId, currentTime) {
  const currentDateTime = new Date(Number(currentTime * 1000));
  const pacificDateTime = convertTZ(currentDateTime, 'America/Los_Angeles');
  const year = pacificDateTime.getFullYear();
  const month = String(pacificDateTime.getMonth()+1).padStart(2, '0');
  const day = String(pacificDateTime.getUTCDate()).padStart(2, '0');
  const hour = String(pacificDateTime.getUTCHours()).padStart(2, '0');
  // console.log('looking at bucket year %i, month %i, day %i, hour %i', year, month, day, hour);
  return `${agencyId}/${year}/${month}/${day}/${hour}/`;
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
  // Idea: there are 1440 minutes in a day, and the API return at most 1-2 days,
  // so we can iterate every minute (as we have to get each file individually anyways)
  let hourPrefixes = [];
  for (let time = startEpoch; time < endEpoch; time += 60) {
    hourPrefixes.push(getBucketHourPrefix(agencyId, time));
  }
  let uniquehourPrefixes = [...new Set(hourPrefixes)];
  // console.log(uniquehourPrefixes)
  let files = _.flatten(await Promise.all(uniquehourPrefixes.map(prefix => getS3Paths(prefix))));

  let timestampsMap = {};
  let res = [];

  files.map(key => {
     const timestamp = getTimestamp(key);
     if (timestamp >= startEpoch && timestamp < endEpoch && !timestampsMap[timestamp]) {
         timestampsMap[timestamp] = true;
         res.push(key);
     }
  });
  // console.log(res)
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
            resolve(decompressData(data.Body));
          }
        });
      }).catch((err) => {
        return Promise.reject(`Error loading s3://${s3Bucket}/${key}: ${err}`);
      });
  })));
}

function getTimestamp(key) {
    const keyParts = key.split('-');
    const raw_timestamp = Number(keyParts[keyParts.length - 1].split('.json')[0])
    return raw_timestamp;
}

module.exports = {
  getVehicles,
};

