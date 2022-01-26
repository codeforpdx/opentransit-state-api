const s3Helper = require('./helpers/s3Helper.js');
const removeMuniMetroDuplicates = require('./helpers/removeMuniMetroDuplicates');
const BigInt = require('./bigint');

const _ = require('lodash');

const debug = !!process.env.DEBUG;

const resolvers = {

    // use BigInt to represent Unix timestamps because GraphQL Int type is only 32-bit
    // and would overflow in January 2038 https://github.com/graphql/graphql-js/issues/292
    BigInt: BigInt,

    Query: {
        state: async (obj, params) => {
            const { agencyId, routes } = params;

            let { startTime, endTime } = params;

            console.log(agencyId, routes)

            const resultSets = await s3Helper.getVehicles(agencyId, startTime, endTime);


            const vehiclesByTripByTime = {};
            const vehiclesByRouteByTime = {};
            const UniqueVehicleTimeKeys = {};

            // group the vehicles by route, and then by time
            // below are fields in the vehicle api response
            // 'serviceDate', 'latitude', 'nextStopSeq', 'type', 'blockID',
//         'signMessageLong', 'lastLocID', 'nextLocID', 'locationInScheduleDay',
//         'longitude', 'direction', 'routeNumber', 'bearing', 'garage', 'tripID',
//         'delay', 'lastStopSeq', 'vehicleID', 'time'

            

                resultSets.forEach(results => {

                    const queryTime = results.resultSet.queryTime;

                    if (!(results.resultSet['vehicle']===undefined)) {

                    results.resultSet.vehicle.forEach(vehicle => {

                        // console.log(vehicle)
                        const routeId = vehicle.routeNumber;
                        const vtime = queryTime;
                        const vehicleID = vehicle.vehicleID;
                        const tempVehicleTime = vehicleID+'_'+vtime;

                        const secsSinceReport = Math.floor((Number(vtime)-Number(vehicle.time))/1000);

                        vehicle.secsSinceReport = secsSinceReport;

                        if (!vehiclesByRouteByTime[routeId]) {
                            vehiclesByRouteByTime[routeId] = {};
                        }
                        if (!vehiclesByRouteByTime[routeId][vtime]) {
                            vehiclesByRouteByTime[routeId][vtime] = [];
                        }

                        if (!UniqueVehicleTimeKeys[tempVehicleTime]) {
                            vehiclesByRouteByTime[tempVehicleTime] = [];
                            vehiclesByRouteByTime[routeId][vtime].push(vehicle);
                        }

                    });

                    


                    
                    
                    
                }
                
            });

            // get all the routes
            const routeIDs = routes ?
                _.intersection(routes, Object.keys(vehiclesByRouteByTime)) :
                Object.keys(vehiclesByRouteByTime);


            return {
                agencyId,
                routeIDs,
                startTime,
                endTime,
                vehiclesByRouteByTime
            };
        
        },
    },

    AgencyState: {
        agencyId: obj => obj.agencyId,
        startTime: obj => obj.startTime,
        endTime: obj => obj.endTime,
        routes: obj => {
            return obj.routeIDs.map((rid) => {
                return {id: rid, agencyId: obj.agencyId, vehiclesByTime: obj.vehiclesByRouteByTime[rid]};
            });
        }
    },

    RouteHistory: {
        routeId: route => route.id,
        states: route => {
            const vehiclesByTime = route.vehiclesByTime || {};
            return Object.keys(vehiclesByTime).map((timestamp) => ({
                timestamp: timestamp,
                vehicles: vehiclesByTime[timestamp],
            }));
        }
    },

            // list all available fields
            // 'serviceDate', 'latitude', 'nextStopSeq', 'type', 'blockID',
//         'signMessageLong', 'lastLocID', 'nextLocID', 'locationInScheduleDay',
//         'longitude', 'direction', 'routeNumber', 'bearing', 'garage', 'tripID',
//         'delay', 'lastStopSeq', 'vehicleID', 'time'

    VehicleState: {
        vehicleID: vehicle => vehicle.vehicleID,
        direction: vehicle => vehicle.direction,
        latitude: vehicle => vehicle.latitude,
        longitude: vehicle => vehicle.longitude,
        bearing: vehicle => vehicle.bearing,
        tripID: vehicle => vehicle.tripID,
        nextStopSeq: vehicle => vehicle.nextStopSeq,
        lastStopSeq: vehicle => vehicle.lastStopSeq,
        time: vehicle => vehicle.time,
        secsSinceReport: vehicle => vehicle.secsSinceReport,
    }
};

module.exports = resolvers;
