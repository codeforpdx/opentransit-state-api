# tryn-api

API for historical transit vehicle location data.

tryn-api provides a GraphQL API for the data previously stored in S3 by Orion (https://github.com/trynmaps/orion).

## Getting Started

See our welcome doc for contribution and deployment guidelines.
https://bit.ly/opentransit-onboarding

1. Clone this repo.

2. Create a docker-compose.override.yml file that sets the environment variable TRYNAPI_S3_BUCKET to the name of the S3 bucket where data was stored by Orion,
and provides AWS credentials with read access to that bucket. tryn-api reads data from S3 using the AWS credentials from the default locations,
e.g. a credentials file located within the Docker container at /root/.aws/credentials (using the default profile or a profile named by AWS_PROFILE),
or using the environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY. For example:

```
version: "3.7"
services:
  tryn-api-dev:
    volumes:
      - ../.aws:/root/.aws
    environment:
      AWS_PROFILE: "default"
      TRYNAPI_S3_BUCKET: "my-opentransit-bucket"
```

2. Run `docker-compose up`

The GraphQL API will be available at http://localhost:4000/graphql .

## API Documentation

### Query

The root query object for the API.

| Field Name | Type | Description |
| --- | --- | --- |
| `state` | [`[AgencyState]`](#agencystate) | Returns historical state of vehicles for a particular transit agency. |

#### Parameters for `state`

| Parameter Name | Type | Description |
| --- | --- | --- |
| `agency` | `String!` | ID of the agency. The agency ID should be the same ID used in the configuration for Orion. |
| `startTime` | `BigInt!` | Start timestamp in seconds since the Unix epoch. |
| `endTime` | `BigInt!` | End timestamp in seconds since the Unix epoch , exclusive. |
| `routes` | `[String!]` | List of route IDs to return vehicle data. |

### AgencyState

| Field Name | Type | Description |
| --- | --- | --- |
| `agencyId` | `String` | ID of the agency. The agency ID should be the same ID used in the configuration for Orion. |
| `startTime` | `BigInt` | Start timestamp in seconds since the Unix epoch. |
| `endTime` | `BigInt` | End timestamp in seconds since the Unix epoch, exclusive. |
| `routes` | [`[RouteHistory]`](#routehistory) | Array of historical state for each route. |

### RouteHistory

| Field Name | Type | Description |
| --- | --- | --- |
| `routeId` | `String` | ID of the route. |
| `states` | [`[RouteState]`](#routestate) | Array of historical data for this route. |

### RouteState

State of a particular route at a particular time.

| Field Name | Type | Description |
| --- | --- | --- |
| `timestamp` | `BigInt` | Timestamp when vehicle data was retrieved, in seconds since the Unix epoch. |
| `vehicles` | [`[VehicleState]`](#vehiclestate) | Array of vehicles observed at this timestamp. |

### VehicleState

State of a particular vehicle at a particular time. Some fields may be null if not provided by the data source.

| Field Name | Type | Description |
| --- | --- | --- |
| `vehicleID` | `String` | ID of this vehicle. |
| `direction` | `String` | ID of the direction the vehicle reported it was going. |
| `latitude` | `Float` | Reported latitude of vehicle. |
| `longitude` | `Float` | Reported longitude of vehicle. |
| `bearing` | `Float` | Reported heading of vehicle in degrees. |
| `secsSinceReport` | `Int` | Number of seconds old this observation was when it was retrieved (at `timestamp` of `RouteState`). |
| `tripID` | `String` | ID of the trip the vehicle reported it was running (GTFS-realtime providers only) |

## Sample Query

Once you run it, go to http://localhost:4000/graphql in your browser and run this query:

```
query {
  state(agencyId: "trimet"
    , startTime: 1642867201
    , endTime: 1642867500
    , routes: ["100"]) {
    agencyId
    startTime
    routes {
      routeId
      states {
        timestamp
        vehicles {
          vehicleID
          tripID
          latitude
          longitude
          direction
          bearing
          time
          secsSinceReport
        }
      }
    }
  }
}
```

Substitute startTime and endTime with epoch timestamps (in seconds) corresponding to when Orion was actually running.
