const {
  pipe, map, slice, join,
} = require('lodash/fp');
const { encode } = require('ngeohash');

class GeoDataManager {
  constructor(DynamoDB, TableName, GSI) {
    this.DynamoDB = DynamoDB;
    this.TableName = TableName;
    this.GSI = GSI;
  }

  putPoint(item) {
    const itemInput = item;

    const geohash = encode(item.latitude, item.longitude, 12);

    const hashKey = pipe(
      slice(0, 3),
      join(''),
    )(geohash);

    itemInput.geohash = geohash;
    itemInput.hashKey = hashKey;
    itemInput.geoJson = `{ type: 'POINT', coordinates: [${item.latitude}, ${item.longitude}] }`;

    const params = {
      TableName: this.TableName,
      Item: itemInput,
    };

    return this.DynamoDB.put(params).promise();
  }

  batchPutPoints(items) {
    const toBatchItem = item => ({
      PutRequest: {
        Item: item,
      },
    });

    const batchPoints = pipe(
      map(toBatchItem),
    )(items);

    const params = {
      RequestItems: {
        'parking-api-dev': batchPoints,
      },
    };

    return this.DynamoDB.batchWrite(params).promise();
  }

  getNearest(coordinates, distanceInMeters) {
    const minGeohash = encode((coordinates.latitude - (distanceInMeters / 111000)), (coordinates.longitude - (distanceInMeters / 111000)), 12);
    const maxGeohash = encode((coordinates.latitude + (distanceInMeters / 111000)), (coordinates.longitude + (distanceInMeters / 111000)), 12);

    const hashKey = pipe(
      slice(0, 3),
      join(''),
    )(maxGeohash);

    const params = {
      TableName: this.TableName,
      IndexName: 'geohash-index',
      KeyConditionExpression: '#hashKey = :hashKey and #geohash between :minGeohash and :maxGeohash',
      ExpressionAttributeNames: {
        '#hashKey': 'hashKey',
        '#geohash': 'geohash',
      },
      ExpressionAttributeValues: {
        ':hashKey': hashKey,
        ':minGeohash': minGeohash,
        ':maxGeohash': maxGeohash,
      },
    };

    this.GSI
      ? params.IndexName = this.GSI
      : params.TableName = this.TableName;

    return this.DynamoDB.query(params).promise();
  }
}

module.exports = {
  GeoDataManager,
};
