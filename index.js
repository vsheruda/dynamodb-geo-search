const {
  pipe, slice, join, assign,
} = require('lodash/fp');
const { encode } = require('ngeohash');
const { METERS_IN_DEGREE, GEOHASH_LENGTH, HASHKEY_LENGTH } = require('./utils/constants.utils');

class GeoDataManager {
  constructor(TableName, GSI) {
    this.TableName = TableName;
    this.GSI = GSI;
  }

  static toGeoItem(item) {
    const geohash = encode(item.latitude, item.longitude, GEOHASH_LENGTH);

    const hashKey = pipe(
      slice(0, HASHKEY_LENGTH),
      join(''),
    )(geohash);

    const GeoItem = assign({}, item);

    GeoItem.geohash = geohash;
    GeoItem.hashKey = hashKey;
    GeoItem.geoJson = `{ type: 'POINT', coordinates: [${item.latitude}, ${item.longitude}] }`;

    return GeoItem;
  }

  toGetNearestQuery({ latitude, longitude }, distanceInMeters) {
    const minGeohash = encode((latitude - (distanceInMeters / METERS_IN_DEGREE)),
      (longitude - (distanceInMeters / METERS_IN_DEGREE)), GEOHASH_LENGTH);

    const maxGeohash = encode((latitude + (distanceInMeters / METERS_IN_DEGREE)),
      (longitude + (distanceInMeters / METERS_IN_DEGREE)), GEOHASH_LENGTH);

    const hashKey = pipe(
      slice(0, HASHKEY_LENGTH),
      join(''),
    )(maxGeohash);

    const params = {
      TableName: this.TableName,
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

    return params;
  }
}

module.exports = {
  GeoDataManager,
};
