
const errors = require('./errors');

// Validation is not strictly necessary since Cumul.io
// will not send you illegal queries. While testing it might however indicate very quickly
// that there is a mistake in the 'test query'.
// Besides of that it is helpful to have an idea what syntax exists.

const LEVEL = [
  'year',
  'quarter',
  'month',
  'week',
  'day',
  'hour',
  'minute',
  'second',
  'millisecond'
];
const AGGREGATION = ['sum', 'min', 'max', 'count'];
const FILTER = ['<', '>', '=', 'in', 'not in', '<=', '>=', 'is null', 'is not null'];

class Validation {

  isValidDateLevel(level) {
    const res = LEVEL.includes(level);
    if (!res)
      console.log('Invalid level: ', level);

    return res;
  }

  isValidAggregation(aggregation) {
    const res = AGGREGATION.includes(aggregation);
    if (!res)
      console.log('Invalid aggregation: ', aggregation);

    return res;
  }

  isValidFilter(filter) {
    const res = FILTER.includes(filter);
    if (!res)
      console.log('Invalid filter: ', filter);

    return res;
  }

  validateSecret(secret) {
    if (secret !== process.env.CUMULIO_SECRET && !process.env.LOCAL )
      throw errors.unauthorizedError('The plugin secret is invalid.');
  }

}

module.exports = new Validation();
