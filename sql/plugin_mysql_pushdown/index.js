'use strict';

// Set local caching
const app = require('./src/webserver');
const controller = require('./src/controller');

const configureEndpoint = (method, endpoint, endpointFun) => {
  app[method](endpoint, (request, resolve, next) => {
    endpointFun(request)
      .then((result) => {
        return resolve.status(result.statusCode).json(result.body);
      })
      .catch((error) => {
        console.log('error in index', error);
        if (error.statusCode) {
          return resolve.status(error.statusCode).json(error.body);
        }
        return resolve.status(500).json('Oops something went wrong');
      });
  });
};

configureEndpoint('post', '/authorize', controller.authorize);
configureEndpoint('post', '/datasets', controller.datasets);
configureEndpoint('get', '/status', controller.status);
configureEndpoint('post', '/query', controller.query);
