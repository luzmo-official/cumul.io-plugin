const app = require( './src/webserver' );
const controller = require( './src/controller' );
const errors = require('./src/util/errors');

const configureEndpoint = ( method, endpoint, endpointFun ) => {
  app[method]( endpoint, ( request, resolve, next ) => {
    endpointFun( request )
      .then(( result ) => {
        return resolve.status( result.statusCode ).json( result.body );
      })
      .catch( error => {
        console.log( 'error in index', error );
        if ( error.type && error.type.code )
          return resolve.status( error.type.code ).json( error );

        return resolve.status( 500 ).json( errors.unexpectedError() );
      });
  });
};

const configureStreamingEndpoint = ( method, endpoint, endpointFun ) => {
  app[method]( endpoint, ( request, resolve, next ) => {
    endpointFun( request, resolve )
      .then(( statusAndStream ) => {
        if (statusAndStream.statusCode !== 200)
          return resolve.status( statusAndStream.statusCode ).json( statusAndStream.body );
        else {
          const stream = statusAndStream.stream;
          stream.pipe(resolve);
        }
      })
      .catch( error => {
        console.log( 'error in index', error );
        if ( error.type && error.type.code && !error.headers)
          return resolve.status( error.type.code ).json( error );

        return resolve.status( 500 ).json( errors.unexpectedError() );
      });
  });
};

configureEndpoint( 'post', '/authorize', controller.authorize );
configureEndpoint( 'get', '/datasets', controller.datasets );
configureEndpoint( 'get', '/status', controller.status );
configureStreamingEndpoint( 'post', '/query', controller.query );
