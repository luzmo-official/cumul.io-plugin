
const bodyParser = require( 'body-parser' )
const compression = require( 'compression' )
const express = require( 'express' )

const PORT = 3030
// Configure webserver
const app = express()
app.set( 'json spaces', 2 )
app.set( 'x-powered-by', false )
app.use( compression())
app.use(( req, res, next ) => {
  res.setHeader( 'Content-Type', 'application/json' )
  res.setHeader( 'Content-Language', 'en' )
  res.setHeader( 'Access-Control-Allow-Origin', '*' )
  res.setHeader( 'Access-Control-Allow-Methods', 'GET, POST, OPTIONS' )
  res.setHeader( 'Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Content-Language, Accept' )
  next()
})
app.use( bodyParser.json())
app.options( '*', ( req, res ) => {
  res.status( 204 )
})

app.listen( PORT, () => console.log( `[OK] Cumul.io plugin 'SQLServer' listening on port ${PORT}` ))

module.exports = app
