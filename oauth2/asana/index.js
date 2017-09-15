'use strict';

var Asana = require('asana');
var app = require('./webserver')();

// Create Asana client
const asana = Asana.Client.create({
  clientId: process.env.ASANA_CLIENT_ID,
  clientSecret: process.env.ASANA_CLIENT_SECRET,
  redirectUri: 'https://app.cumul.io/start/connect'
});

// Set local caching
var cache = {cached_at: null, cache: null, interval: 15 * 60 * 1000};

// 1. Retrieve an OAuth2 authorization URL to which the user will be redirected
app.post('/authorize', function(req, res) {
  if (req.headers['x-secret'] !== process.env.CUMULIO_SECRET)
    return res.status(403).end('Given plugin secret does not match Cumul.io plugin secret.');
  return res.status(200).json({auth_url: asana.app.asanaAuthorizeUrl()});
});

// 2. Exchange an 'authorization grant' code for an access token & refresh token
app.post('/exchange', function(req, res) {
  if (req.headers['x-secret'] !== process.env.CUMULIO_SECRET)
    return res.status(403).end('Given plugin secret does not match Cumul.io plugin secret.');
  return asana.app.accessTokenFromCode(req.body.code)
    .then(function(credentials) {
      return res.status(200).json(credentials);
    })
    .catch(function(error) {
      console.error(error);
      return res.status(500).end('Internal Server Error');
    });
});

const COLUMNS = [
  {id: 'id', name: {en: 'ID'}, type: 'hierarchy'},
  {id: 'assignee', name: {en: 'Assignee'}, type: 'hierarchy'},
  {id: 'assignee_status', name: {en: 'Assignee status'}, type: 'hierarchy'},
  {id: 'created_at', name: {en: 'Created at'}, type: 'datetime'},
  {id: 'completed', name: {en: 'Completed'}, type: 'hierarchy'},
  {id: 'completed_at', name: {en: 'Completed at'}, type: 'datetime'},
  {id: 'due_at', name: {en: 'Due at'}, type: 'datetime'},
  {id: 'modified_at', name: {en: 'Modified at'}, type: 'datetime'},
  {id: 'name', name: {en: 'Name'}, type: 'hierarchy'},
  {id: 'num_hearts', name: {en: 'Hearts'}, type: 'numeric'},
  {id: 'workspace', name: {en: 'Workspace'}, type: 'hierarchy'},
  {id: 'project', name: {en: 'Project'}, type: 'hierarchy'},
  {id: 'parent', name: {en: 'Parent'}, type: 'hierarchy'}
];

// 3. List datasets
app.get('/datasets', function(req, res) {
  if (req.headers['x-secret'] !== process.env.CUMULIO_SECRET)
    return res.status(403).end('Given plugin secret does not match Cumul.io plugin secret.');
  
  var currentClient = asana.useOauth({credentials: {refresh_token: req.headers['x-token']}});
  return res.status(200).json([{
    id: 'tasks',
    name: {en: 'Tasks'},
    description: {en: 'All open & closed tasks tracked in your Asana account'},
    columns: COLUMNS
  }]);
});

// 4. Query
app.post('/query', function(req, res) {
  if (req.headers['x-secret'] !== process.env.CUMULIO_SECRET)
    return res.status(403).end('Given plugin secret does not match Cumul.io plugin secret.');

  var currentClient = asana.useOauth({credentials: {refresh_token: req.headers['x-token']}});

  // Asana does not give a clean Promise reject on invalid credentials, so we catch it here
  process.on("unhandledRejection", function(reason, promise) {
    return res.status(500).end('Internal Server Error');
  });

  if (cache.cached_at > (new Date()).getTime() - cache.interval)
    return res.status(200).json(cache.cache);
  
  cache.cache = [];
  return currentClient.workspaces.findAll()
    .then(function(collection) {
      return collection.fetch();
    })
    .map(function(workspace) {
      return asana.projects.findAll({workspace: workspace.id})
        .then(function(collection) {
          return collection.fetch();
        })
        .map(function(project) {
          return asana.tasks.findAll({
            project: project.id,
            opt_fields: 'id,assignee,assignee.name,assignee_status,created_at,completed,completed_at,due_at,modified_at,name,num_hearts,parent'
          })
            .then(function(collection) {
              return collection.fetch();
            })
            .map(function(task) {
              task.assignee = task.assignee ? task.assignee.name : null;
              task.parent = task.parent ? task.parent.id : null;
              task.workspace = workspace.name;
              task.project = project.name;
              cache.cache.push(COLUMNS.map((column) => task[column.id]));
            });
        });
    })
    .then(function() {
      cache.cached_at = (new Date()).getTime();
      return res.status(200).json(cache.cache);
    })
    .catch(function(error) {
      return res.status(500).end('Internal Server Error');
    });
});