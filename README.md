# cumul.io-plugin

Cumul.io Plugin reference implementations for:

* Citybik.es - bike sharing visualization
* MongoDB - NoSQL data store connection
* Asana - web service integration via OAuth2

The actual plugin implementation can be found in the `index.js` file. A small webserver with some boilerplate configuration is included in `webserver.js`.

To run a plugin, register it via Cumul.io > Profile > Plugins and copy the *plugin secret* to the .env environment variable file.
