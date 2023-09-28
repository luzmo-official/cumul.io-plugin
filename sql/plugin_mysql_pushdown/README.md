# plugin_mysql_pushdown

This is a template pushdown plugin for MySQL using Node.js with key & token (username & password MySQL DB). This template can be used as a starting point for the development of a Cumul.io plugin.
It includes a webserver to host the plugin.

Oct 2023 **_WARNING_** This plugin has been updated to allow upper and lower case column names. This is a breaking change and will cause existing Cumul.io datasets to no longer query correctly.

The layout of the src folder tries to highlight the four different concerns:

- **src**

  - **plugin** // Everything that is specific to this plugin
    - **database-wrapper.js** // Wrapper to query your database
    - **query-generator.js** // Transform the incoming Cumul.io query to a query that can be understood by the target database
    - **results-generator.js** // Sometimes results of a database are not directly in the required Cumul.io format. This is the place to transform results
    - **type-mapping.js** // Cumul.io makes it easy and only has three types: hierarchy/numeric/datetime. This is where you map your database types to Cumul.io types.
  - **util**: // Everything that will typically not change if you write a different plugin

    - ...

## Initial setup

```
npm install
```

```
node index.js
```

Use ngrok, heroku or similar to obtain a https base url. Register your plugin in Cumul.io using this base url, make sure to end the base url without trailing '/' character.

Fill in your MYSQL_HOST & DATABASE in .env file.

Add your .pem files for SSL connection in a /keys folder.

Register your plugin in Cumul.io with authentication "key/token". Use the key for your database user & token for the database password.

## Security through plugin secret validation

To enable the validation of the plugin secret, fill in your plugin secret in the .env file and uncomment the "Verify Cumul.io plugin secret" blocks in index.js.
