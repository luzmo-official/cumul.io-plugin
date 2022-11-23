# Introduction

A pushdown-enabled plugin for the PostgreSQL database that can serve as an example. The code aims to highlight the steps you have to take when you write a pushdown plugin.

For more information about plugins or the difference between a pushdown-enabled and a basic plugin: 
https://developer.cumul.io/#plugin-api.

 This code adheres to [the Cumul.io plugin API specification 2.0.0](https://developer.cumul.io/#version-history).

# src folder layout

- **plugin**                     // Everything that is specific to this plugin
     - **database-wrapper.js**    // Wrapper to query your database
     - **query-generator.js**     // Transform the incoming Cumul.io query to a query that can be understood by the target database
     - **results-generator.js**   // Sometimes results of a database are not directly in the required Cumul.io format. This is the place to transform results
     - **type-mapping.js**        // Cumul.io makes it easy and only has three types: hierarchy/numeric/datetime. This is where you map your database types to Cumul.io types.
- **util**:                       // Everything that will typically not change if you write a different plugin

   - ...


# Define custom plugin authentication properties
The following plugin authentication properties should be defined when adding your plugin to Cumul.io (described here: https://developer.cumul.io/#registering-a-plugin):
* host: expects a value with a structure `host:port/databaseName`, e.g. `localhost:443/DemoDB`.
* key: expects the username of the read-only user created in PostgreSQL.
* token: expects the password of the read-only user created in PostgreSQL.

You can set up other/additional custom properties, these properties will be sent to the plugin's endpoints as 'X-Property-<property_name>' header values.


# How to run

Install the libraries
```shell
npm install
```

Once you have added your plugin to Cumul.io, export the secret as an environment variable.

```shell
CUMULIO_SECRET=plugin_secret
```

Run the plugin
```shell
node index
```         
