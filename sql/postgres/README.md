# Introduction

A plugin for the Postgres SQL database that can serve as an example.
The code aims to highlight the steps you have to take when you write a pushdown plugin.

The layout of the src folder tries to highlight the four different concerns:

- **src**
   - **plugin**                     // Everything that is specific to this plugin
        - **database-wrapper.js**    // Wrapper to query your database
        - **query-generator.js**     // Transform the incoming Cumul.io query to a query that can be understood by the target database
        - **results-generator.js**   // Sometimes results of a database are not directly in the required Cumul.io format. This is the place to transform results
        - **type-mapping.js**        // Cumul.io makes it easy and only has three types: hierarchy/numeric/datetime. This is where you map your database types to Cumul.io types.
   - **util**:                       // Everything that will typically not change if you write a different plugin

      - ...


# How to run

```shell
npm install
node index
```         
