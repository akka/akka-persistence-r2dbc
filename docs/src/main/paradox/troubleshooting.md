# Troubleshooting 

## Failing to use the configured plugin 

If you see something like this, where the driver is not available:

```
 Unable to create a ConnectionFactory for 'ConnectionFactoryOptions{options={...}}'. Available drivers: [ pool ]
```

It is likely because you are building a shaded jar with all dependencies and the ´META-INF/services` entry for 
the R2DBC driver for the database you are using got lost.

If using maven shading plugin, make sure to use the [ServicesResourceTransformer](https://maven.apache.org/plugins/maven-shade-plugin/examples/resource-transformers.html#ServicesResourceTransformer). 

For other build tools, make sure the tool merges `META-INF/services/io.r2dbc.spi.ConnectionFactoryProvider` 
files rather than dropping one when there is a conflict.