# Building Native Images

Building native images with Akka Persistence R2DBC is supported out of the box for the event sourced journal, snapshot store and 
durable state store with the following databases:

* H2 (inmem and file)
* Postgres

Other databases can likely be used but will require figuring out and adding additional native-image metadata.

For details about building native images with Akka in general, see the @extref[Akka Documentation](akka:additional/native-image.html).