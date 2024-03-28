# Database Cleanup

## Event Sourced cleanup tool

If possible, it is best to keep all events in an event sourced system. That way new @extref:[Akka Projection R2DBC](akka-projection:r2dbc.html)
can be re-built. A @ref[Projection can also start or continue from a snapshot](./query.md#eventsbyslicesstartingfromsnapshots),
and then events can be deleted before the snapshot. 

In some cases keeping all events is not possible or must be removed for regulatory reasons, such as compliance with
GDPR. `EventSourcedBehavior`s can automatically snapshot state and delete events as described in the
@extref:[Akka docs](akka:typed/persistence-snapshot.html#snapshot-deletion). Snapshotting is useful even if events
aren't deleted as it speeds up recovery.

Deleting all events immediately when an entity has reached its terminal deleted state would have the consequence that
Projections might not have consumed all previous events yet and will not be notified of the deleted event. Instead, it's
recommended to emit a final deleted event and store the fact that the entity is deleted separately via a Projection.
Then a background task can clean up the events and snapshots for the deleted entities by using the
@apidoc[EventSourcedCleanup] tool. The entity itself knows about the terminal state from the deleted event and should
not emit further events after that and typically stop itself if it receives more commands.

@apidoc[EventSourcedCleanup] operations include:

* Delete all events and snapshots for one or many persistence ids
* Delete all events for one or many persistence ids
* Delete all snapshots for one or many persistence ids
* Delete events before snapshot for one or many persistence ids
* Delete events before a timestamp

@@@ warning

When running an operation with `EventSourcedCleanup` that deletes all events for a persistence id,
the actor with that persistence id must not be running! If the actor is restarted it would in that
case be recovered to the wrong state since the stored events have been deleted. Delete events before
snapshot can still be used while the actor is running.

@@@

The cleanup tool can be combined with the @ref[query plugin](./query.md) which has a query to get all persistence ids.

Java
: @@snip [cleanup](/docs/src/test/java/jdocs/home/cleanup/CleanupDocExample.java) { #cleanup }

Scala
: @@snip [cleanup](/docs/src/test/scala/docs/home/cleanup/CleanupDocExample.scala) { #cleanup }

## Durable State cleanup tool

@apidoc[DurableStateCleanup] operations include:

* Delete state for one or many persistence ids

@@@ warning

When running any operation with `DurableStateCleanup` for a persistence id, the actor with that persistence id must
not be running! If the actor is restarted it would in that case be recovered to the wrong state since the stored state
hase been deleted.

@@@

The cleanup tool can be combined with the @ref[query plugin](./query.md) which has a query to get all persistence ids.
