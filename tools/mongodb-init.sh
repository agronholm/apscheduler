#!/bin/bash

# Wait for MongoDB to start up.
until mongosh --quiet --eval "db.adminCommand('ping')" > /dev/null 2>&1; do
    sleep 1
done

# Initializes the replica set.
mongosh --eval "
rs.initiate({
    _id: 'rs0',
    members: [
        { _id: 0, host: 'localhost:27017' }
    ]
});
"
