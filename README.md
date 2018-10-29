# Logstash Plugin

This is a plugin for [Logstash](https://github.com/elasticsearch/logstash).

It is fully free and fully open source. The license is Apache 2.0, meaning you are pretty much free to use it however you want in whatever way.

It was initiall forked from [phutchins](https://github.com/phutchins/logstash-input-mongodb)'s repository, and 
tailored to my needs. That is why it needs an date window for document retrieval.

## Documentation

This is a logstash plugin for pulling data out of mongodb and processing with logstash. It will connect to the database specified in `uri`, use the `collection` attribute to find collections to pull documents from, start at the first collection it finds and pull the number of documents specified in `batch_size`, save it's progress in an sqlite database who's location is specified by `last_run_storage_path` 
and repeat. It will continue this until it no longer finds documents newer than ones that it has processed, sleep for a moment, then continue to loop over the collections.

This was designed for parsing logs that were written into mongodb. This means that it may not re-parse db entries that were changed and already parsed.


### Installation

+ Logstash installed from ZIP | TGZ
  + bin/plugin install /path/to/logstash-input-mongodb-0.4.2.gem

+ Logstash from GIT
  + git clone https://github.com/elastic/logstash.git
  + cd logstash
  + (ensure that the
   correct jruby is installed for the version of logstash you are installing)
  + rake test:install-core
  + bin/plugin install /path/to/logstash-input-mongodb-0.4.2.gem
  + bin/plugin install --development

### Configuration Options

```
Name                  Type          Description
uri                   [String]      A MongoDB URI for your database or cluster (check the MongoDB documentation for further info on this) [No Default, Required]
last_run_storage_path [String]      Complete path+filename where the place holder database will be stored locally to disk [No Default, Required]
                                    The file gets created by the plugin so the directory needs to be writeable by the user that logstash is running as
collection            [String]      A regex that will be used to find desired collecitons. [No Default, Required]
batch_size            [Int]         Size of the batch of mongo documents to pull at a time [Default: 30]
sort_on               [String]      Field name to sort documents. Documents will be returned with this field increasing. [Required]
                                    Constraint: this field must contain isodates.
start_window          [String]      Retrieve only docs with field "sort_on" posterior to this date [Required]
end_window            [String]      Retrieve only docs with field "sort_on" anterior to this date [Required]
query                 [String]      A query to retrieve only documents that match this query. 
                                    Must be a Json parseable structure like in Mongo Shell [Required]
projection            [String]      Json hash of fields to retrieve, in case we do not want all fields returned.
                                    Must be a Json parseable structure like in Mongo Shell [Default nil: retrieve all fields]
parse_method          [String]      Built in parsing of the mongodb document object [Default: 'flatten']
                                    Only tested method is 'flatten'
dig_fields            [Array]       An array of fields that should employ the dig method [Untested]
dig_dig_fields        [Array]       This provides a second level of hash flattening after the initial dig has been done [Untested]
```


### Configuration

Example
```
input {
  mongodb {
    uri => 'mongodb://10.0.0.30/my-logs?ssl=true'
    last_run_storage_path => '/opt/logstash-mongodb/logstash_sqlite.db'
    collection => 'events_'
    batch_size => 5000
    query => ' { "$or" [ {"myField" : "target1"}, { "myField" : "target2"}] } ' 
    projection => ' { "_id" : 1} '
  }
}

```

### MongoDB URI

The URI parameter is where you would specify all of your mongodb options including things like auth and SSL. You should use a connection string (URI) compatible with the mongodb spec.

For more information on MongoDB URI's please see the MongoDB documentation: https://docs.mongodb.org/v3.0/reference/connection-string/

### General Mongo Shell query syntax

For more information on the query syntax in Mongo Shell: https://docs.mongodb.com/manual/tutorial/query-documents/

For more information on the projection operator: https://docs.mongodb.com/manual/reference/method/db.collection.find/#find-projection
