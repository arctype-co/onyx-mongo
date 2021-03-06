## onyx-mongo

Onyx plugin for Mongo DB.

#### Installation

In your project file:

```clojure
[onyx-mongo "0.14.4.0"]
```

In your peer boot-up namespace:

```clojure
(:require [onyx.plugin.mongo])
```

#### Functions

##### sample-entry

Catalog entry:

```clojure
{:onyx/name :entry-name
 :onyx/plugin :onyx.plugin.mongo/input
 :onyx/type :input
 :onyx/medium :mongo
 :onyx/batch-size batch-size
 :onyx/doc "Reads segments from mongo"}
```

Lifecycle entry:

```clojure
[{:lifecycle/task :your-task-name
  :lifecycle/calls :onyx.plugin.mongo/lifecycle-calls}]
```

#### Attributes

|key                           | type      | description
|------------------------------|-----------|------------
|`:mongo/attr`            | `string`  | Description here.

#### Contributing

Pull requests into the master branch are welcomed.

#### License

Copyright © 2018 Arctype Corp.

Distributed under the Eclipse Public License, the same as Clojure.
