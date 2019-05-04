(ns onyx.plugin.mongo
  (:require [onyx.peer.function :as function]
            [onyx.plugin.protocols :as p]
            [onyx.schema :as os]
            [schema.core :as S]
            [somnium.congomongo :as mongo]
            [taoensso.timbre :as log]))

(defn inject-into-eventmap
  [event lifecycle]
  {:mongo/example-datasink (atom (list))})

;; Map of lifecycle calls that are required to use this plugin.
;; Users will generally always have to include these in their lifecycle calls
;; when submitting the job.
(def writer-calls
  {:lifecycle/before-task-start inject-into-eventmap})

(defn- connect
  [task-map]
  (log/debug {:message "Opening mongo connection"
              :db (:mongo/db task-map)})
  (let [conn (mongo/make-connection (:mongo/db task-map)
                         :instances (:mongo/instances task-map)
                         :options (:mongo/options task-map)
                         :username (:mongo/username task-map)
                         :password (:mongo/password task-map))]
    (when-let [write-concern (:mongo/write-concern task-map)]
      (mongo/set-write-concern conn write-concern))
    conn))

(def WriteOperation
  {:op (S/enum :insert :update :remove)
   :collection S/Keyword
   (S/optional-key :query) S/Any
   (S/optional-key :value) S/Any
   (S/optional-key :options) {S/Keyword S/Any}})

(S/defn ^:private write-op
  [{:keys [op collection query value options] :as segment} :- WriteOperation]
  (let [result (case op
                 :insert (apply mongo/insert! collection value (apply concat options))
                 :update (apply mongo/update! collection query value (apply concat options))
                 :remove (apply mongo/destroy! collection query (apply concat options))
                 (throw (ex-info "Undefined MongoDB operation"
                                 segment)))]

    (log/debug (merge {:message "Mongo write operation"
                       :input segment
                       :result result}))))

(defrecord MongoOutput [task-map conn]
  p/Plugin
  (start [this event]
    ; Use lifecycles to connect
    this)

  (stop [this event]
    ; Use lifecycles to disconnect
    this)

  p/Checkpointed
  (checkpoint [this])

  (recover! [this replica-version checkpoint])

  (checkpointed! [this epoch])

  p/BarrierSynchronization
  (synced? [this epoch]
    true)

  (completed? [this]
    true)

  p/Output
  (prepare-batch [this event replica messenger]
    true)

  (write-batch [this {:keys [onyx.plugin.mongo/conn onyx.core/write-batch]} replica messenger]
    (mongo/with-mongo conn
      (loop [batch write-batch]
        (when-let [msg (first batch)]
          (write-op msg)
          (recur (rest batch)))))
    true))

(defn open-connection
  [{:keys [onyx.core/task-map] :as event} lifecycle]
  (assoc event ::conn (connect task-map)))

(defn close-connection
  [{:keys [onyx.core/task-map] :as event} lifecycle]
  (when-let [conn (::conn task-map)]
    (log/debug {:message "Closing mongo connection"
                :db (:mongo/db task-map)})
    (mongo/close-connection conn))
  (dissoc event ::conn))

(def ^:private MongoInstance {:host S/Str (S/optional-key :port) S/Int})

;    :errors-ignored will not report any errors - fire and forget  (:none)
;    :unacknowledged will report network errors - but does not wait for the write to be acknowledged  (:normal - this was the default prior to 0.4.0)
;    :acknowledged will report key constraint and other errors - this is the default  (:safe, :strict was deprecated in 0.1.9)
;    :journaled waits until the primary has sync'd the write to the journal  (:journal-safe)
;    :fsynced waits until a write is sync'd to the filesystem  (:fsync-safe)
;    :replica-acknowledged waits until a write is sync'd to at least one replica as well  (:replicas-safe, :replica-safe)
;    :majority waits until a write is sync'd to a majority of replica nodes  (no previous equivalent)
(def MongoWriteConcern
  (S/enum :errors-ignored :unacknowledged :acknowledged :journaled :fsynced :replica-acknowledged :majority))

(def MongoOutputTaskMap
  {:mongo/db S/Str
   :mongo/instances [MongoInstance]
   (S/optional-key :mongo/options) {S/Keyword S/Any}
   (S/optional-key :mongo/username) S/Str
   (S/optional-key :mongo/password) S/Str
   (S/optional-key :mongo/write-concern) MongoWriteConcern
   (os/restricted-ns :mongo) S/Any})

(def connection-lifecycle
  {:lifecycle/before-task-start open-connection
   ;:lifecycle/handle-exception handle-exception
   :lifecycle/after-task-stop close-connection})

(defn output [{:keys [onyx.core/task-map]}]
  (S/validate MongoOutputTaskMap task-map)
  (map->MongoOutput {:task-map task-map}))
