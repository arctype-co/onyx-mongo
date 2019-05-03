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
  (let [conn (mongo/make-connection (:mongo/db task-map)
                         :instances (:mongo/instances task-map)
                         :options (:mongo/options task-map)
                         :username (:mongo/username task-map)
                         :password (:mongo/password task-map))]
    (when-let [write-concern (:mongo/write-concern task-map)]
      (mongo/set-write-concern conn write-concern))
    conn))

(defrecord MongoOutput [task-map conn]
  p/Plugin
  (start [this event]
    (as-> this
      (assoc this :conn (connect task-map))))

  (stop [this event]
    (mongo/close-connection conn)
    (dissoc this :conn))

  p/Checkpointed
  ;; Nothing is required here. This is normally useful for checkpointing in
  ;; input plugins.
  (checkpoint [this])

  ;; Nothing is required here. This is normally useful for checkpointing in
  ;; input plugins.
  (recover! [this replica-version checkpoint])

  ;; Nothing is required here. This is normally useful for checkpointing in
  ;; input plugins.
  (checkpointed! [this epoch])

  p/BarrierSynchronization
  (synced? [this epoch]
    ;; Nothing is required here. This is commonly used to check whether all
    ;; async writes have finished.
    true)

  (completed? [this]
    ;; Nothing is required here. This is commonly used to check whether all
    ;; async writes have finished (just like synced).
    true)

  p/Output
  (prepare-batch [this event replica messenger]
    ;; Nothing is required here. This is useful for some initial preparation,
    ;; before write-batch is called repeatedly.
    true)

  (write-batch [this {:keys [onyx.core/write-batch mongo/example-datasink]} replica messenger]
    ;; Write the batch to your datasink.
    ;; In this case we are conjoining elements onto a collection.
    (loop [batch write-batch]
      (if-let [msg (first batch)]
        (do
          (swap! example-datasink conj msg)
          (recur (rest batch)))))
    true))

(def ^:private MongoInstance {:host S/Str (S/optional-key :port) S/Str})

;    :errors-ignored will not report any errors - fire and forget  (:none)
;    :unacknowledged will report network errors - but does not wait for the write to be acknowledged  (:normal - this was the default prior to 0.4.0)
;    :acknowledged will report key constraint and other errors - this is the default  (:safe, :strict was deprecated in 0.1.9)
;    :journaled waits until the primary has sync'd the write to the journal  (:journal-safe)
;    :fsynced waits until a write is sync'd to the filesystem  (:fsync-safe)
;    :replica-acknowledged waits until a write is sync'd to at least one replica as well  (:replicas-safe, :replica-safe)
;    :majority waits until a write is sync'd to a majority of replica nodes  (no previous equivalent)
(def ^:private MongoWriteConcern
  (S/enum :errors-ignored :unacknowledged :acknowledged :journaled :fsynced :replica-acknowledged :majority))

(def MongoOutputTaskMap
  {:mongo/db S/String
   :mongo/instances [MongoInstance]
   (S/optional-key :mongo/options) {S/Keyword S/Any}
   (S/optional-key :mongo/username) S/Str
   (S/optional-key :mongo/password) S/Str
   (S/optional-key :mongo/write-concern) S/Str
   (os/restricted-ns :mongo) S/Any})

;; Builder function for your output plugin.
;; Instantiates a record.
;; It is highly recommended you inject and pre-calculate frequently used data 
;; from your task-map here, in order to improve the performance of your plugin
;; Extending the function below is likely good for most use cases.
(defn output [{:keys [onyx.core/task-map]}]
  (S/validate MongoOutputTaskMap task-map)
  (map->MongoOutput {:task-map task-map}))
