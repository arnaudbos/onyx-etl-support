(ns onyx-etl-support.lifecycles.datomic-lifecycles)

(defn datomic-bulk-writer-entries []
  [{:lifecycle/task :write-to-datomic
    :lifecycle/calls :onyx.plugin.datomic/write-bulk-tx-calls}])
