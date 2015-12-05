(ns onyx-etl-support.catalogs.sql-catalog)

(defn sql-input-entries
  [{:keys [classname subprotocol subname user password] :as jdbc-spec}
   table id-column rows-per-segment batch-size]
  [{:onyx/name :partition-keys
    :onyx/plugin :onyx.plugin.sql/partition-keys
    :onyx/type :input
    :onyx/medium :sql
    :sql/classname classname
    :sql/subprotocol subprotocol
    :sql/subname subname
    :sql/user user
    :sql/password password
    :sql/table table
    :sql/id id-column
    :sql/rows-per-segment rows-per-segment
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Partitions a range of primary keys into subranges"}

   {:onyx/name :read-rows
    :onyx/fn :onyx.plugin.sql/read-rows
    :onyx/type :function
    :sql/classname classname
    :sql/subprotocol subprotocol
    :sql/subname subname
    :sql/user user
    :sql/password password
    :sql/table table
    :sql/id id-column
    :onyx/batch-size batch-size
    :onyx/doc "Reads rows of a SQL table bounded by a key range"}])
