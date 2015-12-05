(ns onyx-etl-support.catalogs.datomic-catalog)

(defn datomic-output-entries
  [db-uri db-partition key-map transform-batch-size output-batch-size]
  [{:onyx/name :prepare-datoms
    :onyx/fn :onyx-etl-support.functions.transformers/prepare-datoms
    :onyx/type :function
    :datomic/partition db-partition
    :datomic/key-map key-map
    :onyx/params [:datomic/partition :datomic/key-map]
    :onyx/batch-size transform-batch-size
    :onyx/doc "Semantically transform the SQL rows to Datomic datoms"}

   {:onyx/name :write-to-datomic
    :onyx/plugin :onyx.plugin.datomic/write-bulk-datoms
    :onyx/type :output
    :onyx/medium :datomic
    :datomic/uri db-uri
    :datomic/partition db-partition
    :onyx/batch-size output-batch-size
    :onyx/doc "Transacts segments to storage"}])
