(ns onyx-etl-support.core
  (:require [clojure.tools.cli :refer [parse-opts]]
            [onyx-etl-support.lifecycles.datomic-lifecycles :as dl]
            [onyx-etl-support.lifecycles.sql-lifecycles :as sl]
            [onyx-etl-support.functions.transformers :as t]
            [onyx-etl-support.catalogs.datomic-catalog :as dc]
            [onyx-etl-support.catalogs.sql-catalog :as sc]
            [onyx-etl-support.workflows.sql-to-datomic :as sd]
            [onyx.plugin.datomic]
            [onyx.plugin.sql]))

(def default-batch-size 20)

(def default-sql-rows-per-segment 50)

(defmulti build-workflow
  (fn [from to]
    [from to]))

(defmulti find-input-lifecycles
  (fn [medium]
    medium))

(defmulti find-output-lifecycles
  (fn [medium]
    medium))

(defmulti find-input-catalog-entries
  (fn [medium opts]
    medium))

(defmulti find-output-catalog-entries
  (fn [medium opts]
    medium))

(defmethod build-workflow [:sql :datomic]
  [from to]
  sd/sql-to-datomic-workflow)

(defmethod find-input-catalog-entries :sql
  [medium opts]
  (sc/sql-input-entries
   {:classname (:sql-classname opts)
    :subprotocol (:sql-subprotocol opts)
    :subname (:sql-subname opts)
    :user (:sql-user opts)
    :password (:sql-password opts)}
   (:sql-table opts)
   (:sql-id-column opts)
   (:sql-rows-per-segment opts)
   (:input-batch-size opts)))

(defmethod find-output-catalog-entries :datomic
  [medium opts]
  (when-let [kf (:datomic-key-file opts)]
    (when-let [key-map (read-string (slurp kf))]
      (dc/datomic-output-entries
       (:datomic-uri opts)
       (:datomic-partition opts)
       key-map
       (:transform-batch-size opts)
       (:output-batch-size opts)))))

(defmethod find-input-lifecycles :sql
  [medium]
  (sl/sql-reader-entries))

(defmethod find-output-lifecycles :datomic
  [medium]
  (dl/datomic-bulk-writer-entries))

(defmethod build-workflow :default
  [from to])

(defmethod find-input-lifecycles :default
  [medium])

(defmethod find-output-lifecycles :default
  [medium])

(defmethod find-input-catalog-entries :default
  [medium opts])

(defmethod find-output-catalog-entries :default
  [medium opts])

(def cli-options
  [["-f" "--from <medium>" "Input storage medium"
    :missing "--from is a required parameter, it was missing or incorrect"
    :parse-fn #(keyword %)
    :validate [#(some #{%} #{:sql}) "Must be one of #{sql}"]]

   ["-t" "--to <medium>" "Output storage medium"
    :missing "--to is a required parameter, it was missing or incorrect"
    :parse-fn #(keyword %)
    :validate [#(some #{%} #{:datomic}) "Must be one of #{datomic}"]]

   [nil "--input-batch-size <n>" "Batch size of the input task"
    :parse-fn #(Integer/parseInt %)
    :default default-batch-size
    :validate [pos? "Must be a positive integer"]]

   [nil "--transform-batch-size <n>" "Batch size of the transformation task"
    :parse-fn #(Integer/parseInt %)
    :default default-batch-size
    :validate [pos? "Must be a positive integer"]]

   [nil "--output-batch-size <n>" "Batch size of the output task"
    :parse-fn #(Integer/parseInt %)
    :default default-batch-size
    :validate [pos? "Must be a positive integer"]]

   [nil "--datomic-uri <uri>" "Datomic URI"]
   [nil "--datomic-partition <part>" "Datomic partition to use"
    :parse-fn #(keyword %)]
   [nil "--datomic-key-file <file>" "Absolute or relative path to a Datomic transformation file. See the project README for the spec."]

   [nil "--sql-classname <JDBC classname>" "The SQL JDBC spec classname"]
   [nil "--sql-subprotocol <JDBC subprotocol>" "The SQL JDBC spec subprotocol"]
   [nil "--sql-subname <JDBC subname>" "The SQL JDBC spec subname"]
   [nil "--sql-user <user>" "The user to log in to the SQL database as"]
   [nil "--sql-password <password>" "The password to authenticate the user"]
   [nil "--sql-table <table>" "The SQL table to read from"
    :parse-fn #(keyword %)]
   [nil "--sql-id-column <column-name>" "The SQL column in the table to partition by"
    :parse-fn #(keyword %)]
   [nil "--sql-rows-per-segment <n>" "The number of rows to compact into a single segment at read time"
    :default default-sql-rows-per-segment
    :parse-fn #(Integer/parseInt %)]

   ["-h" "--help"]])

(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (clojure.string/join \newline errors)))

(defn parse-cli-opts
  "Takes a string that represents command-line input to
   an onyx-etl program and returns a map. The key :success
   is always present. If false, building the Onyx job failed.
   A :msgs key will be present with a sequence of strings
   offering an explanation about the failure. If :success
   is set to true, the key :job contains the job."
  [args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)]
    (cond (:help options)
          {:success false :msgs (clojure.string/split summary #"\n")}

          errors
          {:success false :msgs [(error-msg errors)]}

          :else
          (let [opts (parse-opts args cli-options)
                from (:from (:options opts))
                to (:to (:options opts))
                workflow (build-workflow from to)
                
                input-catalog-entries (find-input-catalog-entries from (:options opts))
                output-catalog-entries (find-output-catalog-entries to (:options opts))
                catalog (concat input-catalog-entries output-catalog-entries)

                input-lifecycle-entries (find-input-lifecycles from)
                output-lifecycle-entries (find-output-lifecycles to)
                lifecycles (concat input-lifecycle-entries output-lifecycle-entries)]

            (cond (not workflow)
                  {:success false
                   :msgs [(format "onyx-etl doesn't support moving data from %s to %s. Aborting." from to)]}

                  (not input-catalog-entries)
                  {:success false
                   :msgs [(format "onyx-etl doesn't have input catalog entries for %s. Aborting." from)]}

                  (not output-catalog-entries)
                  {:success false
                   :msgs [(format "onyx-etl doesn't have output catalog entries for %s. Aborting." to)]}

                  (not input-lifecycle-entries)
                  {:success false
                   :msgs [(format "onyx-etl doesn't have input lifecycles for %s" from)]}

                  (not output-catalog-entries)
                  {:success false
                   :msgs [(format "onyx-etl doesn't have output lifecycles for %s" to)]}

                  :else
                  {:success true
                   :job
                   {:workflow workflow
                    :catalog catalog
                    :lifecycles lifecycles
                    :task-scheduler :onyx.task-scheduler/balanced}})))))
