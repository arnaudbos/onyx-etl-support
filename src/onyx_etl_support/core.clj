(ns onyx-etl-support.core
  (:require [clojure.tools.cli :refer [parse-opts]]
            [onyx-etl-support.lifecycles.datomic-lifecycles :as dl]
            [onyx-etl-support.lifecycles.sql-lifecycles :as sl]
            [onyx-etl-support.functions.transformers :as t]
            [onyx-etl-support.catalogs.datomic-catalog :as dc]
            [onyx-etl-support.catalogs.sql-catalog :as sc]
            [onyx-etl-support.workflows.sql-to-datomic :as sd]
            [onyx.plugin.datomic]
            [onyx.plugin.sql]
            [rewrite-clj.zip :as z]))

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
  [[nil "--dry-run" "When set, prints out the Onyx job, but does not execute it."
    :parse-fn #(boolean %)
    :default :false]

   ["-f" "--from <medium>" "Input storage medium. Choices are #{sql}."
    :missing "--from is a required parameter, it was missing or incorrect"
    :parse-fn #(keyword %)
    :validate [#(some #{%} #{:sql}) "Must be one of #{sql}"]]

   ["-t" "--to <medium>" "Output storage medium. Choices are #{datomic}."
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

   [nil "--job-file <file name>" "Writes the Onyx job data into a Clojure file for standalone execution."]

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

(defn ns-form [lead-ns ns-name dev-sys-ns]
 (-> (z/of-string "(ns)")
     (z/down)
     (z/rightmost)
     (z/insert-right (symbol (str lead-ns ns-name)))
     (z/rightmost)
     (z/insert-right `(:require))
     (z/append-newline)
     (z/rightmost)
     (z/down)
     (z/insert-right `[onyx.api])
     (z/append-space 10)
     (z/append-newline)
     (z/insert-right `[onyx.plugin.sql])
     (z/append-space 10)
     (z/append-newline)
     (z/insert-right `[onyx.plugin.datomic])
     (z/append-space 10)
     (z/append-newline)
     (z/insert-right `[onyx-etl-support.functions.transformers])
     (z/append-space 10)
     (z/append-newline)
     (z/insert-right [(symbol dev-sys-ns) :as 's])
     (z/append-space 10)
     (z/append-newline)
     (z/insert-right `[com.stuartsierra.component :as ~'component])
     (z/up)
     (z/up)
     (z/string)))

(defn wf-form [wf]
  (-> (z/of-string "(def)")
      (z/down)
      (z/rightmost)
      (z/insert-right (symbol (with-out-str (clojure.pprint/pprint wf))))
      (z/append-newline)
      (z/insert-right 'workflow)
      (z/up)
      (z/string)))

(defn catalog-form [catalog]
  (-> (z/of-string "(def)")
      (z/down)
      (z/rightmost)
      (z/insert-right (symbol (with-out-str (clojure.pprint/pprint catalog))))
      (z/append-newline)
      (z/insert-right 'catalog)
      (z/up)
      (z/string)))

(defn lifecycles-form [lifecycles]
  (-> (z/of-string "(def)")
      (z/down)
      (z/rightmost)
      (z/insert-right (symbol (with-out-str (clojure.pprint/pprint lifecycles))))
      (z/append-newline)
      (z/insert-right 'lifecycles)
      (z/up)
      (z/string)))

(defn main-form []
 (-> (z/of-string "(defn)")
     (z/down)
     (z/rightmost)
     (z/insert-right '-main)
     (z/rightmost)
     (z/insert-right `[& ~'args])
     (z/rightmost)
     (z/insert-right `(~'let))
     (z/append-newline)
     (z/rightmost)
     (z/down)
     (z/insert-right `[~'n-peers])
     (z/rightmost)
     (z/down)
     (z/insert-right 4)
     (z/rightmost)
     (z/insert-right 'dev-env)
     (z/append-space 6)
     (z/append-newline)
     (z/rightmost)
     (z/insert-right `(component/start (s/onyx-dev-env ~'n-peers)))
     (z/rightmost)
     (z/insert-right 'peer-config)
     (z/append-space 6)
     (z/append-newline)
     (z/rightmost)
     (z/insert-right `(s/load-peer-config (:onyx-id ~'dev-env)))
     (z/rightmost)
     (z/insert-right 'job)
     (z/append-space 6)
     (z/append-newline)
     (z/rightmost)
     (z/insert-right {:workflow 'workflow
                      :catalog 'catalog
                      :lifecycles 'lifecycles
                      :task-scheduler :onyx.task-scheduler/balanced})
     (z/append-space 6)
     (z/append-newline)
     (z/rightmost)
     (z/insert-right 'job-id)
     (z/append-space 6)
     (z/append-newline)
     (z/rightmost)
     (z/insert-right `(:job-id (~'onyx.api/submit-job ~'peer-config ~'job)))
     (z/up)
     (z/append-space 2)
     (z/append-newline)
     (z/rightmost)
     (z/insert-right `(~'onyx.api/await-job-completion ~'peer-config ~'job-id))
     (z/append-space 2)
     (z/append-newline)
     (z/rightmost)
     (z/insert-right `(~'component/stop ~'dev-env))
     (z/append-space 2)
     (z/append-newline)
     (z/rightmost)
     (z/up)
     (z/up)
     (z/string)))

(defn write-to-ns [target-file lead-ns runner-ns dev-sys-ns job]
  (spit target-file (ns-form lead-ns runner-ns dev-sys-ns))
  (spit target-file "\n\n" :append true)
  (spit target-file (wf-form (:workflow job)) :append true)
  (spit target-file "\n\n" :append true)
  (spit target-file (catalog-form (:catalog job)) :append true)
  (spit target-file "\n\n" :append true)
  (spit target-file (lifecycles-form (:lifecycles job)) :append true)
  (spit target-file "\n\n" :append true)
  (spit target-file (main-form) :append true))

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
                catalog (vec (concat input-catalog-entries output-catalog-entries))

                input-lifecycle-entries (find-input-lifecycles from)
                output-lifecycle-entries (find-output-lifecycles to)
                lifecycles (vec (concat input-lifecycle-entries output-lifecycle-entries))]

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
                  (let [job {:workflow workflow
                             :catalog catalog
                             :lifecycles lifecycles
                             :task-scheduler :onyx.task-scheduler/balanced}
                        result {:success true :job job
                                :dry-run? (:dry-run (:options opts))
                                :job-file (:job-file (:options opts))}]
                    result))))))
