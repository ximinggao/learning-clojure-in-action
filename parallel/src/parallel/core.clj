(ns parallel.core
  (:require [taoensso.carmine :as redis]))

(defn new-job [job-id worker batch-size batch-wait-time id-generator]
  {:tasks-atom (atom {})
   :job-id job-id
   :worker worker
   :batch-size batch-size
   :batch-wait-time batch-wait-time
   :id-gen id-generator})

(def ^:const KEY-SEPARATOR "___")

(defn managed-key [job-id task-id]
  (str job-id KEY-SEPARATOR task-id))

(def ^:const STATUS-FIELD "status")

(defn update-status-as [job-id task-id status]
  (redis/hset (managed-key job-id task-id) STATUS-FIELD status))

(defn status-of [job-id task-id]
  (redis/hget (managed-key job-id task-id) STATUS-FIELD))

(def ^:const DISPATCHED "dispatched")
(def ^:const INITIAL "initial")
(def ^:const COMPLETE "complete")
(def ^:const ERROR "error")
(def ^:const RECOVERY "recovery")
(def ^:const SECONDARY "secondary")
(def ^:const UNKNOWN "unknown")

(defn mark-dispatched [job-id task-id]
  (update-status-as job-id task-id DISPATCHED))

(defn mark-error [job-id task-id]
  (update-status-as job-id task-id ERROR))

(def ^:const next-status
  {DISPATCHED INITIAL,
   INITIAL COMPLETE,
   RECOVERY SECONDARY,
   SECONDARY COMPLETE,
   "" UNKNOWN,
   nil UNKNOWN,
   UNKNOWN UNKNOWN})

(defn increment-status [job-id task-id]
  (->> (status-of job-id task-id)
       (next-status)
       (update-status-as job-id task-id)))

(defn start-job [{:keys [batch-size] :as job} args-seq]
  (let [args-batches (partition-all batch-size args-seq)]
    (doseq [args-batch args-batches]
      (run-batch job args-batch))))
