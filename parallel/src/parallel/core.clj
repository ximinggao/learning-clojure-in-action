(ns parallel.core
  (:require [taoensso.carmine :as redis])
  (:use (messaging worker)))

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

(declare run-batch run-task)

(defn start-job [{:keys [batch-size] :as job} args-seq]
  (let [args-batches (partition-all batch-size args-seq)]
    (doseq [args-batch args-batches]
      (run-batch job args-batch))))

(defn run-batch [{:keys [id-gen tasks-atom batch-wait-time] :as job} args-batch]
  (doseq [args args-batch]
    (run-task job (apply id-gen args) args mark-dispatched))
  (wait-until-completion (map :proxy (vals @tasks-atom)) batch-wait-time))

(defn run-task [{:keys [job-id worker tasks-atom]} task-id args mark-status]
  (mark-status job-id task-id)
  (let [task-info {:args args
                   :proxy (apply worker [job-id task-id args])}]
    (swap! tasks-atom assoc task-id task-info)))

(defn redis-config []
  {})

(def ^:dynamic *return-value*)

(defmacro with-redis [& body]
  `(binding [*return-value* nil]
     (redis/wcar (redis-config)
                 ~@(drop-last body)
                 (set! *return-value* ~(last body)))
     *return-value*))

(defn slave-wrapper [worker-function]
  (fn [job-id task-id worker-args]
    (with-redis
      (increment-status job-id task-id)
      (try
        (let [return (apply worker-function worker-args)]
          (increment-status job-id task-id)
          return)
        (catch Exception e
          (mark-error job-id task-id))))))

(defmacro slave-worker [name args & body]
  `(let [simple-function# (fn ~args (do ~@body))
         slave-function# (slave-wrapper simple-function#)]
     (defworker ~name [~'job-id ~'task-id ~'worker-args]
       (slave-function# ~'job-id ~'task-id ~'worker-args))))
