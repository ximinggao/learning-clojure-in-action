(ns parallel.core
  (:require [taoensso.carmine :as redis])
  (:use (messaging core worker)))

(defn redis-config []
  {})

(defmacro with-redis [& body]
  `(redis/wcar (redis-config) ~@body))

(def ^:const KEY-SEPARATOR "___")

(defn managed-key [job-id task-id]
  (str job-id KEY-SEPARATOR task-id))

(def ^:const STATUS-FIELD "status")

(defn update-status-as [job-id task-id status]
  (with-redis (redis/hset (managed-key job-id task-id) STATUS-FIELD status)))

(defn status-of [job-id task-id]
  (with-redis (redis/hget (managed-key job-id task-id) STATUS-FIELD)))

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

(defn mark-recovery [job-id task-id]
  (update-status-as job-id task-id RECOVERY))

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

(declare run-batch run-task job-successful? recover-job)

(defn new-job [job-id worker batch-size batch-wait-time id-generator]
  {:tasks-atom (atom {})
   :job-id job-id
   :worker worker
   :batch-size batch-size
   :batch-wait-time batch-wait-time
   :id-gen id-generator})

(defn start-job [{:keys [batch-size] :as job} args-seq]
  (reset! (:tasks-atom job) {})
  (let [args-batches (partition-all batch-size args-seq)]
    (doseq [args-batch args-batches]
      (run-batch job args-batch)))
  (loop [succeed? (job-successful? job)]
    (if-not succeed?
      (do
        (println "Rerunning failed tasks...")
        (recur (recover-job job))))))

(defn run-batch [{:keys [id-gen tasks-atom batch-wait-time] :as job} args-batch]
  (doseq [args args-batch]
    (run-task job (apply id-gen args) args mark-dispatched))
  (wait-until-completion (map :proxy (vals @tasks-atom)) batch-wait-time))

(defn should-run? [job-id task-id]
  (let [status (status-of job-id task-id)]
    (or (nil? status)
        (some #{status} [DISPATCHED RECOVERY INITIAL SECONDARY ERROR]))))

(defn run-task [{:keys [job-id worker tasks-atom]} task-id args mark-status]
  (when (should-run? job-id task-id)
    (println "Running task [" job-id task-id "]")
    (mark-status job-id task-id)
    (let [task-info {:args args
                     :proxy (apply worker [job-id task-id args])}]
      (swap! tasks-atom assoc task-id task-info))))

(comment (def ^:dynamic *return-value*)
         (defmacro with-redis [& body]
           `(binding [*return-value* nil]
              (redis/wcar (redis-config)
                          ~@(drop-last body)
                          (set! *return-value* ~(last body)))
              *return-value*)))

(defn slave-wrapper [worker-function]
  (fn [job-id task-id worker-args]
    (increment-status job-id task-id)
    (try
      (let [return (apply worker-function worker-args)]
        (increment-status job-id task-id)
        return)
      (catch Exception e
        (mark-error job-id task-id)))))

(defmacro slave-worker [name args & body]
  `(let [simple-function# (fn ~args (do ~@body))
         slave-function# (slave-wrapper simple-function#)]
     (defworker ~name [~'job-id ~'task-id ~'worker-args]
       (slave-function# ~'job-id ~'task-id ~'worker-args))))

(defn from-proxies [job proxy-command]
  (->> @(:tasks-atom job)
       vals
       (map :proxy)
       (map #(% proxy-command))))

(defn values-from [job]
  (from-proxies job :value))

(defn job-complete? [job]
  (every? true? (from-proxies job :complete?)))

(defn task-successful? [job-id task-id]
  (= COMPLETE (status-of job-id task-id)))

(defn job-successful? [job]
  (->> @(:tasks-atom job)
       keys
       (map (partial task-successful? (:job-id job)))
       (every? true?)))

(defn task-statuses [job]
  (->> @(:tasks-atom job)
       keys
       (map (fn [task-id] {task-id (status-of (:job-id job) task-id)}))
       (apply merge)
       sort))

(defn failure-tasks [job]
  (->> (task-statuses job)
       (remove #(= COMPLETE (val %)))))

(defmacro with-local-rabbit [& body]
  `(with-rabbit ["localhost" "guest" "guest"]
     ~@body))

(defn incomplete-tasks [{:keys [job-id tasks-atom]}]
  (remove (partial task-successful? job-id) (keys @tasks-atom)))

(defn recover-job [{:keys [tasks-atom] :as job}]
  (let [incomplete-ids (incomplete-tasks job)]
   (doseq [incomplete-id incomplete-ids]
     (let [args (get-in @tasks-atom [incomplete-id :args])]
       (run-task job incomplete-id args mark-recovery)))
   (wait-until-completion (map #(get-in @tasks-atom [% :proxy]) incomplete-ids) 10000))
  (job-successful? job))
