(ns messaging.worker
  (:use (messaging core common)))

(defn all-complete? [swarm-tasks]
  (every? #(% :complete?) swarm-tasks))

(defn disconnect-worker [[channel q-name]]
  (.queueDelete channel q-name))

(defn disconnect-all [swarm-tasks]
  (doseq [req swarm-tasks]
    (req :disconnect)))

(defn wait-until-completion [swarm-tasks allowed-time]
  (loop [all-complete (all-complete? swarm-tasks)
         elapsed-time 0]
    (if (> elapsed-time allowed-time)
      (do
        (disconnect-all swarm-tasks)
        (throw (RuntimeException. (str "Remote worker timeout exceeded "
                                       allowed-time
                                       " milliseconds"))))
      (if (not all-complete)
        (do
          (Thread/sleep 100)
          (recur (all-complete? swarm-tasks) (+ elapsed-time 100)))))))

(defmacro from-swarm [swarm-tasks & exprs]
  `(do
     (wait-until-completion ~swarm-tasks 5000)
     ~@exprs))

(defn request-envelope
  ([worker-name args]
     {:worker-name worker-name, :worker-args args})
  ([worker-name args return-q-name]
     (assoc (request-envelope worker-name args) :return-q return-q-name)))

(defn update-on-response [worker-ref return-q-name]
  (let [channel (.createChannel *rabbit-connection*)
        consumer (consumer-for channel DEFAULT-EXCHANGE-NAME DEFAULT-EXCHANGE-TYPE return-q-name return-q-name)
        on-response (fn [response-message]
                       (dosync
                        (ref-set worker-ref (read-string response-message))
                        (.queueDelete channel return-q-name)
                        (.close channel)))]
    (future (on-response (delivery-from channel consumer)))
    [channel return-q-name]))

(defn dispatch-work [worker-name args worker-ref]
  (let [return-q-name (str (java.util.UUID/randomUUID))
        request-object (request-envelope worker-name args return-q-name)
        worker-transport (update-on-response worker-ref return-q-name)]
    (send-message WORKER-QUEUE request-object)
    worker-transport))

(defn attribute-from-response [worker-internal-data attribute-name]
  (if (= worker-init-value worker-internal-data)
    (throw (RuntimeException. "Worker not complete!")))
  (if (not (= :success (keyword (worker-internal-data :status))))
    (throw (RuntimeException. (str "Worker has errors: " (worker-internal-data :message)))))
  (worker-internal-data attribute-name))

(defn on-swarm [worker-service args]
  (let [worker-data (ref worker-init-value)
        worker-transport (dispatch-work worker-service args worker-data)]
    (fn [accessor]
       (condp = accessor
         :complete? (not (= worker-init-value @worker-data))
         :value (attribute-from-response @worker-data :value)
         :status (@worker-data :status)
         :disconnect (disconnect-worker worker-transport)))))

(defmacro defworker [service-name args & exprs]
  `(let [worker-name# (keyword '~service-name)]
     (dosync
      (alter workers assoc worker-name# (fn ~args (do ~@exprs))))
     (def ~service-name (worker-runner worker-name# true ~args))))

(defmacro worker-runner [worker-name should-return worker-args]
  `(fn ~worker-args
      (if ~should-return
        (on-swarm ~worker-name ~worker-args))))

(defn run-worker-without-response [worker-name args]
  (let [request (request-envelope worker-name args)]
    (send-message WORKER-QUEUE request)))

(defmacro fire-and-forget [worker & args]
  `(run-worker-without-response (keyword '~worker) '~args))

(defn run-worker-on-all-servers [worker-name args]
  (let [request (request-envelope worker-name args)]
    (send-message BROADCAST-EXCHANGE FANOUT-EXCHANGE-TYPE BROADCAST-QUEUE request)))

(defmacro run-worker-everywhere [worker & args]
  `(run-worker-on-all-servers (keyword '~worker) '~args))


