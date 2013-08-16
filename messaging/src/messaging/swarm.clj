(ns messaging.swarm
  (:use (messaging core common tasks)))

(declare process-request response-for)

(declare ^:dynamic *worker-name*)

(defn handle-request-message [req-str]
  (try
    (let [req (read-string req-str)
          worker-name (req :worker-name)
          worker-args (req :worker-args)
          return-q (req :return-q)
          worker-handler (@workers worker-name)]
      (println "Processing" worker-name "with args:" worker-args)
      (binding [*worker-name* worker-name]
        (process-request worker-handler worker-args return-q)))
    (catch Exception e
      (do
        (println "Exception caught:")
        (.printStackTrace e)))))

(defn process-request [worker-handler worker-args return-q]
  (future
    (with-rabbit ["localhost" "guest" "guest"]
      (let [response-envelope (response-for worker-handler worker-args)]
        (if return-q
          (send-message return-q response-envelope))))))

(defn response-for [worker-handler worker-args]
  (try
    (if worker-handler
      (let [value (apply worker-handler worker-args)]
        {:value value :status :success})
      {:message (str "No handler for " *worker-name*) :status :error})
    (catch Exception e
      {:message (.getMessage e) :status :error})))

(defn start-handler-process []
  (println "Starting handling process...")
  (doseq [request-message (message-seq WORKER-QUEUE)]
    (handle-request-message request-message)))

(defn start-broadcast-listener []
  (println "Listening to broadcasts.")
  (doseq [request (message-seq BROADCAST-EXCHANGE FANOUT-EXCHANGE-TYPE BROADCAST-QUEUE)]
    (handle-request-message request)))

