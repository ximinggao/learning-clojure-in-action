(ns parallel.test
  (:use parallel.core))

(defn fact [n acc]
  (if (= n 0)
    acc
    (recur (dec n) (* n acc))))

(slave-worker factorial [n]
              (let [f (fact n 1)]
                (println "Calculated factorial of" n "value:" f)
                f))

(defn dispatch-factorial [job-id task-id n]
  (with-redis
    (mark-dispatched job-id task-id)
    (factorial job-id task-id [n])))
