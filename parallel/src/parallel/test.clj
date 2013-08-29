(ns parallel.test
  (:use parallel.core))

(defn fact [n acc]
  (if (= n 0)
    acc
    (recur (dec n) (* n acc))))

(defn throw-exception-randomly []
  (if (> 5 (rand-int 10))
    (throw (RuntimeException. "Some error occured in fibonacci!"))))

(slave-worker factorial [n]
              (throw-exception-randomly)
              (let [f (fact n 1)]
                (println "Calculated factorial of" n "value:" f)
                f))

(defn dispatch-factorial [job-id task-id n]
  (with-redis
    (mark-dispatched job-id task-id)
    (factorial job-id task-id [n])))

(def fact-job (new-job "fact-job" factorial 5 10000 identity))
