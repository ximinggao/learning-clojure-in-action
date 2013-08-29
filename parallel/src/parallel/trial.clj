(ns parallel.trial
  (:require [taoensso.carmine :as car]))

(def ^:dynamic *return-value* nil)

(defmacro wcar* [& body]
  `(binding [*return-value* nil]
     (car/wcar {}
               ~@(drop-last body)
               (set! *return-value* ~(last body)))
     *return-value*))

(wcar* (car/ping) (+ 1 2))

(defn foo [a b]
  (wcar*
   (car/ping)
   (+ a b)))

(defmacro inspect-body [& body]
  `(do '~body))

(inspect-body (+ 1 2) (println "hello"))
