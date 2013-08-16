(ns messaging.common)

(def workers (ref {}))

(def ^:const worker-init-value :__worker_init__)

(def ^:const WORKER-QUEUE "katrina-workers-job-queue")

(def ^:const BROADCAST-QUEUE "katrina-workers-broadcast-queue")

(def ^:const BROADCAST-EXCHANGE "katrina-workers-fanex")

