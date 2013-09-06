(ns more-on-fp.core)

(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))

(defn curried-fn [f args-len]
  (fn [& args]
    (let [remaining (- args-len (count args))]
      (if (zero? remaining)
        (apply f args)
        (curried-fn (apply partial f args) remaining)))))

(defmacro defcurried [fname args & body]
  `(let [fun# (fn ~args (do ~@body))]
     (def ~fname (curried-fn fun# ~(count args)))))

(defn new-user [login password email]
  (fn [a & args]
    (condp = a
      :login login
      :password password
      :email email
      :authenticate (= password (first args)))))

(declare new-object method-specs parent-class-spec)

(def OBJECT (new-class :OBJECT nil {}))

(defn find-method [method-name klass]
  (or ((klass :methods) method-name)
      (if-not (= #'OBJECT klass)
        (find-method method-name (klass :parent)))))

(defn new-class [class-name parent methods]
  (fn klass [command & args]
    (condp = command
      :name (name class-name)
      :parent parent
      :new (new-object klass)
      :methods methods
      :method (let [[method-name] args]
                (find-method method-name klass)))))

(defmacro defclass [class-name & specs]
  (let [parent-class (parent-class-spec specs)
        fns (or (method-specs specs) {})]
    `(def ~class-name (new-class '~class-name #'~parent-class ~fns))))

(declare ^:dynamic this)

(defn new-object [klass]
  (let [state (ref {})]
   (fn thiz [command & args]
     (condp = command
       :class klass
       :class-name (klass :name)
       :set! (let [[k v] args]
               (dosync (alter state assoc k v))
               nil)
       :get (let [[key] args]
              (key @state))
       (let [method (klass :method command)]
         (if-not method
           (throw (RuntimeException.
                   (str "Unable to respond to " command))))
         (binding [this thiz]
           (apply method args)))))))

(defn method-spec [sexpr]
  (let [name (keyword (second sexpr))
        body (next sexpr)]
    [name (conj body 'fn)]))

(defn method-specs [sexprs]
  (->> sexprs
       (filter #(= 'method (first %)))
       (mapcat method-spec)
       (apply hash-map)))

(defn parent-class-spec [sexprs]
  (let [extends-spec (filter #(= 'extends (first %)) sexprs)
        extends (first extends-spec)]
    (if (empty? extends)
      'OBJECT
      (last extends))))
