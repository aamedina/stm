(ns stm.core)

(defmacro <?
  [expr]
  `(throw-err (cljs.core.async/<! ~expr)))

(defmacro dosync
  [binding-name & body]
  `(let [~binding-name (or ~binding-name (locking-transaction))]
     (swap! stm assoc (.-id ~binding-name) ~binding-name)
     (cljs.core.async.macros/go-loop [fn# (fn [] ~@body)
                                      tx# ~binding-name]
       (try (<? (runInTransaction tx# fn#))
            (catch js/Error err#
              (if (identical? err# RetryException)
                (recur fn# tx#)
                (throw err#)))))))
