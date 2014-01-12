(ns stm.core)

(defmacro <?
  [expr]
  `(throw-err (cljs.core.async/<! ~expr)))

(defmacro dosync
  [binding-name & body]
  `(let [~binding-name (or ~binding-name (locking-transaction))
         ReadPort# cljs.core.async.impl.protocols/ReadPort
         fn# (fn [] ~@body)
         tx# ~binding-name]
     (swap! stm assoc (.-id ~binding-name) ~binding-name)
     (runInTransaction tx# fn#)))
