(ns stm.core
  (:refer-clojure :exclude [dosync])
  (:require [cljs.compiler :as comp]
            [cljs.analyzer :as ana]
            [cljs.env :as env]
            [cljs.core :as core]))

(defmacro <?
  [expr]
  `(let [ret# ~expr]
     (if (or (instance? cljs.core.async.impl.channels/ManyToManyChannel ret#)
             (core/implements? cljs.core.async.impl.protocols/ReadPort ret#))
       (throw-err (cljs.core.async/<! ret#))
       ret#)))

(defmacro exhaust-channel
  [expr]
  `(loop [ret# ~expr]
     (if (or (instance? cljs.core.async.impl.channels/ManyToManyChannel ret#)
             (core/implements? cljs.core.async.impl.protocols/ReadPort ret#))
       (recur (<? ret#))
       ret#)))

(defmacro dosync
  [binding-name & body]
  `(let [~binding-name (if (core/exists? ~binding-name)
                         ~binding-name
                         (locking-transaction))
         tx# ~binding-name]
     (swap! stm assoc (.-id tx#) tx#)
     (cljs.core.async.macros/go-loop
       [fn# (fn [] (cljs.core.async.macros/go ~@body))
        tx# tx#]
       (try (exhaust-channel (runInTransaction tx# fn#))
            (catch js/Error err#
              (if (identical? err# RetryException)
                (runInTransaction tx# fn#)
                (throw err#)))))))
