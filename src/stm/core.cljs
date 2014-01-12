(ns stm.core
  (:require [clojure.browser.repl]
            [cljs.core.async :as a :refer [<! >! put! take! chan]]
            [clojure.set :as set]
            [clojure.data.avl :as avl]
            [cljs.core.async.impl.protocols :as impl])
  (:require-macros [cljs.core.async.macros :as a :refer [go go-loop]]
                   [stm.core :refer [<? dosync]]))

(enable-console-print!)

(defprotocol IRef
  (setState [iref newval])
  (commitRef [iref tx])
  (deref-with-tx [iref tx]))

(defprotocol ITransaction
  (commitTransaction [tx])
  (doSet [tx ref newval])
  (doAlter [tx ref f args])
  (doCommute [tx ref f args])
  (runInTransaction [tx f]))

(def stm (atom (avl/sorted-map-by #(compare (.-id %1) (.-id %2)))))

(def RetryException (ex-info "RETRY" {}))

(defn retry-transaction
  [tx iref]
  (swap! (.-tvals iref) dissoc (.-id tx))
  (throw RetryException))

(defn error?
  [x]
  (instance? js/Error x))

(defn throw-err
  [x]
  (if (error? x)
    (throw x)
    x))

(defn tx-id
  []
  (cond
    (exists? js/window.performance.now) (js/window.performance.now)
    (exists? js/window.performance.webkitNow)
    (js/window.performance.webKitNow)
    :else (let [t (.getTime (js/Date.))] (if (get @stm t) (recur) t))))

(defn tx-set
  ([] (avl/sorted-set-by (fn [[k1 v2] [k2 v2]] (compare k1 k2))))
  ([& ks]
     (apply avl/sorted-set-by (fn [[k1 v2] [k2 v2]] (compare k1 k2)) ks)))

(deftype Ref [state tvals meta validator watches]
  IEquiv
  (-equiv [iref other] (identical? iref other))
  IMeta
  (-meta [_] meta)
  IDeref
  (-deref [iref] state)
  IWatchable
  (-notify-watches [iref oldval newval]
    (doseq [[key f] watches]
      (f key iref oldval newval)))
  (-add-watch [iref key f]
    (set! (.-watches iref) (assoc watches key f)))
  (-remove-watch [iref key]
    (set! (.-watches iref) (dissoc watches key)))
  IHash
  (-hash [iref] (goog/getUid iref))
  IPrintWithWriter
  (-pr-writer [iref writer opts]
    (-write writer "#<Ref: ")
    (pr-writer state writer opts)
    (-write writer ">"))
  IRef
  (setState [iref newval]
    (when-not (nil? validator)
      (assert (validator newval) "Validated rejected reference state"))
    (let [oldval (.-state iref)]
      (set! (.-state iref) newval)
      (when-not (nil? watches)
        (-notify-watches iref oldval newval)))
    newval)
  (commitRef [iref tx]
    (if (identical? state (first (get @tvals (.-id tx))))
      (setState iref (deref-with-tx iref tx))
      (retry-transaction tx iref)))
  (deref-with-tx [iref tx]
    (or (peek (get @tvals (.-id tx))) state)))

(defn ref-set
  [ref tx val]
  (doSet tx ref val))

(defn alter
  [ref tx fun & args]
  (doAlter tx ref fun args))

(defn commute
  [ref tx fun & args]
  (doCommute tx ref fun args))

(defn ref
  ([state] (Ref. state (atom {}) nil nil nil))
  ([state & {:keys [meta validator]}]
     (Ref. state (atom (avl/sorted-map)) meta validator nil)))

(defn clear!
  [refs tx]
  (if-not (empty? refs)
    (do (doseq [[_ ref] refs]
          (swap! (.-tvals ref) dissoc (.-id tx)))
        (empty refs))
    refs))

(deftype LockingTransaction [id sets alters commutes]
  ITransaction
  (commitTransaction [tx]
    (let [refs (reduce into (tx-set) [@sets @alters @commutes])]
      (doseq [[_ ref] refs]
        (commitRef ref tx))
      (swap! sets clear! tx)
      (swap! alters clear! tx)
      (swap! commutes clear! tx)
      (swap! stm dissoc id)))
  (doSet [tx iref newval]
    (when-not (contains? (set (vals @sets)) iref)
      (swap! sets conj [(tx-id) iref]))
    (let [state (or (first (get @(.-tvals iref) id)) (.-state iref))]
      (if (identical? (.-state iref) state)
        (swap! (.-tvals iref) update-in [id]
               (fnil conj [(.-state iref)]) newval)
        (retry-transaction tx iref))
      newval))
  (doAlter [tx iref f args]
    (when-not (contains? (set (vals @alters)) iref)
      (swap! alters conj [(tx-id) iref]))
    (let [newval (apply f (cons (deref-with-tx iref tx) args))
          state (or (first (get @(.-tvals iref) id)) (.-state iref))]
      (if (identical? (.-state iref) state)
        (swap! (.-tvals iref) update-in [id]
               (fnil conj [(.-state iref)]) newval)
        (retry-transaction tx iref))
      newval))
  (doCommute [tx iref f args]
    (when-not (contains? (set (vals @commutes)) iref)
      (swap! commutes conj [(tx-id) iref]))
    (let [newval (apply f (cons (deref-with-tx iref tx) args))]
      (swap! (.-tvals iref) update-in [id]
             (fnil conj [(.-state iref)]) newval)
      newval))
  (runInTransaction [tx f]
    (swap! sets clear! tx)
    (swap! alters clear! tx)
    (swap! commutes clear! tx)
    (go-loop [ret (f)]
      (if (satisfies? impl/ReadPort ret)
        (recur (<! ret))
        (try (commitTransaction tx)
             (catch js/Error err
               (if (identical? err RetryException)
                 (runInTransaction tx f)
                 (throw err)))))))
  IPrintWithWriter
  (-pr-writer [tx writer opts]
    (pr-writer {:id id :sets sets :alters alters :commutes commutes}
               writer opts)))

(defn locking-transaction
  []
  (LockingTransaction. (tx-id)
                       (atom (tx-set))
                       (atom (tx-set))
                       (atom (tx-set))))
