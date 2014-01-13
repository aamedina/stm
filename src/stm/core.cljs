(ns stm.core
  (:refer-clojure :exclude [compare-and-set!])
  (:require [clojure.browser.repl]
            [cljs.core.async :as a :refer [<! >! put! take! chan]]
            [clojure.set :as set]
            [clojure.data.avl :as avl]
            [cljs.core.async.impl.protocols :as impl]
            [cljs.core.async.impl.channels :refer [ManyToManyChannel]])
  (:require-macros [cljs.core.async.macros :as a :refer [go go-loop]]
                   [stm.core :refer [<? dosync exhaust-channel
                                     compare-and-set!]]))

(enable-console-print!)

(defprotocol ILock
  (lock [_ tx] [_ tx read?])
  (unlock [_ tx] [_ tx read?])
  (^boolean read-locked? [_])
  (^boolean write-locked? [_]))

(defprotocol IRef
  (setState [iref newval])
  (commitRef [iref tx])
  (touch [iref tx])
  (deref-with-tx [iref tx]))

(defprotocol ITransaction
  (commitTransaction [tx])
  (doGet [tx ref])
  (doSet [tx ref newval])
  (doEnsure [tx ref])
  (doCommute [tx ref f args])
  (runInTransaction [tx f])
  (tryWriteLock [tx ref])
  (stop [tx status])
  (run [tx f])
  (release-if-ensured [tx ref])
  (block-and-bail [tx ref])
  (barge [tx ref])
  (lock-tx [tx ref]))

(def RUNNING 0)
(def COMMITTING 1)
(def RETRY 2)
(def KILLED 3)
(def COMMITTED 4)

(declare Info)

(def stm (atom (sorted-map)))

(def ids (atom 0))

(def RetryException (ex-info "RETRY" {}))

(defn retry-transaction
  [tx iref]
  (swap! (.-tvals iref) dissoc (.-id tx))
  (throw RetryException))

(defn ^boolean error?
  [x]
  (instance? js/Error x))

(defn throw-err
  [x]
  (if (error? x)
    (throw x)
    x))

(defn ^number tx-id
  []
  (cond
    (exists? js/window.performance.now) (js/window.performance.now)
    (exists? js/window.performance.webkitNow)
    (js/window.performance.webKitNow)
    :else (let [t (.getTime (js/Date.))] (if (get @stm t) (recur) t))))

(def BARGE_WAIT_NANOS (* 10 1000000))

(defn ^boolean barge-time-elapsed?
  [tx]
  (> (- (tx-id) (.-startTime tx)) BARGE_WAIT_NANOS))

(deftype TVal [val point vals])

(defn tval
  ([val point] (TVal. val point []))
  ([val point vals] (TVal. val point vals)))

(deftype Ref [id state read-lock write-lock tvals tinfo hcount minHistory
              maxHistory meta validator watches]
  IComparable
  (-compare [x y] (-compare id (.-id y)))
  ILock
  (lock [iref tx]
    (when-not write-lock
      (set! (.-write_lock iref) (.-id tx))))
  (lock [iref tx read?]
    (when-not read-lock
      (set! (.-read_lock iref) (.-id tx))))
  (unlock [iref tx]
    (when (== write-lock (.-id tx))
      (set! (.-write_lock iref) nil)))
  (unlock [iref tx read?]
    (when (== read-lock (.-id tx))
      (set! (.-read_lock iref) nil)))
  (read-locked? [iref] (not (nil? read-lock)))
  (write-locked? [iref] (not (nil? write-lock)))
  IEquiv
  (-equiv [iref other] (identical? iref other))
  IMeta
  (-meta [_] meta)
  IDeref
  (-deref [iref]
    (if (write-locked? iref)
      (peek (get @tvals lock))
      state))
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
  (touch [iref tx]
    (doEnsure tx iref))
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

(defn ref-history-count
  [ref]
  (.-hcount ref))

(defn ref-min-history
  ([ref] (.-minHistory ref))
  ([ref n] (set! (.-minHistory ref) n)))

(defn ref-max-history
  ([ref] (.-maxHistory ref))
  ([ref n] (set! (.-maxHistory ref) n)))

(defn ref-set
  [ref tx val]
  (when-not (write-locked? ref)
    (doSet tx ref val)))

(defn alter
  ([ref tx fun]
     (when-not (write-locked? ref)
       (doSet tx ref (fun (deref-with-tx ref tx)))))
  ([ref tx fun x]
     (when-not (write-locked? ref)
       (doSet tx ref (fun (deref-with-tx ref tx) x))))
  ([ref tx fun x y]
     (when-not (write-locked? ref)
       (doSet tx ref (fun (deref-with-tx ref tx) x y))))
  ([ref tx fun x y z]
     (when-not (write-locked? ref)
       (doSet tx ref (fun (deref-with-tx ref tx) x y z))))
  ([ref tx fun x y z & more]
     (when-not (write-locked? ref)
       (doSet tx ref (apply fun (deref-with-tx ref tx) x y z more)))))

(defn commute
  [ref tx fun & args]
  (when-not (write-locked? ref)
    (doCommute tx ref fun args)))

(defn ref
  ([state]
     (Ref. (swap! ids inc) state nil nil (atom {}) nil 0 0 10 nil nil nil))
  ([state & {:keys [meta validator min-history max-history]}]
     (Ref. (swap! ids inc) state nil nil (atom (avl/sorted-map)) nil 0
           min-history max-history meta validator nil)))

(defn clear!
  [refs tx]
  (if-not (empty? refs)
    (do (doseq [ref refs]
          (swap! (.-tvals ref) dissoc (.-id tx)))
        (empty refs))
    refs))

(defprotocol ICountDownLatch
  (countDown [latch])
  (await [latch]))

(deftype CountDownLatch [cnt port cyclic?]
  ICountDownLatch
  (await [latch]
    (go-loop [cnt cnt]
      (if (pos? cnt)
        (recur ((<! port) cnt))
        (when-not cyclic?
          (a/close! port)))))
  (countDown [latch]
    (put! port dec)))

(defn latch
  [cnt]
  (CountDownLatch. cnt (chan (a/dropping-buffer 1)) false))

(defn cyclic-barrier
  [cnt]
  (CountDownLatch. cnt (chan (a/dropping-buffer 1)) true))

(deftype Info [status startPoint latch])

(defn ^boolean running?
  [info]
  (or (== (.-status info) RUNNING) (== (.-status info) COMMITTING)))

(deftype LockingTransaction [id info sets ensures commutes vals startPoint
                             readPoint writePoint lastPoint startTime]
  IEquiv
  (-equiv [x y] (identical? x y))
  IComparable
  (-compare [x y] (-compare id (.-id y)))
  ITransaction
  (commitTransaction [tx]
    (doseq [ref @sets]
      (commitRef ref tx))
    (doseq [ref @ensures]
      (commitRef ref tx))
    (doseq [ref @commutes]
      (setState ref (deref-with-tx ref tx)))
    (swap! stm dissoc id))
  (doGet [tx iref]
    (if (contains? @vals iref)
      (get @vals iref)
      (try (lock iref :read)
           (when (nil? (.-tvals iref))
             (throw (js/Error. (str iref " is unbound."))))
           (let [ver (get @(.-tvals iref) id)]
             (if (<= @readPoint (.-point (peek ver)))
               (.-val (first (drop-while #(> (.-point %) @readPoint)
                                         (reverse ver))))
               (retry-transaction tx iref)))
           (finally (unlock iref :read)))))
  (doSet [tx iref newval]
    (when (contains? @commutes iref)
      (throw (js/Error. "Can't set after commute")))
    (when-not (contains? @sets iref)
      (swap! sets conj iref)
      (lock iref tx)
      (swap! vals assoc iref newval))
    newval)
  (doEnsure [tx iref]
    (when-not (contains? @ensures iref)
      (lock iref :read)
      (when (and (.-tvals iref)
                 (> (.. iref -tvals -point) @readPoint))
        (unlock iref :read))
      (if (write-locked? iref)
        (do (unlock iref :read)
            (when (== id (.-write_lock iref))))
        (swap! ensures conj iref))))
  (doCommute [tx iref f args]
    (when-not (contains? @commutes iref)
      (swap! commutes conj iref))
    (try (lock iref :read)
         (swap! vals assoc iref (get @(.-tvals iref) id))
         (finally (unlock iref :read)))
    (let [fns (get @commutes ref)]
      (if (nil? fns)
        (swap! commutes assoc ref [(delay #(f args))])
        (swap! fns conj (delay #(f args))))
      (let [ret (apply f (cons (get @vals iref) args))]
        (swap! vals assoc iref ret)
        ret)))
  (run [tx f]
    (let [locked (atom #{})]
      (loop [i 0]
        (when (< i 10000)
          (try (swap! readPoint (swap! last inc))
               (when (== i 0)
                 (swap! startPoint @readPoint)
                 (swap! startTime (tx-id)))
               (let [info (set! (.-info tx)
                                (Info. RUNNING @startPoint (latch 1)))]
                 (let [ret (f)]
                   (if (compare-and-set! (.-status info) RUNNING COMMITTING)
                     (doseq [[ref funs] @commutes]
                       (when-not (contains? @sets ref)
                         (let [^boolean ensured? (contains? @ensures ref)]
                           (release-if-ensured tx ref)
                           (tryWriteLock tx ref)
                           (swap! locked conj ref)
                           (when (and ensured?
                                      (not (nil? (.-tvals ref)))
                                      (> (.. ref -tvals -point) @readPoint))
                             (retry-transaction tx ref))
                           (let [refinfo (.-tinfo ref)]
                             (when (and (not (nil? refinfo))
                                        (not (identical? refinfo info))
                                        (running? refinfo))
                               (when-not (barge tx refinfo)
                                 (retry-transaction tx ref)))
                             (let [oldval (.-val (get (.-tvals ref) id))]
                               (swap! vals assoc ref oldval)
                               (doseq [fun funs]
                                 (swap! vals assoc ref (fun (get @vals ref))))
                               (doseq [ref @sets]
                                 (tryWriteLock tx ref)
                                 (swap! locked conj ref))
                               (doseq [[ref val] @vals]
                                 (let [validator (get-validator ref)]
                                   (assert (validator val)
                                           "Invalid ref state")))
                               (let [commitPoint (swap! lastPoint inc)]
                                 (doseq [[ref newval] @vals]
                                   (cond
                                     (nil? (get @(.-tvals ref) id))
                                     (swap! (get @(.-tvals ref) id) assoc id
                                            (tval newval commitPoint))
                                     )
                                   )))))))))))))))
  (runInTransaction [tx f]
    (try (run tx f)
         (catch js/Error err
           (if (identical? err RetryException)
             (println err)
             (throw err))
           ;; (recur tx f)
           )
         (finally (swap! stm dissoc id)))
    (loop []
      (try (go (exhaust-channel (f))
               (commitTransaction tx))
           (catch js/Error err
             (if (identical? err RetryException)
               (do (println err) (recur))
               (throw err))))))
  (stop [tx new-status]
    (set! (.-status info) new-status)
    (swap! vals clear! tx)
    (swap! sets clear! tx)
    (swap! commutes clear! tx))
  (block-and-bail [tx ref] 
    (stop tx RETRY))
  (release-if-ensured [tx ref]
    (when (contains? @(.-ensures tx) ref)
      (swap! (.-ensures tx) disj ref)
      (unlock ref :read)))
  (tryWriteLock [tx ref]
    (when-not (lock ref tx)
      (retry-transaction tx ref)))
  (barge [tx ref])
  (lock-tx [tx ref]
    (release-if-ensured tx ref)
    (let [unlocked (atom true)]
      (try (tryWriteLock tx ref)
           (reset! unlocked false)
           (when (and (seq @(.-tvals ref))
                      (> (.-point (peek (get @(.-tvals ref) id))) readPoint))
             (retry-transaction tx ref))
           (let [refinfo (.-tinfo ref)]
             (when (and (not (nil? refinfo))
                        (not (identical? refinfo info))
                        (== RUNNING (.-status refinfo)))
               (when-not (barge tx ref)))))))
  IPrintWithWriter
  (-pr-writer [tx writer opts]
    (pr-writer {:id id :sets sets :ensures ensures :commutes commutes
                :vals vals}
               writer opts)))

(defn locking-transaction
  []
  (LockingTransaction. (tx-id)
                       nil
                       (atom (sorted-set))
                       (atom (sorted-set))
                       (atom (sorted-map))
                       (atom (sorted-map))
                       (atom 0) (atom 0) (atom 0) (atom 0) (atom 0)))

(defn ^:export -main []
  (let [r (ref 0)]
    (go (time (<! (dosync tx
                    (dotimes [i 10000]
                      (alter r tx inc)))))
        (println "Committed value of ref is :" @r))
    (go (dotimes [i 10]
          (<! (go (<! (dosync tx
                        (dotimes [i 10]
                          (alter r tx inc)))))))
        (println "Committed value of ref is :" @r))
    (def r r)))

    ;; (let [state (or (first (get @(.-tvals iref) id)) (.-state iref))]
    ;;   (if (identical? (.-state iref) state)
    ;;     (swap! (.-tvals iref) update-in [id]
    ;;            (fnil conj [(.-state iref)]) newval)
    ;;     (retry-transaction tx iref))
    ;;   newval)
