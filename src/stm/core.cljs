(ns stm.core
  (:refer-clojure :exclude [compare-and-set!])
  (:require [clojure.browser.repl]
            [cljs.core.async :as a :refer [<! >! put! take! chan alts!]]
            [clojure.set :as set]
            [clojure.data.avl :as avl]
            [cljs.core.async.impl.protocols :as impl]
            [cljs.core.async.impl.channels :refer [ManyToManyChannel]])
  (:require-macros [cljs.core.async.macros :as a :refer [go go-loop alt!]]
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

(defn reentrant-read-write-lock
  []
  (let [writers (chan 1)
        readers (chan 1)
        write-unlock (chan 1)
        read-unlock (chan 1)]
    (go-loop [reader nil writer nil]
      (alt!
        writers ([w writers]
                   (if (and (nil? writer) (nil? reader))
                     (recur reader writer)
                     (let [[_ unlock-chan]
                           (alts! [write-unlock read-unlock])]
                       (condp identical? unlock-chan
                         write-unlock (if (nil? reader)
                                        (recur nil w)
                                        (do (<! read-unlock)
                                            (recur nil w)))
                         read-unlock (if (nil? writer)
                                        (recur nil w)
                                        (do (<! write-unlock)
                                            (recur nil w)))))))
        readers ([r readers]
                   (if (and (nil? writer) (nil? reader))
                     (recur reader writer)
                     (let [_ (<! write-unlock)]
                       (if (or (nil? writer) (== writer r))
                         (recur r writer)
                         (do (<! read-unlock)
                             (recur r writer))))))
        write-unlock ([w write-unlock] (recur reader nil))
        read-unlock ([r read-unlock] (recur nil writer))))
    [writers readers write-unlock read-unlock]))

(deftype TVal [val point])

(deftype Ref [id state read-lock write-lock read-unlock write-unlock tvals
              tinfo faults minHistory maxHistory meta validator watches]
  IComparable
  (-compare [x y] (-compare id (.-id y)))
  ILock
  (lock [iref tx]
    (go (>! write-lock (.-id tx))))
  (lock [iref tx read?]
    (go (>! read-lock (.-id tx))))
  (unlock [iref tx]
    (put! write-unlock (.-id tx)))
  (unlock [iref tx read?]
    (put! read-unlock (.-id tx)))
  (read-locked? [iref] (not (nil? read-lock)))
  (write-locked? [iref] (not (nil? write-lock)))
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
  [ref tx]
  (get @(.-tvals ref) (.-id tx)))

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
     (let [[writers readers write-unlock read-unlock]
           (reentrant-read-write-lock)]
       (Ref. (swap! ids inc) state writers readers write-unlock read-unlock
             (atom (sorted-map)) nil (atom 0) 0 10 nil nil nil)))
  ([state & {:keys [meta validator min-history max-history]}]
     (let [[writers readers write-unlock read-unlock]
           (reentrant-read-write-lock)]
       (Ref. (swap! ids inc) state writers readers write-unlock read-unlock
             (atom (sorted-map)) nil (atom 0) min-history max-history meta
             validator nil))))

(deftype CFn [fn args])

(defprotocol ICountDownLatch
  (countDown [latch])
  (await [latch] [latch timeout-ms]))

(deftype CountDownLatch [cnt port cyclic?]
  ICountDownLatch
  (await [latch]
    (go-loop [cnt cnt]
      (if (pos? cnt)
        (recur ((<! port) cnt))
        (when-not cyclic?
          (a/close! port)))))
  (await [latch timeout-ms]
    (let [timeout (a/timeout timeout-ms)]
      (go-loop [cnt cnt
                [f ch] (alts! [port timeout])]
        (condp identical? ch
          timeout nil
          port (if (pos? cnt)
                 (recur (f cnt) (alts! [port timeout]))
                 (when-not cyclic?
                   (a/close! port)))))))
  (countDown [latch]
    (put! port dec)))

(defn latch
  [cnt]
  (CountDownLatch. cnt (chan (a/dropping-buffer 1)) false))

(defn cyclic-barrier
  [cnt]
  (CountDownLatch. cnt (chan (a/dropping-buffer 1)) true))

(deftype Info [status startPoint latch]
  IPrintWithWriter
  (-pr-writer [info writer opts]
    (pr-writer {:status status :startPoint startPoint :latch latch}
               writer opts)))

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
  (doGet [tx iref]
    (go (when-not (running? info)
          (throw RetryException))
        (if (contains? @vals iref)
          (get @vals iref)
          (try (<! (lock iref tx :read))
               (when (nil? (.-tvals iref))
                 (throw (js/Error. (str iref " is unbound."))))
               (let [ver (get @(.-tvals iref) id)
                     val (.-val (first (drop-while #(> (.-point %) @readPoint)
                                                   (reverse ver))))]
                 (if-not val
                   (do (swap! (.-faults iref) inc) (throw RetryException))
                   val))
               (finally (unlock iref tx :read))))))
  (doSet [tx iref newval]
    (when-not (running? info)
      (throw RetryException))
    (when (contains? @commutes iref)
      (throw (js/Error. "Can't set after commute")))
    (when-not (contains? @sets iref)
      (swap! sets conj iref)
      (lock-tx tx iref)
      (swap! vals assoc iref newval))
    newval)
  (doEnsure [tx iref]
    (when-not (running? info)
      (throw RetryException))
    (when-not (contains? @ensures iref)
      (lock iref tx :read)
      (when (and (get @(.-tvals iref) id)
                 (> (.-point (peek (get @(.-tvals iref) id))) @readPoint))
        (unlock iref tx :read)
        (throw RetryException))
      (let [refinfo (.-tinfo iref)]
        (if (and refinfo (running? refinfo))
          (do (unlock iref tx :read)
              (when-not (identical? refinfo info)
                (go (<? (block-and-bail tx refinfo)))))
          (swap! ensures conj iref)))))
  (doCommute [tx iref f args]
    (when-not (running? info)
      (throw RetryException))
    (when-not (contains? @vals ref)
      (try (lock iref :read)
           (swap! vals assoc iref (.-val (peek (get @(.-tvals iref) id))))
           (finally (unlock iref tx :read))))
    (let [funs (get @commutes ref)]
      (when (nil? funs)
        (swap! commutes assoc ref []))
      (swap! commutes update-in [ref] conj (CFn. f args))
      (let [ret (apply f (get @vals iref) args)]
        (swap! vals assoc iref ret)
        ret)))
  (run [tx f]
    (let [locked (atom [])
          notifies (atom [])
          done (atom false)
          ret (atom nil)]
      (while (not @done)
        (try (reset! readPoint (swap! lastPoint inc))
             (when (== @startPoint 0)
               (reset! startPoint @readPoint)
               (reset! startTime (tx-id)))
             (let [info (set! (.-info tx)
                              (Info. RUNNING @startPoint (latch 1)))
                   maybe-ret (f)]
               (when (compare-and-set! (.-status info) RUNNING COMMITTING)
                 (doseq [[ref funs] @commutes]
                   (when-not (contains? @sets ref)
                     (let [^boolean ensured? (contains? @ensures ref)]
                       (release-if-ensured tx ref)
                       (tryWriteLock tx ref)
                       (swap! locked conj ref)
                       (when (and
                              ensured?
                              (not (nil? (get @(.-tvals ref) id)))
                              (> (.-point (peek (get @(.-tvals ref) id)))
                                 @readPoint))
                         (throw RetryException))
                       (let [refinfo (.-tinfo ref)]
                         (when (and (not (nil? refinfo))
                                    (not (identical? refinfo info))
                                    (running? refinfo)
                                    (not (barge tx refinfo)))
                           (throw RetryException)))
                       (let [val (.-val (peek (get @(.-tvals ref) id)))]
                         (swap! vals assoc ref val)
                         (doseq [cfn funs]
                           (let [[fun args] [(.-fn cfn) (.-args cfn)]]
                             (swap! vals assoc ref
                                    (apply fun (get @vals ref) args))))))))
                 (doseq [ref @sets]
                   (tryWriteLock tx ref)
                   (swap! locked conj ref))
                 (doseq [[ref val] @vals]
                   (let [validator (get-validator ref)]
                     (assert (validator val) "Invalid ref state")))
                 (let [commitPoint (swap! lastPoint inc)]
                   (doseq [[ref newval] @vals]
                     (let [tvals (get @(.-tvals ref) id)
                           oldval (.-val (peek tvals))]
                       (cond
                         (nil? tvals)
                         (swap! (.-tvals ref) assoc id
                                [(TVal. newval commitPoint)])
                         (or (and (pos? @(.-faults ref))
                                  (< (count tvals)
                                     (.-maxHistory ref)))
                             (< (count tvals)
                                (.-minHistory ref)))
                         (do (swap! (.-tvals ref) update-in [id]
                                    conj (TVal. newval commitPoint))
                             (reset! (.-faults ref) 0))
                         :else
                         (swap! (.-tvals ref) update-in [id]
                                conj (TVal. newval commitPoint)))
                       (swap! notifies conj [ref oldval newval])))))
               (set! (.-status info) COMMITTED)
               (reset! done true)
               (reset! ret maybe-ret))
             (catch js/Error err
               (when-not (identical? err RetryException)
                 (throw err)))
             (finally (doseq [ref (reverse @locked)]
                        (println ref)
                        (unlock ref tx))
                      (swap! locked empty)
                      (doseq [ref @ensures]
                        (unlock ref tx :read))
                      (swap! ensures empty)
                      (stop tx (if @done COMMITTED RETRY))
                      (try (doseq [[ref oldval newval] @notifies]
                             (apply -notify-watches ref oldval newval))
                           (finally (swap! notifies empty))))))            
      @ret))
  (runInTransaction [tx f]
    (try (if (nil? info)
           (run tx f)
           (f))
         (finally (swap! stm dissoc id))))
  (stop [tx new-status]
    (when-not (nil? info)
      (set! (.-status (.-info tx)) new-status)
      (swap! vals empty)
      (swap! sets empty)
      (swap! commutes empty)))
  (block-and-bail [tx refinfo] 
    (stop tx RETRY)
    (go (try (<! (await (.-latch refinfo) 100))
             (catch js/Error err
               (when-not (identical? err RetryException)
                 (throw err))))
        RetryException))
  (release-if-ensured [tx ref]
    (when (contains? @(.-ensures tx) ref)
      (swap! (.-ensures tx) disj ref)
      (unlock ref tx :read)))
  (tryWriteLock [tx ref]
    (try (let [lk (a/do-alts (fn [val] val) [(lock ref tx)] {})]
           (if (nil? lk)
             (do (unlock ref tx)
                 (throw RetryException))
             lk))
         (catch js/Error err (throw err))))
  (barge [tx refinfo]
    (when (and (barge-time-elapsed? tx) (< startPoint (.-startPoint refinfo)))
      (let [barged (compare-and-set! (.-status refinfo) RUNNING KILLED)]
        (when barged
          (countDown (.-latch refinfo)))
        barged)))
  (lock-tx [tx ref]
    (go (release-if-ensured tx ref)
        (let [unlocked (atom true)]
          (try (let [[_ write-lock] @(tryWriteLock tx ref)]
                 (reset! unlocked false)
                 (when (and (get @(.-tvals ref) id)
                            (> (.-point (peek (get @(.-tvals ref) id)))
                               @readPoint))
                   (throw RetryException))
                 (let [refinfo (.-tinfo ref)]
                   (when (and (not (nil? refinfo))
                              (not (identical? refinfo info))
                              (running? refinfo)
                              (not (barge tx refinfo)))
                     (set! (.-tinfo ref) info)
                     (.-val (peek (get @(.-tvals ref) id))))))
               (finally (when-not @unlocked
                          (unlock ref tx)))))))
  IPrintWithWriter
  (-pr-writer [tx writer opts]
    (pr-writer {:id id :info info :sets sets :ensures ensures
                :commutes commutes :vals vals}
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
    (time (dosync tx
            (dotimes [i 10000]
              (alter r tx inc))))
    (println "Committed value of ref is :" @r)
    (def r r)))
