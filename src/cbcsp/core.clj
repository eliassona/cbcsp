(ns cbcsp.core
  (:require 
    [clojure.core.async :refer [put! chan go go-loop >! <! >!! <!! timeout thread dropping-buffer]]
    [clojure.data.json :as json])
  (:import [com.couchbase.client.java.document RawJsonDocument]
           [rx Observer]
           [java.util NoSuchElementException]
           [com.couchbase.client.java CouchbaseCluster]
           [com.couchbase.client CouchbaseConnectionFactoryBuilder CouchbaseClient]
           [com.couchbase.client.java.error TranscodingException 
            DocumentAlreadyExistsException 
            RequestTooBigException 
            CouchbaseOutOfMemoryException 
            CASMismatchException
            
            ]
           [com.couchbase.client.core CouchbaseException]
           [java.net URI]))


(defn subscribe [type]
  (let [m {:type type}]
    (fn [observable c]
      (let [s (atom nil)]
        (.subscribe 
          observable
          (reify Observer
            (onNext [_ d] (reset! s d))
            (onError [_ e] (put! c (assoc m :error e)))
            (onCompleted [_] (put! c (assoc m :data @s)))))))))


(defn create! [bucket c]
  (let [s (subscribe :create)]
    (fn [id m]
      (fn [] 
        (-> bucket
          (.insert (RawJsonDocument/create id (json/write-str m)))
          (s c))
        c))))

(defn read-and-lock! [bucket c]
  (let [s (subscribe :read-and-lock)]
    (fn [id]
      (fn []
        (-> bucket
          (.getAndLock id 0 RawJsonDocument)
          (s c))
        c))))


(defn read [bucket c]
  (let [s (subscribe :read)]
    (fn [id]
      (fn []
        (-> bucket
          (.get id RawJsonDocument)
          (s c))
        c))))


(defn update! [bucket c]
  (let [s (subscribe :update)]
    (fn [id m]
      (fn []
        (-> bucket
          (.replace (RawJsonDocument/create id (json/write-str m)))
          (s c))
        c))))

(defn delete! [bucket c]
  (let [s (subscribe :delete)]
    (fn [id]
      (fn []
        (-> bucket
          (.remove id)
          (s c))
        c))))

(def fail-map 
  {:create #{DocumentAlreadyExistsException RequestTooBigException CouchbaseOutOfMemoryException}
   :read #{TranscodingException NoSuchElementException CouchbaseOutOfMemoryException}
   :update #{CASMismatchException RequestTooBigException CouchbaseOutOfMemoryException}
   :delete #{CASMismatchException CouchbaseOutOfMemoryException CouchbaseException}}
  )

(defn dig-out-cause [error]
  (if (and (= (.getClass error) RuntimeException) (.getCause error))
    (.getCause error)
    error))

(defn fail? [v]
  (let [{:keys [type error]} v
        cause (dig-out-cause error)]
    (some #(instance? % cause) (type fail-map))
    )
  )
(defn retry? [v]
  (not (fail? v)))

(defn retry-loop [out-chan nr-of-retries wait-btw-in-ms]
  (fn [crud-fn]
    (go-loop 
      [retry 0]
      (let [v (<! (crud-fn))]
        (if (contains? v :error)
          (if (and (< retry nr-of-retries) (retry? v))
            (do 
              (<! (timeout wait-btw-in-ms))
              (recur (inc retry)))
            (>! out-chan (assoc v :retry retry)))
          (>! out-chan v))))
    out-chan))



(def crud-ops {:create create!, :read read, :update update!, :delete delete!})

(defn crud-op-of [bucket crud-chan retry-chan nr-of-retries wait-btw-in-ms]
  (into {} (map (fn [e] [(key e) (comp (retry-loop retry-chan nr-of-retries wait-btw-in-ms) ((val e) bucket crud-chan))])) crud-ops))
  
(defn success? [m] (contains? m :data))
  

(defn consume [config crud-ops]
  (let [{:keys [key-fn]} config
        {:keys [create read update delete]} crud-ops]
    (fn [data]
      (let [k (key-fn data)]
        (go-loop 
          [retry 0]
          (let [rm (-> k read <!)]
            (if (success? rm)
              (if (-> rm :data nil?)
                (assoc (<! (create k {})) :retry true)
                rm)
              rm)
          )
        )
  ))))

(defn consume-loop! [config]
  (let [{:keys [in-chan bucket]} config
        agg (consume config (crud-op-of bucket (chan)))]
    (go-loop 
      []
      (when-let [data (<! in-chan)]
        (agg data)
        (recur)))
    in-chan))

(defn timeout-loop! []
  )




;;------------------------------------test code-----------------------------------------------





(defonce bucket (.async (.openBucket (CouchbaseCluster/create))))


(defn make-client 
  ([uris bucket]
    (let [cfb (CouchbaseConnectionFactoryBuilder.)]
      ;(.setOpTimeout cfb timeout)
      (CouchbaseClient. (.buildCouchbaseConnection cfb (map #(URI. %) uris) bucket ""))))
  ([host port bucket ]
    (make-client [(format "http://%s:%s/pools" host port)] bucket)))

(defonce old-bucket (make-client "localhost" 8091 "default"))


(defn flush! []
  (.flush old-bucket))





(defn test-retry []
  (let [c (chan)]
    ((retry-loop c 15 500) ((read-and-lock! bucket (chan)) "0"))
    (<!! c)))


(defn test-consume-loop []
  (consume-loop! (concat {:in-chan (chan)} (crud-op-of bucket (chan)))))
  