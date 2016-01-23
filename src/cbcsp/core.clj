(ns cbcsp.core
  (:require 
    [clojure.core.async :refer [put! chan go go-loop >! <! >!! <!! timeout thread dropping-buffer]]
    [clojure.data.json :as json])
  (:import [com.couchbase.client.java.document RawJsonDocument]
           [rx Observer]
           [com.couchbase.client.java CouchbaseCluster]
           [com.couchbase.client CouchbaseConnectionFactoryBuilder CouchbaseClient]
           [com.couchbase.client.java.error TranscodingException]
           [java.net URI]))


(defn subscribe [type]
  (let [m {:type type}]
    (fn [observable c]
      (.subscribe 
        observable
        (reify Observer
          (onNext [_ d] (put! c (assoc m :data d)))
          (onError [_ e] (put! c (assoc m :error e)))
          (onCompleted [_] ))))))

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
          (.getAndLock id RawJsonDocument)
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

(defn retry? [v]
  (let [e (:error v)]
    (condp = (:type v)
      :read-and-lock 
      )))

(defn retry-loop! [out-chan nr-of-retries wait-btw-in-ms]
  (fn [crud-fn]
    (go-loop 
      [retry 0]
      (let [in-chan (crud-fn)
            v (<! in-chan)]
        (if (contains? v :error)
          (if (and (< retry nr-of-retries) (retry? v))
            (do 
              (<! (timeout wait-btw-in-ms))
              (recur (inc retry)))
            (>! out-chan (assoc v :retry retry)))
          (>! out-chan v))))))
  

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
    ((retry-loop! c 15 500) ((read-and-lock! bucket (chan)) "0"))
    (<!! c)))