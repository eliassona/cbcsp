(ns cbcsp.core-test
  (:import [com.couchbase.client.java.error TranscodingException 
            DocumentAlreadyExistsException 
            RequestTooBigException 
            CouchbaseOutOfMemoryException 
            CASMismatchException])
  (:require [clojure.test :refer :all]
            [cbcsp.core :refer :all]
            [clojure.core.async :refer 
             [put! chan go go-loop >! <! >!! <!! timeout thread dropping-buffer]]))

(deftest successfull-agg-create
  (agg-create {} :a (fn [k data] (go data)) nil))   
  

(deftest successful-agg-update 
  (is (= {:data {:b 1}} (<!! (agg-update :a {:b 1} (fn [k m] (go {:data m})))))))

(deftest no-retry-error-agg-update
  (let [e (RuntimeException.)]
    (is (= {:error e, :retry false} (<!! (agg-update :a {:b 1} (fn [k m] (go {:error e}))))))))


(deftest retry-error-agg-update
  (let [e (CASMismatchException.)]
    (is (= {:error e, :retry true} (<!! (agg-update :a {:b 1} (fn [k m] (go {:error e}))))))))