(ns examples.class-scheduling
  (:require [clojure.string :as cs]
            [clj-fdb.core :as fc]
            [clj-fdb.transaction :as ftr]
            [clj-fdb.tuple :as ftup])
  (:import [com.apple.foundationdb Database FDB Transaction TransactionContext]))

(defn available-classes
  "Returns a list of available classes."
  [])

(defn signup-student
  "Signs up a student for a class."
  [student-id class-id])

(defn drop-student
  "Drops a student from a class"
  [student-id class-id])

(defn- add-class
  "Used to populate the database's class list."
  [^TransactionContext db ^String classname ^Integer available-seats]
  (fc/set db (ftup/from "class" classname) available-seats))

(defn- init-db
  [^Database db classnames]
  (ftr/run db
    (fn [^Transaction tr]
      ;; Clear list of who attends which class
      (->> "attends"
           ftup/from
           ftup/range
           (ftr/clear-range tr))
      ;; Clear list of classes
      (->> "class"
           ftup/from
           ftup/range
           (ftr/clear-range tr))
      ;; Add list of classes as given to us
      (doseq [c classnames]
        (add-class tr c (int 100))))))

(comment
  ;; Create classes for fun and profit
  (let [fdb (FDB/selectAPIVersion 510)
        class-levels ["intro" "for dummies" "remedial"
                      "101" "201" "301"
                      "mastery" "lab" "seminar"]
        class-types ["chem" "bio" "cs"
                     "geometry" "calc" "alg" "film"
                     "music" "art" "dance"]
        class-times ["2:00" "3:00" "4:00"
                     "5:00" "6:00" "7:00"
                     "8:00" "9:00" "10:00"
                     "11:00" "12:00" "13:00"
                     "14:00" "15:00" "16:00"
                     "17:00" "18:00" "19:00"]
        classnames (for [le class-levels
                         ty class-types
                         ti class-times]
                     (cs/join " " [le ty ti]))]
    (with-open [db (.open fdb)]
      (init-db db classnames))))
