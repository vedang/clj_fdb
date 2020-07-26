(ns examples.class-scheduling
  (:require
    [byte-streams :as bs]
    [clojure.string :as cs]
    [clojure.tools.logging :as ctl]
    [me.vedang.clj-fdb.FDB :as cfdb]
    [me.vedang.clj-fdb.core :as fc]
    [me.vedang.clj-fdb.transaction :as ftr]
    [me.vedang.clj-fdb.tuple.tuple :as ftup])
  (:import
    (com.apple.foundationdb
      Database
      FDB
      Transaction
      TransactionContext)
    (java.lang
      IllegalArgumentException)))


(defn available-classes
  "Returns a list of available classes. An available class is one with
  1 or more seats open for enrollment."
  [^TransactionContext db]
  (reduce-kv (fn [m k v]
               (if (> v 0)
                 (assoc m k v)
                 m))
             ;; Sorting here just for aesthetic purposes, this is
             ;; not part of the orignal example.
             (sorted-map)
             (fc/get-range db
                           (-> "class"
                               ftup/from
                               ftup/range)
                           :keyfn (fn [k-ba]
                                    (-> k-ba
                                        ftup/from-bytes
                                        ftup/get-items
                                        second))
                           :valfn (fn [v-ba]
                                    (bs/convert v-ba Integer)))))


(defn- signup-student*
  "Internal function. Assumes all checks are cleared and we are inside
  a transaction."
  [^Transaction tr student-id class-id seats-left]
  (fc/set tr (ftup/from "attends" class-id student-id) (ftup/from ""))
  (fc/set tr (ftup/from "attends" student-id class-id) (ftup/from ""))
  (fc/set tr (ftup/from "class" class-id) (int (dec seats-left)))
  (ctl/info (format "Hello %s! You have been signed up for %s!"
                    student-id
                    class-id))
  class-id)


(defn signup-student
  "Signs up a student for a class. Constraints are as follows:

  - The student isn't already enrolled for this class.
  - The class should have seats available for enrollment.
  - The student can sign up for a maximum of 5 classes."
  [^TransactionContext db student-id class-id]
  (ftr/run db
    (fn [^Transaction tr]
      (if (fc/get tr (ftup/from "attends" class-id student-id))
        (ctl/info (format "Hello %s! You are already signed up for %s!"
                          student-id
                          class-id))
        (let [seats-left (fc/get tr
                                 (ftup/from "class" class-id)
                                 :valfn (fn [v-ba]
                                          (bs/convert v-ba Integer)))
              previously-signed-up (->> (ftup/from "attends" student-id)
                                        ftup/range
                                        (fc/get-range tr)
                                        count)]
          (cond
            (and seats-left
                 (pos? seats-left)
                 (< previously-signed-up 5))
            (signup-student* tr student-id class-id seats-left)

            (>= previously-signed-up 5)
            (throw (IllegalArgumentException.
                     (format "Hello %s! You've already signed up for the max number of allowed classes!"
                             student-id)))

            :else
            (throw (IllegalArgumentException.
                     (format "Sorry %s! No seats remaining in %s!"
                             student-id
                             class-id)))))))))


(defn- drop-student*
  "Internal function. Assumes all checks are cleared and we are inside
  a transaction."
  [^Transaction tr student-id class-id]
  (let [seats-left (fc/get tr
                           (ftup/from "class" class-id)
                           :valfn (fn [v-ba]
                                    (bs/convert v-ba Integer)))]
    (ctl/info (format "Hey %s! Sorry to see you go from %s!"
                      student-id
                      class-id))
    (fc/clear tr (ftup/from "attends" class-id student-id))
    (fc/clear tr (ftup/from "attends" student-id class-id))
    (fc/set tr (ftup/from "class" class-id) (int (inc seats-left)))
    class-id))


(defn drop-student
  "Drops a student from a class, if he is signed up for it."
  [^TransactionContext db student-id class-id]
  (ftr/run db
    (fn [^Transaction tr]
      (if (ftup/from "attends" class-id student-id)
        (drop-student* tr student-id class-id)
        (ctl/info (format "Hello %s! You aren't currently already signed up for %s!"
                          student-id
                          class-id))))))


(defn switch-classes
  "Given a student-id and two class-ids, switch classes for the
  student! The contraints are as follows:

  - Don't drop the existing class unless the student has successfully
    signed up for the new class."
  [^TransactionContext db student-id old-class-id new-class-id]
  (ftr/run db
    (fn [^Transaction tr]
      (drop-student tr student-id old-class-id)
      (signup-student tr student-id new-class-id))))


(defn add-class
  "Used to populate the database's class list. Adds a new class to the
  list of available classes and sets the number of available seats for
  the class."
  [^TransactionContext db classname available-seats]
  (fc/set db (ftup/from "class" classname) (int available-seats)))


(defn init-db
  "Helper function to initialize the db with a bunch of classnames.
  Sets the number of available seats for each class to 10. Clears out
  the DB, so all existing information about classes and attendees is
  dropped."
  [^Database db classnames]
  (ftr/run db
    (fn [^Transaction tr]
      ;; Clear list of who attends which class
      (->> "attends"
           ftup/from
           ftup/range
           (fc/clear-range tr))
      ;; Clear list of classes
      (->> "class"
           ftup/from
           ftup/range
           (fc/clear-range tr))
      ;; Add list of classes as given to us
      (doseq [c classnames]
        (add-class tr c (int 10))))))


(defn reset-class
  "Helper function to remove all attendees from a class and reset it.
  If `available-seats` is provided, we use that number as the new
  value of `available-seats`. If not, we set the value to the number
  of attendees in the class + the number of seats still available in
  the class.

  *NOTE*: This is not part of the original example."
  ([^TransactionContext db class-id]
   (ftr/run db
     (fn [^Transaction tr]
       (let [attendance-range-key (ftup/from "attends" class-id)
             class-key (ftup/from "class" class-id)
             attendee-count (->> attendance-range-key
                                 ftup/range
                                 (fc/get-range tr)
                                 count)
             seats-left (fc/get tr
                                class-key
                                :valfn (fn [v-ba]
                                         (bs/convert v-ba Integer)))]
         (reset-class db class-id (+ attendee-count seats-left))))))
  ([^TransactionContext db class-id available-seats]
   (ctl/info (format "Resetting class %s. Available seats: %s"
                     class-id
                     available-seats))
   (ftr/run db
     (fn [^Transaction tr]
       (let [attending-sids (keys (fc/get-range tr
                                                (-> "attends"
                                                    (ftup/from class-id)
                                                    ftup/range)
                                                :keyfn (fn [k-ba]
                                                         (->> k-ba
                                                              ftup/from-bytes
                                                              ftup/get-items
                                                              (drop 2)
                                                              first))))]
         (doseq [s attending-sids]
           (drop-student tr s class-id))
         (add-class tr class-id (int available-seats)))))))


(defn reset-student
  "Drop the given student from all classes he has signed up for."
  [^TransactionContext db student-id]
  (ctl/info (format "Resetting student %s." student-id))
  (ftr/run db
    (fn [^Transaction tr]
      (let [attending-cids (keys (fc/get-range tr
                                               (-> "attends"
                                                   (ftup/from student-id)
                                                   ftup/range)
                                               :keyfn (fn [k-ba]
                                                        (->> k-ba
                                                             ftup/from-bytes
                                                             ftup/get-items
                                                             (drop 2)
                                                             first))))]
        (doseq [c attending-cids]
          (drop-student tr student-id c))))))


(defn- perform-random-action
  "Perform random actions. This function is going to respect the
  constraints from it's point of view. The magic is whether FDB can
  help us respect the constraints over a multi-threaded run."
  [^Database db student-id my-classes]
  (let [all-classes (keys (available-classes db))
        action (cond
                 ;; If I have no classes, I need to signup first.
                 (= 0 (count my-classes)) :add
                 ;; If I have 5 classes, I can't signup for more.
                 (= 5 (count my-classes)) (rand-nth [:drop :switch])
                 :else (rand-nth [:drop :switch :add]))]
    (ctl/info (format "Performing action %s for %s"
                      action
                      student-id))
    (try (case action
           :add (when-let [class-id (signup-student db
                                                    student-id
                                                    (rand-nth all-classes))]
                  (ctl/info (format "[%s][%s][%s]" :add class-id student-id))
                  (conj my-classes class-id))
           :drop (let [class-id (drop-student db
                                              student-id
                                              (rand-nth my-classes))]
                   (ctl/info (format "[%s][%s][%s]" :drop class-id student-id))
                   (remove #{class-id} my-classes))
           :switch (let [existing-class-id (rand-nth my-classes)
                         new-class-id (rand-nth (remove #{existing-class-id}
                                                        all-classes))]
                     (switch-classes db student-id existing-class-id new-class-id)
                     (ctl/info (format "[%s][%s][%s]"
                                       :switch
                                       existing-class-id
                                       new-class-id
                                       student-id))
                     (conj (remove #{existing-class-id} my-classes)
                           new-class-id)))
         (catch Exception e
           (ctl/info e "My My. We hit some constraint. *INVESTIGATE THIS!*")
           my-classes))))


(defn simulate-student
  "Simulates a student represented by `sid`. The student
  performs `ops-per-student` actions. Actions are any of:

  - `signup`: signup for a class (chosen at random)
  - `drop`: drop an existing class (chosen at random)
  - `switch`: switch from an existing class to a different
    class (chosen at random)."
  [sid ops-per-student]
  (let [student-id (str "Student: " sid)
        fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)]
    (with-open [db (cfdb/open fdb)]
      (reduce (fn [my-classes _]
                (perform-random-action db student-id my-classes))
              ;; Pass this student's current classes in.
              (keys (fc/get-range db
                                  (-> "attends"
                                      (ftup/from student-id)
                                      ftup/range)
                                  :keyfn (fn [k-ba]
                                           (->> k-ba
                                                ftup/from-bytes
                                                ftup/get-items
                                                (drop 2)
                                                first))))
              (range ops-per-student)))))


(defn run-sim
  "Runs the `simulate-student` function across multiple threads."
  [num-of-students ops-per-student]
  (let [futures (map (fn [i] (future (simulate-student i ops-per-student)))
                     (range num-of-students))]
    (mapv deref (shuffle futures))))


(comment
  ;; Create classes for fun and profit
  (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)
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
    (with-open [db (cfdb/open fdb)]
      (init-db db classnames)))

  ;; List all the available classes
  (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)]
    (with-open [db (cfdb/open fdb)]
      (available-classes db)))

  ;; Sign-up a student for a class
  (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)]
    (with-open [db (cfdb/open fdb)]
      (signup-student db "student-1" "101 alg 10:00")))

  ;; Switch classes for a student
  (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)]
    (with-open [db (cfdb/open fdb)]
      (switch-classes db "student-1" "101 alg 10:00" "101 alg 12:00")))

  ;; Drop a student from a class
  (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)]
    (with-open [db (cfdb/open fdb)]
      (drop-student db "student-1" "101 alg 12:00")))

  ;; Reset the state of the class
  (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)]
    (with-open [db (cfdb/open fdb)]
      (reset-class db "101 alg 12:00")))

  ;; Reset the state of a student
  (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)]
    (with-open [db (cfdb/open fdb)]
      (reset-student db "student-1")))

  ;; Simulate actions for a single student
  (simulate-student "1" 10)

  ;; Simulate actions for a bunch of students signing up for a bunch
  ;; of classes
  (run-sim 10 10))
