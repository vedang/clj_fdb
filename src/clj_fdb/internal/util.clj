(ns clj-fdb.internal.util)

(let [alphabet (vec "abcdefghijklmnopqrstuvwxyz0123456789")]
  (defn rand-str
    "Generate a random string of length l"
    [l]
    (loop [n l res (transient [])]
      (if (zero? n)
        (apply str (persistent! res))
        (recur (dec n) (conj! res (alphabet (rand-int 36))))))))
