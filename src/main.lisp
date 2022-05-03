(in-package :cl)

(defpackage :mqtt-to-sql
  (:use :cl :s-sql)
  (:local-nicknames (#:a #:alexandria))
  (:export :main))

(in-package :mqtt-to-sql)

(defun getenv (name &optional default)
    #+CMU
    (let ((x (assoc name ext:*environment-list*
                    :test #'string=)))
      (if x (cdr x) default))
    #-CMU
    (or
     #+Allegro (sys:getenv name)
     #+CLISP (ext:getenv name)
     #+ECL (si:getenv name)
     #+SBCL (sb-unix::posix-getenv name)
     #+LISPWORKS (lispworks:environment-variable name)
     default))

(defun get-humidity (payload)
  (getf (getf payload :|measurement|) :|humidity|))

(defun get-location (payload)
  (getf (getf payload :|measurement|) :|location|))

(defun get-temperature (payload)
  (getf (getf payload :|measurement|) :|temperature|))

(defun get-pressure (payload)
  (getf (getf payload :|measurement|) :|pressure|)) 

(defun get-type (payload)
  (getf (getf payload :|measurement|) :|type|))

(defun get-pm2.5 (payload)
  (getf (getf payload :|measurement|) :|pm25|))

(defun get-pm10 (payload)
  (getf (getf payload :|measurement|) :|pm10|))

(defun insert-to-db (type location &key (temperature :null) (humidity :null) (pressure :null) (pm2.5 :null) (pm10 :null))
  (postmodern:query (:insert-into 'measurements
                     :set 'time (:now)
                     'type type
                     'location location
                     'temperature temperature
                     'humidity humidity
                     'pressure pressure
                     'pm2_5 pm2.5
                     'pm10 pm10)))

;; (defmethod mcutiet.client:process-message :after ((message mcutiet.message:pingresp) client)
;;   (format t "Ping RSP~%"))

(defmethod mcutiet.client:process-message ((message mcutiet.message:publish) client)
  (let* ((json (jonathan:parse (babel:octets-to-string (mcutiet.message:payload message))))
         (location (get-location json))
         (type (get-type json)))
    (a:switch (type :test #'equal)
      ("bme280"
       (insert-to-db (get-type json)
                     (get-location json)
                     :temperature (get-temperature json)
                     :humidity (get-humidity json)
                     :pressure (get-pressure json)))
      ("sht85"
       (insert-to-db type
                     location
                     :temperature (get-temperature json)
                     :humidity (get-humidity json)))
      ("sds011"
       (insert-to-db type
                     location
                     :pm2.5 (get-pm2.5 json)
                     :pm10 (get-pm10 json)))
      (t (insert-to-db type
                       location
                       :temperature (get-temperature json)
                       :humidity (get-humidity json))))))


(defun start (db-host db-port db-user db-pass db-name mqtt-client-id mqtt-host mqtt-port topic runs &key (keep-alive 20))
  (postmodern:with-connection `(,db-name ,db-user ,db-pass ,db-host :port ,db-port)
    (let ((client (mcutiet.client:connect-to-server mqtt-host mqtt-port :client-id mqtt-client-id :keep-alive keep-alive)))
      ;; Connect to mqtt
      (mcutiet.client:send-connect client)
      (mcutiet.client:wait-for-connack client)
      (mcutiet.client:subscribe client topic)
      (mcutiet.client:wait-for-suback client)
      ;; Begin publish loop
      (do ((j 0 (+ j (a:if-let (message (mcutiet.client:run-once client))
                       (if (eql (mcutiet.message:packet-type message) :publish) 1 0) 0))))
          ((if runs (= j runs) nil))
        (sleep 0.5)))
    t))

(defun main ()
  (handler-bind
      ((error (lambda (c) (format t "Error: ~A~%" c) (invoke-restart 'continue))))
    (let ((db-name (getenv "POSTGRES_NAME"))
          (db-user (getenv "POSTGRES_USER"))
          (db-pass (getenv "POSTGRES_PASS"))
          (db-host (getenv "POSTGRES_HOST"))
          (db-port (parse-integer (getenv "POSTGRES_PORT")))
          (mqtt-client-id (getenv "MQTT_CLIENT_ID"))
          (mqtt-host (getenv "MQTT_HOST"))
          (mqtt-port (parse-integer (getenv "MQTT_PORT" "30001")))
          (mqtt-topic (getenv "MQTT_TOPIC"))
          (retries (parse-integer (getenv "RETRIES" "10"))))
      (assert db-name (db-name))
      (assert db-user (db-user))
      (assert db-pass (db-pass))
      (assert db-host (db-host))
      (assert db-port (db-port))
      (assert mqtt-client-id (mqtt-client-id))
      (assert mqtt-host (mqtt-host))
      (assert mqtt-port (mqtt-port))
      (assert mqtt-topic (mqtt-topic))
      ;; Loop as long as we don't have a result and we still have retries left
      (loop with i = 0 for
            entry =
                  (restart-case (start db-host db-port db-user db-pass db-name mqtt-client-id mqtt-host mqtt-port mqtt-topic nil)
                    (continue ()
                      :report
                      (lambda (stream)
                        (format stream "Error during processing. Retry ~A from ~A" (1+ i) 3))
                      (incf i) nil))
            while (and (not entry) (< i retries))
            do (sleep 5)))))

