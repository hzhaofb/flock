;; -*- coding: utf-8 -*-
;; Author: Howard Zhao
;; created: 6/10/15
;;
;; Purpose: place holder for task executor
;;

(ns executor)

(defn execute
 [task]
 ; add code here to execute task
 ; task example {:tid 1 :fid 1 :task_key "http://www.test.com/path1" :eta 1433981500
 ;   :param {:user_key val1 ...}}
 ; and return new eta for next schedule for this task for recuring task
 ; can switch on task function id of the task fid
 ; return nil for one time task
 )
