;;; Licensed to the Apache Software Foundation (ASF) under one
;;; or more contributor license agreements.  See the NOTICE file
;;; distributed with this work for additional information
;;; regarding copyright ownership.  The ASF licenses this file
;;; to you under the Apache License, Version 2.0 (the
;;; "License"); you may not use this file except in compliance
;;; with the License.  You may obtain a copy of the License at
;;;
;;;   http://www.apache.org/licenses/LICENSE-2.0
;;;
;;; Unless required by applicable law or agreed to in writing,
;;; software distributed under the License is distributed on an
;;; "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
;;; KIND, either express or implied.  See the License for the
;;; specific language governing permissions and limitations
;;; under the License.

((sh-mode . ((indent-tabs-mode . nil)
             (sh-indentation   . 2)
             (sh-basic-offset  . 2)))
 (cmake-mode . ((indent-tabs-mode . nil)))
 (powershell-mode . ((indent-tabs-mode . nil)))
 (c-mode . ((mode . c++)))
 (nil . ((show-trailing-whitespace . t)
         (c-basic-offset . 2)
         (tab-width . 2)
         (eval . (setq flycheck-clang-include-path
                       (list "/Users/atharva/arrow-improve-cache/cpp/src" "/Users/atharva/arrow-improve-cache/cpp/src/gandiva" "/opt/homebrew/include"
                             "/opt/homebrew/opt/llvm/include"))))))
