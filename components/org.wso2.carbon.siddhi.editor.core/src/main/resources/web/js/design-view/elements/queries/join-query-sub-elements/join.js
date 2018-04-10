/**
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

define(
    function () {

        /**
         * @class Join
         * @constructor
         * @class Join Creates an Join section in join query
         * @param {Object} options Rendering options for the view
         */
        var Join = function (options) {
            /*
             Data storing structure as follows.
                "type":'',
                "left-stream":{
                    "from":'',
                    "filter":'',
                    "window":'',
                    "post-window-query":'',
                    "as":''
                },
                right-stream":{
                    from":'',
                    "filter":'',
                    "window":'',
                    "post-window-query":'',
                    "as":''
                },
                "on":''

            */

            this.type = options.type;
            this.leftStream = options.leftStream;
            this.rightStream = options.rightStream;
            this.on = options.on;
        };

        Join.prototype.getType = function () {
            return this.type;
        };

        Join.prototype.getLeftStream = function () {
            return this.leftStream;
        };

        Join.prototype.getRightStream = function () {
            return this.rightStream;
        };

        Join.prototype.getOn = function () {
            return this.on;
        };

        Join.prototype.setType = function (type) {
            this.type = type;
        };

        Join.prototype.setLeftStream = function (leftStream) {
            this.leftStream = leftStream;
        };

        Join.prototype.setRightStream = function (rightStream) {
            this.rightStream = rightStream;
        };

        Join.prototype.setOn = function (on) {
            this.on = on;
        };

        return Join;

    });