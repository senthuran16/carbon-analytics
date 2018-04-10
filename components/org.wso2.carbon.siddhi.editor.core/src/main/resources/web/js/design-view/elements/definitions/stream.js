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

define(['require', 'elementArray'],
    function (require, ElementArray) {

        /**
         * @class Stream
         * @constructor
         * @class Stream  Creates a Stream
         * @param {Object} options Rendering options for the view
         */
        var Stream = function (options) {
            /*
             Data storing structure as follows
                id: '',
                name: '',
                isInnerStream: {boolean},
                attributeList: [
                    {
                        name: ‘’,
                        type: ‘’
                    }
                ],
                annotationList: [
                    {
                        name: ‘’,
                        type: ‘value’,
                        value: [‘value1’,’value2’]
                    },
                    and|or
                    {
                        name: ‘’
                        type: ‘keyValue’,
                        value: {‘option’:’value’}
                    }
                ]
            */
            this.id = options.id;
            this.name = options.name;
            this.isInnerStream = options.isInnerStream;
            this.attributeList =  new ElementArray();
            this.annotationList =  new ElementArray();
        };

        Stream.prototype.addAttribute = function (attribute) {
            this.attributeList.push(attribute);
        };

        Stream.prototype.addAnnotation = function (annotation) {
            this.annotationList.push(annotation);
        };

        Stream.prototype.getId = function () {
            return this.id;
        };

        Stream.prototype.getName = function () {
            return this.name;
        };

        Stream.prototype.getIsInnerStream = function () {
            return this.isInnerStream;
        };

        Stream.prototype.getAttributeList = function () {
            return this.attributeList;
        };

        Stream.prototype.getAnnotationList = function () {
            return this.annotationList;
        };

        Stream.prototype.setId = function (id) {
            this.id = id;
        };

        Stream.prototype.setName = function (name) {
            this.name = name;
        };

        Stream.prototype.setIsInnerStream = function (isInnerStream) {
            this.isInnerStream = isInnerStream;
        };

        Stream.prototype.setAttributeList = function (attributeList) {
            this.attributeList = attributeList;
        };

        Stream.prototype.setAnnotationList = function (annotationList) {
            this.annotationList = annotationList;
        };

        return Stream;

    });