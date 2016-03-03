package com.dataartisans.querycommon;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class MathUtils {
    public static int murmurHash(int code) {
        code *= 0xcc9e2d51;
        code = Integer.rotateLeft(code, 15);
        code *= 0x1b873593;

        code = Integer.rotateLeft(code, 13);
        code *= 0xe6546b64;

        code ^= 4;
        code ^= code >>> 16;
        code *= 0x85ebca6b;
        code ^= code >>> 13;
        code *= 0xc2b2ae35;
        code ^= code >>> 16;

        if (code >= 0) {
            return code;
        }
        else if (code != Integer.MIN_VALUE) {
            return -code;
        }
        else {
            return 0;
        }
    }
}
