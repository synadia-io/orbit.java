// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.synadia.jnats.extension;

/**
 * The action to execute with retry.
 * @param <T> The return type of the action
 */
public interface RetryAction<T> {
    /**
     * Execute the action
     * @return the result
     * @throws Exception various execution exceptions; The execution throws the last exception
     * if all retries failed or the observer declines to retry.
     */
    T execute() throws Exception;
}
