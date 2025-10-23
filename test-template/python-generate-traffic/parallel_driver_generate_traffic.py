#!/usr/bin/env -S python3 -u

# This file serves as a parallel driver (https://antithesis.com/docs/test_templates/test_composer_reference/#parallel-driver). 
# It does between 1 and 100 random kv puts against a random etcd host in the cluster. We then check to see if successful puts persisted
# and are correct on another random etcd host.

# Antithesis SDK
from antithesis.assertions import (
    always,
    sometimes,
)

import sys
sys.path.append("/opt/antithesis/resources")
import helper
import time
import numpy as np


def simulate_traffic():
    """
        This function will first connect to an etcd host, then execute a certain number of put requests. 
        The key and value for each put request are generated using Antithesis randomness (check within the helper.py file). 
        We return the key/value pairs from successful requests and the response times for ALL requests.
    """
    client = helper.connect_to_host()
    num_requests = helper.generate_requests()
    kvs = []
    times = []

    for _ in range(num_requests):

        # generating random str for the key and value
        key = helper.generate_random_string()
        value = helper.generate_random_string()

        # response of the put request
        stime = time.time()
        success, error = helper.put_request(client, key, value)
        etime = time.time()
        elapsed_time = etime - stime
        times.append(elapsed_time)

        # Antithesis Assertion: sometimes put requests are successful. A failed request is OK since we expect them to happen.
        sometimes(success, "Client can make successful put requests", {"error":error})

        if success:
            kvs.append((key, value))
            print(f"Client: successful put with key '{key}' and value '{value}'")
        else:
            print(f"Client: unsuccessful put with key '{key}', value '{value}', and error '{error}'")

    print(f"Client: traffic simulated!")
    return kvs, times
    

def validate_puts(kvs):
    """
        This function will first connect to an etcd host, then perform a get request on each key in the key/value array. 
        For each successful response, we check that the get request value == value from the key/value array. 
        If we ever find a mismatch, we return it. 
    """
    times = []

    client = helper.connect_to_host()

    for kv in kvs:
        key, value = kv[0], kv[1]
        stime = time.time()
        success, error, database_value = helper.get_request(client, key)
        etime = time.time()
        elapsed_time = etime - stime
        times.append(elapsed_time)

        # Antithesis Assertion: sometimes get requests are successful. A failed request is OK since we expect them to happen.
        sometimes(success, "Client can make successful get requests", {"error":error})

        if not success:
            print(f"Client: unsuccessful get with key '{key}', and error '{error}'")
        elif value != database_value:
            print(f"Client: a key value mismatch! This shouldn't happen.")
            return False, (value, database_value), times

    print(f"Client: validation ok!")
    return True, None, times


def validate_time(times, threshold):
    """
        This function will take the array of response time metrics for the last transaction and compute
        the 95th percentile. Function returns whether the resulting metric is below the threshold (in ms).
    """
    pct95 = np.percentile(times, 0.95) * 1000

    if pct95 >= threshold:
        print(f"Client: 95th percentile response time threshold exceeded " + str(pct95) + " ms")
        return False, pct95

    print(f"Client: 95th percentile response time passed " + str(pct95) + " ms")
    return True, pct95


if __name__ == "__main__":
    kvs, times = simulate_traffic()
    performant, response = validate_time(times, 10)

    # Antithesis Assertion: 95th percentile response time is <10 ms for puts
    always(performant, "95th percentile response times for puts <10 ms", {"response":response})

    values_stay_consistent, mismatch, times = validate_puts(kvs)

    # Antithesis Assertion: for all successful kv put requests, values from get requests should match for their respective keys 
    always(values_stay_consistent, "Database key values stay consistent", {"mismatch":mismatch})

    performant, response = validate_time(times, 10)

    # Antithesis Assertion: 95th percentile response time is <10 ms for gets
    always(performant, "95th percentile response times for gets <10 ms", {"response":response})
