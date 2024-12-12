# primy
A distributed algorithm to find (large) prime numbers using the Fermat primality test. The algorithm has one master function (called is_prime) that is in control of the computation and a dynamic set of workers (processes) that are assigned numbers to test for primarity.

> The current implementation is largely inefficient and can do with some optimizations (especially when computing large number :)).

## Restrictions
The fermat primality test is a probabilistic test, it can correctly find a prime number but is not always accurate when determining non-prime numbers. This current implementation doesn't handle a case where a worker returns a wrong value or when a worker dies without returning a value.

## Development

```sh
gleam run   # Run the project
gleam test  # Run the tests
gleam shell # Run an Erlang shell
```
