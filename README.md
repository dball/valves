# Valves

Valves is a collection of core.async components. It adheres to a plumbing
metaphor, which I've found useful both for conceptualizing systems and
communicating about their design.

## Terms

Sources emit messages, drains consume messages, and valves mediate the flow
between two channels.

## Usage

```
(require '[dball.valves :as valves])
```

### Batching valve

Reads the input and emits it in batches to the output, bounded by a
maximum time and an optional maximum buffer size.
```
(valves/batching-valve input output 5000 100)
```

## License

Copyright © 2015 Donald Ball

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
