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

### Flow limiting valve

Reads the input and emits its messages to the output at a rate no
greater than that given by the period.

```
(valves/flow-limiting-valve input output 1000)
```

### Clock source

Emits :tick messages at a rate given by the period.

```
(valves/clock-source 1000)
```

Clock sources use java.util.concurrent schedulers to achieve greater
precision than that afforded by a timeout channel loop. Clock source
channels must be closed after use in order to clean up internal processes.

## License

Copyright © 2015 Donald Ball

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
