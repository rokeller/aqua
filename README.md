# aqua - Azure QUeue Agent

## Getting Started
Use the NuGet package `aqua.lib`
```
Install-Package aqua.lib
```
or for pre-releases
```
Install-Package aqua.lib -Pre
```

## Summary
aqua is a lightweight helper library to interact with Queues in Azure Storage Accounts in a produce/consumer like
scenario where the messages in the queue define jobs to execute. aqua will be made available as a NuGet package once
the first release candidates are ready.

Currently, jobs are specified in JSON similar to the below.
```json
{ "Job": "HelloWho", "Properties": { "Who": "World" } }
```

You can use the `Producer` to create and enqueue job requests, or you can craft them manually. A message with the above content can be interpreted to instruct a worker (using aqua) to execute the `HelloWho` job with
the parameter `Who` set to `World`.

Let there be a registered job type such as the following:

```c#
public sealed class HelloWho : JobBase
{
    public string Who { get; set; }

    public bool Execute()
    {
        Console.WriteLine("Hello, {0}!", Who);

        return true;
    }
}
```

aqua helps in binding the job descriptor from the message to a new instance of the `HelloWho` class and executing it.

## Configurability
Through a few basic configuration settings, aqua can be configured to either discard messages that are not understood or
that describe unknown jobs, or to requeue them (at the end of the queue). The former can be useful for queues which are
used solely by aqua-based consumers, while the latter may be useful if the same queue is used by a multitude of
different consumers which know how to handle subsets of the messages stored in that queue.

In addition, aqua can be configured to reuse existing CloudStorageAccount objects when connecting to Azure, or to create
new ones based on connection strings or account names and keys. Of course it also allows to connect to the local Azure
Stroage Emulator for local testing and debugging.

When using the aqua `Consumer` to consume (and handle) jobs from the queue, you can also specify the behavior to apply
when the queue is currently empty through strategies. In addition to implementing your own custom strategies, the
following strategies are available out of the box:
- No retry -- i.e. try once and then stop.
- Simple Retry -- i.e. try N times with a static wait time between tries.
- Linear Back-Off -- i.e. try N times with linear increasing wait times based on a configurable base wait time.
- Exponential Back-Off -- i.e. try N times with exponentially increasing wait times based on a configurable base wait
time.
- Add Jitter -- i.e. a strategy which adds jitter to the wait times of any other strategy (e.g. the exponential
back-off) to introduce some variation for competing consumers.
- Virtual Infinite Repeater -- i.e. repeat the last wait time from another strategy virtually forver.

## Recent Changes

* v1.0.2.0
  * Fix `Consumer.One` to respect the return value from Job execution.
  * Update `Producer.One` with overloads that take a parameter `initialVisibilityDelay` which can be used to make a
    message visible only after some time. This can be very useful for job retry scenarios where handling a job should
    only be retried after some time.
* v1.0.1.0
  * Update `Consumer` to support cancelling while waiting for a message, using a `CancellationToken`.
  * Add the `InifiniteRepeaterRetryStrategy` which can be used to wait (based on another strategy) virtually forever.

## Stay tuned ...
... for more information and samples of how to use aqua.
