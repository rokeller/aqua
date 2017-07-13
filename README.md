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
scenario where the messages in the queue define jobs to execute. aqua is made available as a NuGet package with strong
named assemblies for your convenience.

Currently, jobs are specified in JSON similar to the below.
```json
{ "Job": "HelloWho", "Properties": { "Who": "World" } }
```

You can use the `Producer` to create and enqueue job requests, or you can craft them manually. Jobs can be queued on
demand, and you can even use Azure Scheduler Jobs to queue job requests periodically. A message with the above content
can be interpreted to instruct a worker (using aqua) to execute the `HelloWho` job with the parameter `Who` set to `World`.

That is, if there is a registered job type such as the following:

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
that describe unknown jobs, or to requeue them, if desired only up to a certain threshold of times. The former can be
useful for queues which are used solely by aqua-based consumers, while the latter may be useful if the same queue is
used by a multitude of different consumers which know how to handle subsets of the messages stored in that queue.
Requeueing can be done with a visibility timeout to enable basic retry scenarios with wait times.

In addition, aqua can be configured to reuse existing CloudStorageAccount objects when connecting to Azure, or to create
new ones based on connection strings or account names and keys. Of course it also allows to connect to the local Azure
Storage Emulator for local testing and debugging.

When using the aqua `Consumer` to consume (and handle) jobs from the queue, you can also specify, through retry strategies,
the behavior to apply when the queue is currently empty. The following retry strategies come with the aqua library:
- No retry -- i.e. try once and then stop.
- Simple Retry -- i.e. try N times with a static wait time between tries.
- Linear Back-Off -- i.e. try N times with linear increasing wait times based on a configurable base wait time.
- Exponential Back-Off -- i.e. try N times with exponentially increasing wait times based on a configurable base wait
time.
- Add Jitter -- i.e. a strategy which adds jitter to the wait times of any other strategy (e.g. the exponential
back-off) to introduce some variation for competing consumers.
- Virtual Infinite Repeater -- i.e. repeat the last wait time from another strategy virtually forver.

In addition, you can of course also implement your own custom strategies tailored to your needs.

## Recent Changes

* v1.0.6.0
  * Fix a bug in the `Consumer`'s handling of binary messages which cannot be converted to string -- treat them as bad
    messages and apply the configured behavior.
* v1.0.5.0
  * Fix a bug in the `InfiniteRepeaterRetryStrategy` which would end up using an intermediate wait time if hopping
    between attempts.
  * Remove the `DequeueCount` property from queue messages created with the `Producer`.
  * Catch exceptions when binding jobs to a descriptor throws, e.g. due to deserialization exceptions, and treat the
    underlying message as bad messages.
* v1.0.4.0
  * Track job execution statistics such as average duration of succeeded and failed jobs per known job type.
  * Add requeue behaviors for bad messages and unknown jobs up a certain threshold, then delete messages.
  * Add consumer settings to configure the behavior for failed jobs too, offering the same options as for bad messages
    and unknown jobs.
  * Allow to specify a requeue visibility timeout for bad messages and unknown jobs, as well as failed jobs.
* v1.0.3.0
  * Fix a bug in the `InfiniteRepeaterRetryStrategy` which would always return 00:00:00 as the wait time if asked right
    away for the wait time of an attempt not supported by the inner strategy. I.e. if an inner strategy was used to
    return a wait time of 00:00:10 with their last try #5, and the `InfiniteRepeaterRetryStrategy` was asked right away
    for the wait time of try #6 (instead of asking for 1-5 first), 00:00:00 was returned.
  * Removed some leftover (unused) `Console.WriteLine` calls.
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
