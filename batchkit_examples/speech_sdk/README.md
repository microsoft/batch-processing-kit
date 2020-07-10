### Overview

The Speech Batch Kit can be used for transcription of very large numbers of audio files against
 Azure Cognitive Service Speech containers or cloud endpoints. It is built on top of the batch-processing-kit framework.
 It serves as both an example of how to use the framework, as well as providing a production-ready utility to
 users who want to run many files against Microsoft's speech-to-text services without having to upload their
 files to the cloud.
 
 If you are unfamiliar with Azure Speech containers, first [read about them](https://docs.microsoft.com/en-us/azure/cognitive-services/speech-service/speech-container-howto).
The Speech Batch Kit runs in its own container and offers a number of built-in facilities to rapidly scale transcription work
against hundreds of such speech containers (or a few cloud endpoints) with minimal effort. The official Azure page
describing Speech Batch Kit [can be found here](https://docs.microsoft.com/en-us/azure/cognitive-services/speech-service/speech-container-batch-processing?tabs=oneshot).
  
 ![](https://docs.microsoft.com/en-us/azure/cognitive-services/speech-service/media/containers/general-diagram.png)
 
### Features

**Scale out massive batches.** Run batches with millions of files against hundreds of speech containers.
The tool takes care of automatically dispatching work.

**Keep data on-premise**. You can hit your own self-hosted container and/or cloud endpoints. Users with extreme
data compliance restrictions will use the kit to hit their self-hosted speech containers. However, the kit can also be used
against cloud endpoints if you're permitted to stream. 

**Endpoint saturation.** The tool tries to dispatch work to endpoints such that they remain saturated with work
yet not oversubscribed, based on a target concurrency and starting point for real-time factor (RTF). The scheduling
is simple Work Stealing from a work queue. Each endpoint has its own EndpointManager which steals work for each
endpoint up to its concurrency limit, and deals with placing failed requests back into the work queue up when retries
are appropriate.

**Result caching.** If a running instance of the kit is stopped by accident or intentionally, it can be restarted
with the same command-line parameters and will automatically detect audio files that have already been processed
and placed in the output directory. Caching is based on the audio fingerprint and not just filename.
Note that this feature only applies to `ONESHOT` and `DAEMON` modes.

**Provides Speech SDK Parameters.** Pass in the common parameters that developers otherwise programmatically pass to Speech SDK, without
writing any code. In particular, control features lke: N-best Hypotheses, Diarizaton, Language, Profanity Masking, Sentiment Analysis.

**Run modes.** Three modes to cover any use case. `ONESHOT` runs on a directory of files. `DAEMON` mode will
also continuously wait for any new input files and places them on the dispatch queue. `APISERVER` mode provides
a basic set of HTTP APIs for submitting batches of files and querying for their status.

**Fault tolerance and retries.** Automatic detection of errors and smart retry policy in case of transient events, such as containers
intentionally or accidentally being stopped in the middle of processing, network errors, and other glitches. Early give-up
on files that cannot be retried, e.g. invalid audio files.

**Endpoint Availability Detection.**  If one of the endpoints becomes unavailable in the middle of processing,
the kit will quarantine the endpoint from receiving any more work, and schedule any affected files for retries.
If the endpoint later becomes available again, the kit will automatically resume scheduling on it.

**Endpoint Hot-Swapping.** Add, remove, or modify the manifest of Speech container endpoints during runtime with
immediate effectiveness and without interrupting batch progress. Users can use their own logic to add or
remove containers based on the backlog of pending work without having to stop the kit, even to zero containers and back.
Simply modifying the manifest file triggers this feature.

**Real-time logging.** Real-time logging from both the framework level (e.g. orchestration, dispatch tracing), 
item level (e.g. attempts, timestamps, failure reasons), and Speech SDK logs (for each audio file).

**Run Summary.** Periodically updated report in JSON format describing all cumulative historical progress and status.
Internally, the kit does accounting of what's been tried how many times, how many files are queued, what
previous failures were, and what's being actively worked on. It periodically summarizes a report based on a snapshot
in time of this information.

**Per-Item Process Isolation.** Supports infinite runtime duration in `DAEMON` and `APISERVER` modes.

**Automatic audio format conversion.** The tool attempts to convert your audio files to the correct wave format and 
bit rate as needed by the speech containers.

### Docker Container for Linux

We only officially support the Speech Batch Kit running as a Docker container on Linux. The container should also
run on any POSIX operating system. 

The container in this repository is built and published to DockerHub on any minor revisions.
You can find tags here: https://hub.docker.com/r/batchkit/speech-batch-kit/tags
Any tags that do not contain `dev` are fully tested and production-ready. There is also a `latest` tag.

```
docker pull docker.io/batchkit/speech-batch-kit:latest
```

You can build the container yourself by simply running the [build-docker](https://github.com/microsoft/batch-processing-kit/blob/master/batchkit_examples/speech_sdk/build-docker) script.


### Endpoint Configuration

The endpoints are specified in a YAML configuration file. Refer to [this section](https://docs.microsoft.com/en-us/azure/cognitive-services/speech-service/speech-container-batch-processing?tabs=oneshot#endpoint-configuration)

### Running the kit

Refer to [this section](https://docs.microsoft.com/en-us/azure/cognitive-services/speech-service/speech-container-batch-processing?tabs=oneshot#run-the-batch-processing-container)

**Health probe on HTTP endpoint** for the kit itself. The Speech Batch Kit container exposes ```HTTP GET /ready``` on port 5000
which you can map to another port outside the container as needed.

### Run modes

Refer to [this section](https://docs.microsoft.com/en-us/azure/cognitive-services/speech-service/speech-container-batch-processing?tabs=oneshot#run-the-batch-processing-container)

### Logging

Refer to [this section](https://docs.microsoft.com/en-us/azure/cognitive-services/speech-service/speech-container-batch-processing?tabs=oneshot#logging)