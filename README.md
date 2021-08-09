# Introduction

Generic batch processing framework for managing the orchestration, dispatch, fault tolerance, and monitoring of 
arbitrary work items against many endpoints. Extensible via dependency injection. Worker endpoints can be local,
remote, containers, cloud APIs, different processes, or even just different listener sockets in the same process.

Includes examples against Azure Cognitive Service containers for ML eval workloads.

# Consuming

The framework can be built on via template method pattern and dependency injection. One simply needs to provide concrete implementation for the following types:

`WorkItemRequest`: Encapsulates all the details needed by the `WorkItemProcessor` to process a work item.

`WorkItemResult`: Representation of the outcome of an attempt to process a `WorkItemRequest`.

`WorkItemProcessor`: Provides implementation on how to process a `WorkItemRequest` against an endpoint.

`BatchRequest`: Represents a batch of work items to do. Produces a collection of `WorkItemRequest`s.

`BatchConfig`: Details needed for a `BatchRequest` to produce the collection of `WorkItemRequest`s.

`BatchRunSummarizer`: Implements a near-real-time status updater based on `WorkItemResult`s as the batch progresses.

`EndpointStatusChecker`: Specifies how to determine whether an endpoint is healthy and ready to take on work from a `WorkItemProcessor`.


The [Speech Batch Kit](https://github.com/microsoft/batch-processing-kit/blob/master/batchkit_examples/speech_sdk/README.md) is currently our prime example for consuming the framework.

The `batchkit` package is available as an ordinary pypi package. See versions here: https://pypi.org/project/batchkit

# Dev Environment

This project is developed for and consumed in Linux environments. Consumers also use WSL2, and other POSIX platforms may be compatible but are untested. For development and deployment outside of a container, we recommend using a Python virtual environment to install the `requirements.txt`. The [Speech Batch Kit](https://github.com/microsoft/batch-processing-kit/blob/master/batchkit_examples/speech_sdk/README.md) example [builds a container](https://github.com/microsoft/batch-processing-kit/blob/master/batchkit_examples/speech_sdk/build-docker).

## Tests

This project uses both unit tests `run-tests` and stress tests `run-stress-tests` for functional verification.

## Building

There are currently 3 artifacts:

- The pypi library of the batchkit framework as a library.


- The pypi library of the batchkit-examples-speechsdk.
- Docker container image for speech-batch-kit.


# Examples

### Speech Batch Kit
The Speech Batch Kit (batchkit_examples/speech_sdk) uses the framework to produce a tool that can be used for
transcription of very large numbers of audio files against Azure Cognitive Service Speech containers or cloud endpoints.

For introduction, see the [Azure Cognitive Services page](https://docs.microsoft.com/azure/cognitive-services/speech-service/speech-container-batch-processing).

For detailed information, see the [Speech Batch Kit's README](https://github.com/microsoft/batch-processing-kit/blob/master/batchkit_examples/speech_sdk/README.md).

# Contributing

This project welcomes contributions and suggestions. Most contributions require you to
agree to a Contributor License Agreement (CLA) declaring that you have the right to,
and actually do, grant us the rights to use your contribution. For details, visit
https://cla.microsoft.com.

When you submit a pull request, a CLA-bot will automatically determine whether you need
to provide a CLA and decorate the PR appropriately (e.g., label, comment). Simply follow the
instructions provided by the bot. You will only need to do this once across all repositories using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/)
or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
