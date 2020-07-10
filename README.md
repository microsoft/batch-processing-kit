# Introduction

Generic batch processing framework for managing the orchestration, dispatch, fault tolerance, and monitoring of 
arbitrary work items against many endpoints. Extensible via dependency injection. 

Includes examples against Azure Cognitive Service containers for ML eval workloads.

# Usage

The `batchkit` package is available as an ordinary pypi package. See versions here: https://pypi.org/project/batchkit

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
