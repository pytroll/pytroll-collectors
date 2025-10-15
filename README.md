# pytroll-collectors


[![Build status](https://github.com/pytroll/pytroll-collectors/workflows/CI/badge.svg?branch=main)](https://github.com/pytroll/pytroll-collectors/workflows/CI/badge.svg?branch=main)

[![Build status](https://ci.appveyor.com/api/projects/status/5lm42n0l65l5o9xn?svg=true)](https://ci.appveyor.com/project/pytroll/pytroll-collectors)

[![Coverage Status](https://coveralls.io/repos/github/pytroll/pytroll-collectors/badge.svg?branch=main)](https://coveralls.io/github/pytroll/pytroll-collectors?branch=main)

[![PyPI version](https://badge.fury.io/py/pytroll-collectors.svg)](https://badge.fury.io/py/pytroll-collectors)

A set of modules and functions to support real-time processing of satellite
data with pytroll. It requires the Posttroll library for messaging. Satellite
data processing is often done in chunks, usually referred to as granules or
segments. Several chunks are normally required to cover a given area of
interest. When these chunks of data are processed and a Posttroll message is
sent modules in Pytroll-collectors support the collection of such chunks into
datasets, allowing other (Pytroll) processes to start processing on all the
chunks in one batch once all relevant chunks are available. This is, however,
only one example of what this package provides. There are also functionality to
list and stalk files in an object store like AWS S3 for instance.

[Documentation on Readthedocs](https://pytroll-collectors.readthedocs.io/en/latest/).


# License

Copyright 2012 Pytroll-collectors developers

Licensed under the Apache License, Version 2.0 (the "License");
you may not use these files except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
