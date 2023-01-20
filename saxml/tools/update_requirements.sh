#!/bin/bash
# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


set -e

BASE=$(mktemp -d)

copybara $(dirname $0)/../copy.bara.sky to_folder_experimental .. --folder-dir=${BASE}/saxml

docker run -it -v ${BASE}/saxml:/saxml python:3.9 \
    bash -c "pip install -U pip pip-tools && cd /saxml && pip-compile --allow-unsafe requirements.in --output-file=requirements.txt && pip-compile --allow-unsafe requirements-cuda.in --output-file=requirements-cuda.txt"

cp ${BASE}/saxml/requirements*.txt $(dirname $0)/../