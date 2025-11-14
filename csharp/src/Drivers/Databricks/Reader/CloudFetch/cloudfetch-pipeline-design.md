<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

Current cloudfetch implementation downloads the cloud result file inline with the reader, which generates performance problems, as it slows down the reader when needing to download the next result file.

We need to add prefetch functionality to the cloudfetch downloader, e.g., we shouldn't block the reader because of the file download. Instead, we should have a separate downloader class to handle the parallel downloading of the result files.

If the current batch of downloads is finished, the downloader should be able to asynchronously fetch the next batch of files and start prefetching.

There is some file download code currently in the SparkCloudFetchReader.cs; please remove or refactor it into the new design. We will just use prefetch logic for downloading to simplify the code.

Also, the logic of the FetchResults call should be included in the prefetch logic.

Additionally, we need to guarantee that the order of the read by the reader is the same as that of the TSparkArrowResultLink in TFetchResultsResp.

We need to add the following configuration items:
1. How many parallel downloads are allowed (default value: 3)
2. How many files we want to prefetch (default value: 2)
3. How much memory we want to use to buffer the prefetched files (default value: 200MB)
4. Whether prefetch is enabled (default: true)

Here are some high level class designs for this work:

- The current SparkCloudFetchReader class will be simplified; all file download and fetch-next-result-set logic will be moved out of this class.

- A new DownloadResult class is central for monitoring download and usage status of each file:
  - Contains the link to the file.
  - Holds the memory stream of the file.
  - Tracks the size of the downloaded file.
  - Includes a TaskSource so that SparkCloudFetchReader can wait if a download is not finished.
  - Acts as the event for the pipeline and should be disposable, holding a reference to the CloudFetchMemoryBufferManager and returning the memory upon disposal.

- Pipeline Design:
  - Uses a concurrent queue to build the pipeline, with DownloadResult as the event for each stage.
  - Two workers process the pipeline:
    - The result chunk fetcher worker:
      - Continuously fetches results from the Thrift server via a background task and appends events to the download queue.
      - Monitors the pending download queue and continues fetching new results only if the event count is below a configurable threshold.
    - The file download worker:
      - Polls events from the download queue, performs file downloads, and appends them to the result queue for consumption by the SparkCloudFetchReader.
      - Adheres to the concurrent download and memory limit configurations.
      - Runs as a background task.

- A new class, CloudFetchDownloader, will maintain a concurrent queue of DownloadResult objects:
  - When getNextDownloadedFileAsync is called, it will pop and return a DownloadResult to the SparkCloudFetchReader.
  - If the queue is empty but there are still results being fetched by CloudFetchResultFetcher, it should wait for new results before returning them.
  - If the queue is empty and no more results are forthcoming, it should return null.

- The CloudFetchMemoryBufferManager class will restrict how many files can be buffered in memory:
  - Memory must be acquired before scheduling a download.
  - Memory is released once SparkCloudFetchReader has finished reading a file.
  - DownloadResult can be made disposable to ensure safe operation.

- The CloudFetchDownloadManager class will manage all the above components:
  - SparkCloudFetchReader will obtain new download results in batches from this manager.
  - It monitors both fetch and download statuses, returning null to the SparkCloudFetchReader when there are no more files (i.e., no more results fetched from the Thrift server and no pending events in any queues).

- Interfaces should be used to decouple the implementations of each class.
