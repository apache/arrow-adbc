current cloudfetch implementation download the cloud result file inline with the reader, which generate performance problem, it slows down the reader when need download the next result file

we need add prefetch functionality  to cloudfetch downloader, eg, we shouldn't block the reader because of the file download. infect we should have a separate downloader class to handle the parallel downloading of the result files.

if finished the current batch of download, the downloader need to be able to go ahead fetch next batch of download file async and start prefetching.

There are some file download code currently in the SparkCloudFetchReader.cs please remove or refactor them into the new design.
we will just use perfetch logic for downloading. to make the code simpler.

Also the logic of FetchResults call should be also in the prefetch logic.

also we need guarantee the oder of the read by the reader is same with the TSparkArrowResultLink in TFetchResultsResp

also need add following config items
1. how many parallel download allowed default value 3
2. how many file we want to prefetch default value 2
3. how many memory we want to use to buffer the prefetched files. default value 200MB
4. is prefetch enabled, default to true


here are some high level class design of this work.

** the current SparkCloudFetchReader class will be simplified, all the download file and fetch next result set logic will be moved out of this class.
** a new DownloadResult class is the central for monitor download and usage status of each fild downloaded.
    it has the link of the files
    it has the memory stream of the file
    it track the size of the downloaded file
    it also has a tasksource so that SparkCloudFetchReader can wait on if download is not finished.
    it's the event for the pipeline. and need to be disposable and holding a reference to the CloudFetchMemoryBufferManager, and return the memory to it when disposal.

** a pipeline design
    we are using a pipeline design for this work, using concurrentqueue to build the pipeline, and DownloadResult as the event of each stage
    in the pipeline. there will be 2 workers in this pipeline.
*** the resultchunk fetcher worker
    this worker will keep fetching result using a backgroud tasks from thrift server and append event to the queue waiting for download
    the fetcher worker will monitor the pending download queue only the event count less than a configurable amount to continue fetch the
    new result.

*** The file download worker
    this worker will poll event from the download queue and perferm file download and append to the result queue so that the result can be consume by SparkCloudFetchReader
    this worker will satify the concurrent download config and memory limit config.
    this is also a background task

** a new class CloudFetchDownloader to has a list of DownloadResult in a concurrent queue
    when getNextDownloadedFileAsync is called, pop and return one download result to SparkCloudFetchReader
    if the queue is empty, but there is still more result in CloudFetchResultFetcher, it should wait for new
        result comes in and restun.
    if queue is emtpy and no more result in CloudFetchResultFetcher, return null.

** class  CloudFetchMemoryBufferManager used to gate how many files to buffer in the memory
    memory need to be acquired before downlaod being scheduled.
    memory will be released once SparkCloudFetchReader finished reading this file.
    we can make DownloadResult disposable to make safe operation.

** class CloudFetchDownloadManager is the class manager all the works and classes above.
    SparkCloudFetchReader will get new download result and return batches.
    this class also moniotr the fetch and download status, when there is no more files it will reutn null to SparkCloudFetchReader
    no more files means nomore fetch can be downn from thrift server, no more events from all queues

** let's also use interface to decouple the implementation of each classes.
