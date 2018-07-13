#if ENABLE_S3

#include "s3.hh"
#include "s3-binary-cache-store.hh"
#include "nar-info.hh"
#include "nar-info-disk-cache.hh"
#include "globals.hh"
#include "compression.hh"
#include "download.hh"
#include "istringstream_nocopy.hh"
#include "json.hh"
#include "derivations.hh"
#include "finally.hh"

#include <aws/core/Aws.h>
#include <aws/core/VersionConfig.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/core/utils/logging/FormattedLogSystem.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/GetBucketLocationRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/sqs/SQSClient.h>
#include <aws/sqs/model/CreateQueueRequest.h>
#include <aws/sqs/model/DeleteQueueRequest.h>
#include <aws/sqs/model/ReceiveMessageRequest.h>
#include <aws/sqs/model/SendMessageRequest.h>
#include <aws/transfer/TransferManager.h>

#include <nlohmann/json.hpp>

using namespace Aws::Transfer;

namespace nix {

class AwsLogger : public Aws::Utils::Logging::FormattedLogSystem
{
    using Aws::Utils::Logging::FormattedLogSystem::FormattedLogSystem;

    void ProcessFormattedStatement(Aws::String && statement) override
    {
        debug("AWS: %s", chomp(statement));
    }
};

static void initAWS()
{
    static std::once_flag flag;
    std::call_once(flag, []() {
        Aws::SDKOptions options;

        /* We install our own OpenSSL locking function (see
           shared.cc), so don't let aws-sdk-cpp override it. */
        options.cryptoOptions.initAndCleanupOpenSSL = false;

        if (verbosity >= lvlDebug) {
            options.loggingOptions.logLevel =
                verbosity == lvlDebug
                ? Aws::Utils::Logging::LogLevel::Debug
                : Aws::Utils::Logging::LogLevel::Trace;
            options.loggingOptions.logger_create_fn = [options]() {
                return std::make_shared<AwsLogger>(options.loggingOptions.logLevel);
            };
        }

        Aws::InitAPI(options);
    });
}

S3Helper::S3Helper(const std::string & profile, const std::string & region)
    : config(makeConfig(region))
    , credentials(profile == ""
            ? std::dynamic_pointer_cast<Aws::Auth::AWSCredentialsProvider>(
                std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>())
            : std::dynamic_pointer_cast<Aws::Auth::AWSCredentialsProvider>(
                std::make_shared<Aws::Auth::ProfileConfigFileAWSCredentialsProvider>(profile.c_str())))
    , client(make_ref<Aws::S3::S3Client>(
            credentials,
            *config,
            // FIXME: https://github.com/aws/aws-sdk-cpp/issues/759
#if AWS_VERSION_MAJOR == 1 && AWS_VERSION_MINOR < 3
            false,
#else
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
#endif
            false))
{
}

/* Log AWS retries. */
class RetryStrategy : public Aws::Client::DefaultRetryStrategy
{
    bool ShouldRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors>& error, long attemptedRetries) const override
    {
        auto retry = Aws::Client::DefaultRetryStrategy::ShouldRetry(error, attemptedRetries);
        if (retry)
            printError("AWS error '%s' (%s), will retry in %d ms",
                error.GetExceptionName(), error.GetMessage(), CalculateDelayBeforeNextRetry(error, attemptedRetries));
        return retry;
    }
};

ref<Aws::Client::ClientConfiguration> S3Helper::makeConfig(const string & region)
{
    initAWS();
    auto res = make_ref<Aws::Client::ClientConfiguration>();
    res->region = region;
    res->requestTimeoutMs = 600 * 1000;
    res->retryStrategy = std::make_shared<RetryStrategy>();
    res->caFile = settings.caFile;
    return res;
}

S3Helper::DownloadResult S3Helper::getObject(
    const std::string & bucketName, const std::string & key)
{
    debug("fetching 's3://%s/%s'...", bucketName, key);

    auto request =
        Aws::S3::Model::GetObjectRequest()
        .WithBucket(bucketName)
        .WithKey(key);

    request.SetResponseStreamFactory([&]() {
        return Aws::New<std::stringstream>("STRINGSTREAM");
    });

    DownloadResult res;

    auto now1 = std::chrono::steady_clock::now();

    try {

        auto result = checkAws(fmt("AWS error fetching '%s'", key),
            client->GetObject(request));

        res.data = decodeContent(
            result.GetContentEncoding(),
            make_ref<std::string>(
                dynamic_cast<std::stringstream &>(result.GetBody()).str()));

    } catch (AwsError<Aws::S3::S3Errors> & e) {
        if (e.err != Aws::S3::S3Errors::NO_SUCH_KEY) throw;
    }

    auto now2 = std::chrono::steady_clock::now();

    res.durationMs = std::chrono::duration_cast<std::chrono::milliseconds>(now2 - now1).count();

    return res;
}

struct S3BinaryCacheStoreImpl : public S3BinaryCacheStore
{
    const Setting<std::string> profile{this, "", "profile", "The name of the AWS configuration profile to use."};
    const Setting<std::string> region{this, Aws::Region::US_EAST_1, "region", {"aws-region"}};
    const Setting<std::string> narinfoCompression{this, "", "narinfo-compression", "compression method for .narinfo files"};
    const Setting<std::string> lsCompression{this, "", "ls-compression", "compression method for .ls files"};
    const Setting<std::string> logCompression{this, "", "log-compression", "compression method for log/* files"};
    const Setting<uint64_t> bufferSize{
        this, 5 * 1024 * 1024, "buffer-size", "size (in bytes) of each part in multi-part uploads"};

    std::string bucketName;

    Stats stats;

    S3Helper s3Helper;

    S3BinaryCacheStoreImpl(
        const Params & params, const std::string & bucketName)
        : S3BinaryCacheStore(params)
        , bucketName(bucketName)
        , s3Helper(profile, region)
    {
        diskCache = getNarInfoDiskCache();
    }

    std::string getUri() override
    {
        return "s3://" + bucketName;
    }

    void init() override
    {
        if (!diskCache->cacheExists(getUri(), wantMassQuery_, priority)) {

            /* Create the bucket if it doesn't already exists. */
            // FIXME: HeadBucket would be more appropriate, but doesn't return
            // an easily parsed 404 message.
            auto res = s3Helper.client->GetBucketLocation(
                Aws::S3::Model::GetBucketLocationRequest().WithBucket(bucketName));

            if (!res.IsSuccess()) {
                if (res.GetError().GetErrorType() != Aws::S3::S3Errors::NO_SUCH_BUCKET)
                    throw Error(format("AWS error checking bucket '%s': %s") % bucketName % res.GetError().GetMessage());

                printInfo("creating S3 bucket '%s'...", bucketName);

                // Stupid S3 bucket locations.
                auto bucketConfig = Aws::S3::Model::CreateBucketConfiguration();
                if (s3Helper.config->region != "us-east-1")
                    bucketConfig.SetLocationConstraint(
                        Aws::S3::Model::BucketLocationConstraintMapper::GetBucketLocationConstraintForName(
                            s3Helper.config->region));

                checkAws(format("AWS error creating bucket '%s'") % bucketName,
                    s3Helper.client->CreateBucket(
                        Aws::S3::Model::CreateBucketRequest()
                        .WithBucket(bucketName)
                        .WithCreateBucketConfiguration(bucketConfig)));
            }

            BinaryCacheStore::init();

            diskCache->createCache(getUri(), storeDir, wantMassQuery_, priority);
        }
    }

    const Stats & getS3Stats() override
    {
        return stats;
    }

    /* This is a specialisation of isValidPath() that optimistically
       fetches the .narinfo file, rather than first checking for its
       existence via a HEAD request. Since .narinfos are small, doing
       a GET is unlikely to be slower than HEAD. */
    bool isValidPathUncached(const Path & storePath) override
    {
        try {
            queryPathInfo(storePath);
            return true;
        } catch (InvalidPath & e) {
            return false;
        }
    }

    bool fileExists(const std::string & path) override
    {
        stats.head++;

        auto res = s3Helper.client->HeadObject(
            Aws::S3::Model::HeadObjectRequest()
            .WithBucket(bucketName)
            .WithKey(path));

        if (!res.IsSuccess()) {
            auto & error = res.GetError();
            if (error.GetErrorType() == Aws::S3::S3Errors::RESOURCE_NOT_FOUND
                || error.GetErrorType() == Aws::S3::S3Errors::NO_SUCH_KEY
                // If bucket listing is disabled, 404s turn into 403s
                || error.GetErrorType() == Aws::S3::S3Errors::ACCESS_DENIED)
                return false;
            throw Error(format("AWS error fetching '%s': %s") % path % error.GetMessage());
        }

        return true;
    }

    void uploadFile(const std::string & path, const std::string & data,
        const std::string & mimeType,
        const std::string & contentEncoding)
    {
        auto stream = std::make_shared<istringstream_nocopy>(data);

        auto maxThreads = std::thread::hardware_concurrency();

        static std::shared_ptr<Aws::Utils::Threading::PooledThreadExecutor>
            executor = std::make_shared<Aws::Utils::Threading::PooledThreadExecutor>(maxThreads);

        TransferManagerConfiguration transferConfig(executor.get());

        transferConfig.s3Client = s3Helper.client;
        transferConfig.bufferSize = bufferSize;

        if (contentEncoding != "")
            transferConfig.createMultipartUploadTemplate.SetContentEncoding(
                contentEncoding);

        transferConfig.uploadProgressCallback =
            [&](const TransferManager *transferManager,
                const std::shared_ptr<const TransferHandle>
                    &transferHandle) {
              //FIXME: find a way to properly abort the multipart upload.
              checkInterrupt();
              debug("upload progress ('%s'): '%d' of '%d' bytes",
                             path,
                             transferHandle->GetBytesTransferred(),
                             transferHandle->GetBytesTotalSize());
            };

        transferConfig.transferStatusUpdatedCallback =
            [&](const TransferManager *,
                const std::shared_ptr<const TransferHandle>
                    &transferHandle) {
              switch (transferHandle->GetStatus()) {
                  case TransferStatus::COMPLETED:
                      printTalkative("upload of '%s' completed", path);
                      stats.put++;
                      stats.putBytes += data.size();
                      break;
                  case TransferStatus::IN_PROGRESS:
                      break;
                  case TransferStatus::FAILED:
                      throw Error("AWS error: failed to upload 's3://%s/%s'",
                                  bucketName, path);
                      break;
                  default:
                      throw Error("AWS error: transfer status of 's3://%s/%s' "
                                  "in unexpected state",
                                  bucketName, path);
              };
            };

        std::shared_ptr<TransferManager> transferManager =
            TransferManager::Create(transferConfig);

        auto now1 = std::chrono::steady_clock::now();

        std::shared_ptr<TransferHandle> transferHandle =
            transferManager->UploadFile(stream, bucketName, path, mimeType,
                                        Aws::Map<Aws::String, Aws::String>());

        transferHandle->WaitUntilFinished();

        auto now2 = std::chrono::steady_clock::now();

        auto duration =
            std::chrono::duration_cast<std::chrono::milliseconds>(now2 - now1)
                .count();

        printInfo(format("uploaded 's3://%1%/%2%' (%3% bytes) in %4% ms") %
                  bucketName % path % data.size() % duration);

        stats.putTimeMs += duration;
    }

    void upsertFile(const std::string & path, const std::string & data,
        const std::string & mimeType) override
    {
        if (narinfoCompression != "" && hasSuffix(path, ".narinfo"))
            uploadFile(path, *compress(narinfoCompression, data), mimeType, narinfoCompression);
        else if (lsCompression != "" && hasSuffix(path, ".ls"))
            uploadFile(path, *compress(lsCompression, data), mimeType, lsCompression);
        else if (logCompression != "" && hasPrefix(path, "log/"))
            uploadFile(path, *compress(logCompression, data), mimeType, logCompression);
        else
            uploadFile(path, data, mimeType, "");
    }

    void getFile(const std::string & path, Sink & sink) override
    {
        stats.get++;

        // FIXME: stream output to sink.
        auto res = s3Helper.getObject(bucketName, path);

        stats.getBytes += res.data ? res.data->size() : 0;
        stats.getTimeMs += res.durationMs;

        if (res.data) {
            printTalkative("downloaded 's3://%s/%s' (%d bytes) in %d ms",
                bucketName, path, res.data->size(), res.durationMs);

            sink((unsigned char *) res.data->data(), res.data->size());
        } else
            throw NoSuchBinaryCacheFile("file '%s' does not exist in binary cache '%s'", path, getUri());
    }

    PathSet queryAllValidPaths() override
    {
        PathSet paths;
        std::string marker;

        do {
            debug(format("listing bucket 's3://%s' from key '%s'...") % bucketName % marker);

            auto res = checkAws(format("AWS error listing bucket '%s'") % bucketName,
                s3Helper.client->ListObjects(
                    Aws::S3::Model::ListObjectsRequest()
                    .WithBucket(bucketName)
                    .WithDelimiter("/")
                    .WithMarker(marker)));

            auto & contents = res.GetContents();

            debug(format("got %d keys, next marker '%s'")
                % contents.size() % res.GetNextMarker());

            for (auto object : contents) {
                auto & key = object.GetKey();
                if (key.size() != 40 || !hasSuffix(key, ".narinfo")) continue;
                paths.insert(storeDir + "/" + key.substr(0, key.size() - 8));
            }

            marker = res.GetNextMarker();
        } while (!marker.empty());

        return paths;
    }

};

static RegisterStoreImplementation regStore([](
    const std::string & uri, const Store::Params & params)
    -> std::shared_ptr<Store>
{
    if (!hasPrefix(uri, "s3://")) return 0;
    auto store = std::make_shared<S3BinaryCacheStoreImpl>(params, std::string(uri, 5));
    store->init();
    return store;
});

struct AwsStoreImpl : public S3BinaryCacheStoreImpl, public AwsStore
{
    const Setting<std::string> buildQueue{this, "nix-build-queue",
            "sqs-queue", "The name of the AWS SQS queue to which derivations are posted."};

    ref<Aws::SQS::SQSClient> sqsClient;

    AwsStoreImpl(
        const Params & params, const std::string & bucketName)
        : S3BinaryCacheStoreImpl(params, bucketName)
        , sqsClient(make_ref<Aws::SQS::SQSClient>(s3Helper.credentials, *s3Helper.config))
    {
    }

    std::string getUri() override
    {
        return "aws://" + bucketName;
    }

    BuildResult buildDerivation(const Path & drvPath, const BasicDerivation & drv,
        BuildMode buildMode) override
    {
        if (buildMode != bmNormal)
            throw Error("store '%s' does not support this build mode", getUri());

        auto buildQueueUrl = createQueue(buildQueue);
        auto resultQueueUrl = createQueue(
            fmt("nix-build-tmp-%d-%d", time(0), rand()));

        /* Delete the result queue when we're done. */
        Finally deleteResultQueue([&]() {
            try {
                checkAws(fmt("AWS error deleting queue '%s'", resultQueueUrl),
                    sqsClient->DeleteQueue(
                        Aws::SQS::Model::DeleteQueueRequest()
                        .WithQueueUrl(resultQueueUrl)));
            } catch (...) {
                ignoreException();
            }
        });

        std::ostringstream jsonStr;

        {
            JSONObject jsonRoot(jsonStr, false);
            jsonRoot.attr("drvPath", drvPath);
            {
                auto drvObj(jsonRoot.object("drv"));
                drv.toJSON(drvObj);
            }
            jsonRoot.attr("resultQueue", resultQueueUrl);
        }

        checkAws(fmt("AWS error sending message to queue '%s'", buildQueueUrl),
            sqsClient->SendMessage(
                Aws::SQS::Model::SendMessageRequest()
                .WithQueueUrl(buildQueueUrl)
                .WithMessageBody(jsonStr.str())));

        while (true) {
            checkInterrupt();

            auto res = checkAws(fmt("AWS error receiving message from queue '%s'", resultQueueUrl),
                sqsClient->ReceiveMessage(
                    Aws::SQS::Model::ReceiveMessageRequest()
                    .WithQueueUrl(resultQueueUrl)
                    .WithWaitTimeSeconds(20)));

            assert(res.GetMessages().size() <= 1);

            if (res.GetMessages().empty()) continue;

            auto & msg = res.GetMessages()[0];

            debug("got JSON result from SQS: %s", msg.GetBody());

            // FIXME: should not be necessary.
            auto s = msg.GetBody();
            s = replaceStrings(s, "&lt;", "<");
            s = replaceStrings(s, "&gt;", ">");

            auto resultData = nlohmann::json::parse(s);

            BuildResult buildResult;
            buildResult.status = resultData["status"];
            buildResult.errorMsg = resultData["errorMsg"];
            buildResult.startTime = resultData["startTime"];
            buildResult.stopTime = resultData["stopTime"];

            return buildResult;
        }
    }

    std::string createQueue(const std::string & queueName)
    {
        auto res = checkAws(fmt("AWS error creating queue '%s'", queueName),
            sqsClient->CreateQueue(
                Aws::SQS::Model::CreateQueueRequest()
                .WithQueueName(queueName)));

        return res.GetQueueUrl();
    }

    ref<Aws::SQS::SQSClient> getSQSClient() override
    {
        return sqsClient;
    };

    std::string getBuildQueueUrl() override
    {
        return createQueue(buildQueue);
    }
};

static RegisterStoreImplementation regAwsStore([](
    const std::string & uri, const Store::Params & params)
    -> std::shared_ptr<Store>
{
    if (!hasPrefix(uri, "aws://")) return 0;
    auto store = std::make_shared<AwsStoreImpl>(params, std::string(uri, 6));
    store->init();
    return store;
});

}

#endif
