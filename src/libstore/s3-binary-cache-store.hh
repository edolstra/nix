#pragma once

#include "binary-cache-store.hh"

#include <atomic>

namespace Aws { namespace SQS { class SQSClient; } }

namespace nix {

class S3BinaryCacheStore : public BinaryCacheStore
{
protected:

    S3BinaryCacheStore(const Params & params)
        : BinaryCacheStore(params)
    { }

public:

    struct Stats
    {
        std::atomic<uint64_t> put{0};
        std::atomic<uint64_t> putBytes{0};
        std::atomic<uint64_t> putTimeMs{0};
        std::atomic<uint64_t> get{0};
        std::atomic<uint64_t> getBytes{0};
        std::atomic<uint64_t> getTimeMs{0};
        std::atomic<uint64_t> head{0};
    };

    virtual const Stats & getS3Stats() = 0;
};

struct AwsStore
{
    virtual ref<Aws::SQS::SQSClient> getSQSClient() = 0;

    virtual std::string getBuildQueueUrl() = 0;
};

}
