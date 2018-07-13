#pragma once

#if ENABLE_S3

#include "ref.hh"
#include "types.hh"

#include <aws/core/utils/Outcome.h>

namespace Aws { namespace Client { class ClientConfiguration; } }
namespace Aws { namespace Auth { class AWSCredentialsProvider; } }
namespace Aws { namespace S3 { class S3Client; } }

namespace nix {

struct S3Helper
{
    ref<Aws::Client::ClientConfiguration> config;
    ref<Aws::Auth::AWSCredentialsProvider> credentials;
    ref<Aws::S3::S3Client> client;

    S3Helper(const std::string & profile, const std::string & region);

    ref<Aws::Client::ClientConfiguration> makeConfig(const std::string & region);

    struct DownloadResult
    {
        std::shared_ptr<std::string> data;
        unsigned int durationMs;
    };

    DownloadResult getObject(
        const std::string & bucketName, const std::string & key);
};

template<typename Err>
struct AwsError : public Error
{
    Err err;
    AwsError(Err err, const FormatOrString & fs)
        : Error(fs), err(err) { };
};

/* Helper: given an Outcome<R, E>, return R in case of success, or
   throw an exception in case of an error. */
template<typename R, typename E>
R && checkAws(const FormatOrString & fs, Aws::Utils::Outcome<R, E> && outcome)
{
    if (!outcome.IsSuccess())
        throw AwsError<decltype(outcome.GetError().GetErrorType())>(
            outcome.GetError().GetErrorType(),
            fs.s + ": " + outcome.GetError().GetMessage());
    return outcome.GetResultWithOwnership();
}

}

#endif
